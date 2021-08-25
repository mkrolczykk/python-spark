from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

import os
from enum import Enum

from dependencies.logger import Log4j

""" sql queries """
from .sql_queries.CONVERT_ARRAY_OF_MAPS_TO_MAP import convert_array_of_maps_to_map

""" TASK #1 - Build Purchases Attribution Projection """

MOBILE_SCHEMA = StructType([
    StructField('userId', StringType(), True),
    StructField('eventId', StringType(), True),
    StructField('eventType', StringType(), True),
    StructField('eventTime', TimestampType(), True),
    StructField('attributes', StringType(), True)
])
USER_SCHEMA = StructType([
    StructField('purchaseId', StringType(), True),
    StructField('purchaseTime', TimestampType(), True),
    StructField('billingCost', DoubleType(), True),
    StructField('isConfirmed', BooleanType(), True),
])

# root folder path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Input
MOBILE_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/mobile_app_clickstream/*.csv.gz")
PURCHASES_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/user_purchases/*.csv.gz")

# Output
RESULT_FOLDER = os.path.join(ROOT_DIR, "result_data/")
TARGET_DATAFRAME_OUTPUT = RESULT_FOLDER + "data_frame.parquet"

class EventType(Enum):
    APP_OPEN = 'app_open'
    SEARCH_PRODUCT = 'search_product'
    PRODUCT_DETAILS = 'view_product_details'
    PURCHASE = 'purchase'
    APP_CLOSE = 'app_close'

def generate_structured_mobile_data(source_df):
    attributes_to_map_struct = source_df \
        .withColumn("attributes", explode(split(col("attributes"), ", "))) \
        .withColumn("key_temp", split(col("attributes"), ":").getItem(0)) \
        .withColumn("key", translate("key_temp", "{} '\"“", "")) \
        .withColumn("value_temp", split(col("attributes"), ":").getItem(1)) \
        .withColumn("value", trim(translate("value_temp", "{}'\"“", ""))) \
        .withColumn("attributes", create_map(col("key"), col("value"))) \
        .select(col("eventId"), col("attributes")) \
        .alias('attributes_to_map_struct')

    return source_df.alias("m") \
        .join(attributes_to_map_struct.alias("a"), attributes_to_map_struct.eventId == source_df.eventId, "left") \
        .select("m.userId", "m.eventId", "m.eventType", "m.eventTime", "a.attributes")


def generate_sessions(df):
    w5 = Window.orderBy("userId", "eventTime")
    w6 = Window.partitionBy("sessionId").orderBy("userId", "eventTime")
    w7 = Window.partitionBy("sessionId")

    return df \
        .withColumn("sessionId", sum(when((col("eventType") == EventType.APP_OPEN.value), lit(1))
                                     .otherwise(lit(0))
                                     ).over(w5)) \
        .withColumn("rowNum", row_number().over(w6)) \
        .withColumn("max", max("rowNum").over(w7)) \
        .withColumn("first", when((col("rowNum") == 1) & (
        (col("eventType") == EventType.APP_CLOSE.value)), lit(1))
                    .otherwise(lit(0))) \
        .filter('max>=2 and first=0 and sessionId!=0') \
        .drop(*['rowNum', 'max', 'first']) \
        .orderBy("userId", "eventTime")


def aggregate_mobile_data(df):
    work_df = df \
        .groupBy("userId", "sessionId") \
        .agg(collect_list("attributes").alias("attributes_temp")) \
        .withColumn("length", size(col("attributes_temp"))) \
        .withColumn("sessionId", df["sessionId"].cast(StringType())) \
        .select("userId",
                "sessionId",
                "length",
                "attributes_temp")\
        .cache()

    max_col_length = work_df.select("length").agg({"length": "max"}).collect()[0][0]

    return work_df \
        .withColumn("campaign", slice("attributes_temp", start=1, length=2)) \
        .withColumn("all_purchases", slice("attributes_temp", start=3, length=max_col_length)) \
        .select("userId",
                "sessionId",
                expr(convert_array_of_maps_to_map).alias("campaign"),
                explode("all_purchases").alias("purchases"))

def create_target_dataframe_from(df_1, df_2):
    return df_2 \
        .join(df_1, df_2.purchaseId == df_1.purchases.purchase_id, "inner") \
        .select("purchaseId",
                "purchaseTime",
                "billingCost",
                "isConfirmed",
                "sessionId",
                col("campaign")["campaign_id"].alias("campaignId"),
                col("campaign")["channel_id"].alias("channelIid"))


""" Task #1.2 - Implement target dataframe by using a custom UDF """

@udf(returnType=StringType())
def prepare_attributes_udf(event_type, attributes):
    attr = str(attributes)
    if (event_type == EventType.PURCHASE.value) or (event_type == EventType.APP_OPEN.value):
        if attr.startswith("{{") and attr.endswith("}}"):
            attr = attr[1:len(attributes) - 1]
    return attr

@udf(returnType=IntegerType())
def generate_sessions_udf(event_type, generated_id):
    if event_type == EventType.APP_OPEN.value:
        session_id = generated_id
        return session_id
    else:
        return None

def create_target_dataframe_udf(mobile_app_data, purchase_data):
    w1 = Window.partitionBy('userId').orderBy('eventTime')
    w2 = Window.partitionBy('sessionId')

    result_df = (mobile_app_data
               .withColumn('sessionId_temp', generate_sessions_udf(mobile_app_data['eventType'], monotonically_increasing_id() + 1))
               .withColumn('sessionId', (sum(col('sessionId_temp')).over(w1)).cast(StringType()))
               .withColumn('attr', prepare_attributes_udf(mobile_app_data['eventType'], mobile_app_data['attributes']))
               .withColumn('campaign_id',
                           when(
                               get_json_object('attr', '$.campaign_id').isNotNull(),
                               get_json_object('attr', '$.campaign_id')
                           ).otherwise(None))
               .withColumn('channel_id',
                           when(
                               get_json_object('attr', '$.channel_id').isNotNull(),
                               get_json_object('attr', '$.channel_id')
                           ).otherwise(None))
               .withColumn('purchase_id',
                           when(
                               get_json_object('attr', '$.purchase_id').isNotNull(),
                               get_json_object('attr', '$.purchase_id')
                           ).otherwise(None))
               .withColumn('campaignId', last(col('campaign_id'), ignorenulls=True)
                           .over(w2.rowsBetween(Window.unboundedPreceding, 0)))
               .withColumn('channelIid', last(col('channel_id'), ignorenulls=True)
                           .over(w2.rowsBetween(Window.unboundedPreceding, 0)))
               .filter(mobile_app_data['attributes'].isNotNull())
               .drop(*['attributes', 'sessionId_temp', 'attr', 'campaign_id', 'channel_id', 'eventId', 'eventTime'])
               )

    return result_df \
        .join(purchase_data, result_df['purchase_id'] == purchase_data['purchaseId'], 'inner') \
        .select(
            col('purchaseId'),
            col('purchaseTime'),
            col('billingCost'),
            col('isConfirmed'),
            col('sessionId'),
            col('campaignId'),
            col('channelIid')
        )


def main(spark: SparkContext, spark_logger: Log4j, spark_config):
    """ get .csv data """
    mobile_app_data = spark.read.csv(MOBILE_DATA_PATH,
                                     header=True,
                                     schema=MOBILE_SCHEMA,
                                     ).alias("mobile_app_data")

    purchases_structured_data = spark.read.csv(PURCHASES_DATA_PATH,
                                               header=True,
                                               schema=USER_SCHEMA,
                                               ).alias("purchases_data")

    """ default Spark SQL capabilities version """

    structured_mobile_data = generate_structured_mobile_data(mobile_app_data).cache()

    result = generate_sessions(structured_mobile_data).cache()  # split data into unique sessions (session starts with app_open event and finishes with app_close)

    aggr_data = aggregate_mobile_data(result)

    target_dataframe = create_target_dataframe_from(aggr_data, purchases_structured_data)     # target dataframe

    """ END of Spark SQL capabilities version """

    """ UDF version """
    '''
    target_dataframe = create_target_dataframe_udf(mobile_app_data, purchases_structured_data)
    '''
    """ END of UDF version """

    """ show result in console """
    target_dataframe.show(10, False)  # target dataframe result

    """ save result as parquet files """
    target_dataframe.write.parquet(TARGET_DATAFRAME_OUTPUT, mode='overwrite')
