from enum import Enum

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

import os

from dependencies.logger import Log4j

""" sql queries """
from .sql_queries.CONVERT_ARRAY_OF_MAPS_TO_MAP import convert_array_of_maps_to_map

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
# MOBILE_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/mobile_app_clickstream/*.csv.gz")
# PURCHASES_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/user_purchases/*.csv.gz")

# mock
MOBILE_DATA_PATH = os.path.join(ROOT_DIR, 'tests/test_data/mobile-app-clickstream_sample.tsv')
PURCHASES_DATA_PATH = os.path.join(ROOT_DIR, 'tests/test_data/purchases_sample.tsv')

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
        .withColumn("value", translate("value_temp", "{} '\"“", "")) \
        .withColumn("attributes", create_map(col("key"), col("value"))) \
        .select(col("eventId"), col("attributes")) \
        .alias('attributes_to_map_struct')

    result = source_df.alias("m") \
        .join(attributes_to_map_struct.alias("a"), attributes_to_map_struct.eventId == source_df.eventId, "left") \
        .select("m.userId", "m.eventId", "m.eventType", "m.eventTime", "a.attributes")

    return result


def generate_sessions(df):
    w5 = Window.orderBy("userId", "eventTime")
    w6 = Window.partitionBy("session_id").orderBy("userId", "eventTime")
    w7 = Window.partitionBy("session_id")

    result = df \
        .withColumn("session_id", sum(when((col("eventType") == EventType.APP_OPEN.value), lit(1))
                                      .otherwise(lit(0))).over(w5)) \
        .withColumn("rowNum", row_number().over(w6)) \
        .withColumn("max", max("rowNum").over(w7)) \
        .withColumn("first", when((col("rowNum") == 1) & (
        (col("eventType") == EventType.APP_CLOSE.value)), lit(1))
                    .otherwise(lit(0))) \
        .filter('max>=2 and first=0') \
        .drop(*['rowNum', 'max', 'first']) \
        .orderBy("userId", "eventTime") \

    return result

def aggregate_mobile_data(df):
    work_df = df \
        .groupBy("userId", "session_id") \
        .agg(collect_list("eventType").alias("sessionId"),
             collect_list("attributes").alias("temp")) \
        .withColumn("length", size(col("temp"))) \
        .withColumn("campaign", slice("temp", start=1, length=2)) \
        .select("userId",
                "sessionId",
                "temp")

    temp_df = work_df \
        .withColumn("length", size(col("temp"))) \
        .withColumn("campaign", slice("temp", start=1, length=2)) \

    col_length = temp_df.select("length").distinct().collect()[0][0]

    temp_df2 = temp_df.withColumn("channel_id", slice("temp", start=3, length=col_length))

    result = temp_df2 \
        .select("userId",
                "sessionId",
                expr(convert_array_of_maps_to_map).alias("campaign"),
                explode(temp_df2.channel_id).alias("purchases"))

    return result

def create_target_dataframe_from(df_1, df_2):
    result = df_2 \
        .join(df_1, df_2.purchaseId == df_1.purchases.purchase_id, "inner") \
        .withColumn("sessionId", monotonically_increasing_id() + 1) \
        .select("purchaseId",
                "purchaseTime",
                "billingCost",
                "isConfirmed",
                "sessionId",
                col("campaign")["campaign_id"].alias("campaignId"),
                col("campaign")["channel_id"].alias("channelIid")) \
        .cache() \

    return result

def main(spark: SparkContext, spark_logger: Log4j, spark_config):

    """ get .csv data """
    mobile_app_data = spark.read.csv(MOBILE_DATA_PATH,
                                     header=True,
                                     # schema=MOBILE_SCHEMA
                                     sep='\t'
                                     ).alias("mobile_app_data")

    purchases_structured_data = spark.read.csv(PURCHASES_DATA_PATH,
                                               header=True,
                                               schema=USER_SCHEMA,
                                               sep='\t'
                                               ).alias("purchases_data")

    structured_mobile_data = generate_structured_mobile_data(mobile_app_data)

    result = generate_sessions(structured_mobile_data).cache()  # split data into unique sessions (session starts with app_open event and finishes with app_close)

    aggr_data = aggregate_mobile_data(result).cache()

    target_dataframe = create_target_dataframe_from(aggr_data, purchases_structured_data)     # target dataframe

    """ show result in console """
    target_dataframe.show(10, False)    # target dataframe result

    """ save result as parquet files """
    target_dataframe.write.parquet(TARGET_DATAFRAME_OUTPUT, mode='overwrite')


if __name__ == '__main__':
    main()

