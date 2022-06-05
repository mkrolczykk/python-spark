import os

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

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


def generate_purchases_attribution_projection(clickstream_data, purchases_data_df):
    w1 = Window.partitionBy('userId').orderBy('eventTime')
    w2 = Window.partitionBy('sessionId')

    clickstream_data_df = clickstream_data \
        .filter(clickstream_data["attributes"].isNotNull()) \
        .withColumn('sessionStart',
                    when(
                        col('eventType') == EventType.APP_OPEN,
                        monotonically_increasing_id() + 1
                    ).otherwise(lit(0))
                    ) \
        .withColumn('sessionId', sum("sessionStart").over(w1).cast(StringType)) \
        .withColumn("allAttributes", collect_list('attributes').over(w2)) \
        .dropDuplicates('sessionId') \
        .withColumn("campaign_details", col("allAttributes")(0)) \
        .withColumn("purchase", explode(expr("slice(allAttributes, 2, SIZE(allAttributes))"))) \
        .withColumn('campaignId', get_json_object(col("campaign_details"), "$.campaign_id")) \
        .withColumn('channelId', get_json_object(col("campaign_details"), "$.channel_id")) \
        .withColumn("purchase_id", get_json_object(col("purchase"), "$.purchase_id")) \
        .select(
        col('userId'),
        col('sessionId'),
        col('campaignId'),
        col('channelId'),
        col("purchase_id")
    )

    return clickstream_data_df \
        .join(broadcast(purchases_data_df), clickstream_data_df['purchase_id'] == purchases_data_df['purchaseId'], 'inner') \
        .select(
        col('purchase_id'),
        col('purchaseTime'),
        col('billingCost'),
        col('isConfirmed'),
        col('sessionId'),
        col('campaignId'),
        col('channelId'),
    )


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
                 .withColumn('sessionId_temp',
                             generate_sessions_udf(mobile_app_data['eventType'], monotonically_increasing_id() + 1))
                 .withColumn('sessionId', (sum(col('sessionId_temp')).over(w1)).cast(StringType()))
                 .withColumn('attr',
                             prepare_attributes_udf(mobile_app_data['eventType'], mobile_app_data['attributes']))
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
        .join(broadcast(purchase_data), result_df['purchase_id'] == purchase_data['purchaseId'], 'inner') \
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

    target_dataframe = generate_purchases_attribution_projection(mobile_app_data,
                                                                 purchases_structured_data)  # target dataframe

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
