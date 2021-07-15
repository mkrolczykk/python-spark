from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from dependencies.spark import start_spark
from pyspark.sql.functions import udf

import os

""" sql queries """
from sql_queries.BIGGEST_REVENUE_QUERY import biggest_revenue_query
from sql_queries.MOST_POPULAR_CHANNEL_QUERY import most_popular_channel_query
from sql_queries.CONVERT_ARRAY_OF_MAPS_TO_MAP import convert_array_of_maps_to_map
from sql_queries.AGGREGATE_BILLINGS import aggregate_billings

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
BIGGEST_REVENUE_OUTPUT = RESULT_FOLDER + "biggest_revenue.parquet"
MST_POPULAR_CHANNEL_OUTPUT = RESULT_FOLDER + "most_popular_channel.parquet"


def create_map_struct(source_df):
    result = source_df \
        .withColumn("attributes", explode(split(col("attributes"), ", "))) \
        .withColumn("key_temp", split(col("attributes"), ":").getItem(0)) \
        .withColumn("key", translate("key_temp", "{} '", "")) \
        .withColumn("value_temp", split(col("attributes"), ":").getItem(1)) \
        .withColumn("value", translate("value_temp", "{} '", "")) \
        .withColumn("attributes", create_map(col("key"), col("value"))) \
        .select(col("eventId"), col("attributes")) \
        .alias('attributes_to_map_struct')

    return result


def generate_sessions(df):
    w5 = Window.orderBy("userId", "eventTime")
    w6 = Window.partitionBy("session_id").orderBy("userId", "eventTime")
    w7 = Window.partitionBy("session_id")

    result = df \
        .withColumn("eventTime", to_timestamp("eventTime", 'yyyy-MM-dd HH:mm:ss.SSS')) \
        .withColumn("session_id", sum(when((col("eventType") == 'app_open'), lit(1))
                                      .otherwise(lit(0))).over(w5)) \
        .withColumn("rowNum", row_number().over(w6)) \
        .withColumn("max", max("rowNum").over(w7)) \
        .withColumn("first", when((col("rowNum") == 1) & (
        (col("eventType") == 'app_close')), lit(1))
                    .otherwise(lit(0))) \
        .filter('max>=2 and first=0') \
        .drop(*['rowNum', 'max', 'first']) \
        .orderBy("userId", "eventTime") \

    return result

def aggregate_user_data(df):
    result = df \
        .groupBy("userId", "session_id") \
        .agg(collect_list("eventType").alias("sessionId"),
             collect_list("attributes").alias("temp")) \
        .select("userId",
                "sessionId",
                expr(convert_array_of_maps_to_map).alias("attributes"))

    return result

def join_sessions_with_purchases_data(session_df, purchases_data_df):
    result = session_df \
        .join(purchases_data_df,
              session_df.attributes.purchase_id == purchases_data_df.purchaseId, "inner") \
        .select("userId", "eventTime", "purchaseId", "purchaseTime", "billingCost", "isConfirmed") \
        .sort(col("userId"), col("eventTime")) \
        .cache()

    return result

def create_target_dataframe_from(df_1, df_2):
    result = df_2 \
        .filter(df_2.purchaseId.isNotNull()) \
        .drop(col("eventTime")) \
        .join(df_1, df_2.userId == df_1.userId, "inner") \
        .select("purchaseId",
                "purchaseTime",
                "billingCost",
                "isConfirmed",
                col("attributes")["campaign_id"].alias("campaignId"),
                col("attributes")["channel_id"].alias("channelIid")) \
        .withColumn("sessionId", monotonically_increasing_id() + 1) \
        .cache() \

    return result

def calculate_campaigns_revenue(df):
    result = df \
        .filter(df.isConfirmed == True) \
        .groupBy("campaignId") \
        .agg(collect_list(col("billingCost")).alias("billings")) \
        .select(col("campaignId"),
                round(expr(aggregate_billings), 2).alias('revenue')) \
        .orderBy(col("revenue").desc()) \
        .limit(10)

    return result

def calculate_the_most_popular_channel_in_each_campaign(df):
    w3 = Window\
        .partitionBy("campaignId")\
        .orderBy(col("count").desc())

    result = df \
        .groupBy("campaignId", "channelIid") \
        .count() \
        .withColumn("row", row_number().over(w3)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed("count", "unique_sessions")

    return result

def main():
    # start spark session
    spark, spark_logger, spark_config = start_spark(
        app_name='Capstone project 1',
        files=['configs/config.json'])

    """ get .csv data """
    mobile_app_data = spark.read.csv(MOBILE_DATA_PATH,
                                     header=True,
                                     schema=MOBILE_SCHEMA
                                     ).alias("mobile_app_data")

    purchases_structured_data = spark.read.csv(PURCHASES_DATA_PATH,
                                               header=True,
                                               schema=USER_SCHEMA
                                               ).alias("purchases_data")

    attributes_to_map_struct = create_map_struct(mobile_app_data)   # generate sql map struct from mobile data attributes

    structured_mobile_data = mobile_app_data.alias("m") \
        .join(attributes_to_map_struct.alias("a"), attributes_to_map_struct.eventId == mobile_app_data.eventId, "left") \
        .select("m.userId", "m.eventId", "m.eventType", "m.eventTime", "a.attributes") \

    result = generate_sessions(structured_mobile_data).cache()  # split data into unique sessions (session starts with app_open event and finishes with app_close)

    aggr_data = aggregate_user_data(result)

    joined_data = join_sessions_with_purchases_data(result, purchases_structured_data)

    target_dataframe = create_target_dataframe_from(aggr_data, joined_data)     # target dataframe

    """ TASK #2 - Calculate Marketing Campaigns And Channels Statistics """

    """ SQL version """

    target_dataframe.registerTempTable("target_dataframe")
    biggest_revenue = spark.sql(biggest_revenue_query)
    mst_popular_channel = spark.sql(most_popular_channel_query)

    """ SQL version END """

    """ dataframe API version """
    '''
    biggest_revenue = calculate_campaigns_revenue(target_dataframe)
    mst_popular_channel = calculate_the_most_popular_channel_in_each_campaign(target_dataframe)
    '''
    """ dataframe API version END """

    """ show results in console """
    target_dataframe.show(10, False)    # target dataframe result
    biggest_revenue.show()  # biggest revenue result
    mst_popular_channel.show(10, False)     # most popular channel result

    """ save results as parquet files """
    target_dataframe.write.parquet(TARGET_DATAFRAME_OUTPUT, mode='overwrite')
    target_dataframe.write.parquet(BIGGEST_REVENUE_OUTPUT, mode='overwrite')
    target_dataframe.write.parquet(MST_POPULAR_CHANNEL_OUTPUT, mode='overwrite')


if __name__ == '__main__':
    main()
