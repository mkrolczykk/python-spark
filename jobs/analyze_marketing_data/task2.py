""" TASK #2 - Calculate Marketing Campaigns And Channels Statistics """
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from dependencies.logger import Log4j

""" sql queries """
from .sql_queries.BIGGEST_REVENUE_QUERY import biggest_revenue_query
from .sql_queries.MOST_POPULAR_CHANNEL_QUERY import most_popular_channel_query
from .sql_queries.AGGREGATE_BILLINGS import aggregate_billings

import os

TARGET_DATAFRAME_SCHEMA = StructType([
    StructField('purchaseId', StringType(), True),
    StructField('purchaseTime', TimestampType(), True),
    StructField('billingCost', DoubleType(), True),
    StructField('isConfirmed', BooleanType(), True),
    StructField('sessionId', StringType(), True),
    StructField('campaignId', StringType(), True),
    StructField('channelIid', StringType(), True)
])

# root folder path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Input
RESULT_FOLDER = os.path.join(ROOT_DIR, "result_data/")
TARGET_DATAFRAME_INPUT = RESULT_FOLDER + "data_frame.parquet"

# Output
BIGGEST_REVENUE_OUTPUT = RESULT_FOLDER + "biggest_revenue.parquet"
MST_POPULAR_CHANNEL_OUTPUT = RESULT_FOLDER + "most_popular_channel.parquet"


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
    w3 = Window \
        .partitionBy("campaignId") \
        .orderBy(col("count").desc())

    result = df \
        .groupBy("campaignId", "channelIid") \
        .count() \
        .withColumn("row", row_number().over(w3)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed("count", "unique_sessions")

    return result


def main(spark: SparkContext, spark_logger: Log4j, spark_config):

    target_dataframe = spark.read.parquet(TARGET_DATAFRAME_INPUT,
                                          header=True,
                                          schema=TARGET_DATAFRAME_SCHEMA,
                                          ).alias('target_dataframe')
    """ SQL version """

    target_dataframe.registerTempTable("target_dataframe")
    biggest_revenue = spark.sql(biggest_revenue_query)
    mst_popular_channel = spark.sql(most_popular_channel_query)

    """ END of SQL version """

    """ dataframe API version """
    '''
    biggest_revenue = calculate_campaigns_revenue(target_dataframe)
    mst_popular_channel = calculate_the_most_popular_channel_in_each_campaign(target_dataframe)
    '''
    """ END of dataframe API version  """

    """ show results in console """
    biggest_revenue.show()  # biggest revenue result
    mst_popular_channel.show(10, False)  # most popular channel result

    """ save results as parquet files """
    target_dataframe.write.parquet(BIGGEST_REVENUE_OUTPUT, mode='overwrite')
    target_dataframe.write.parquet(MST_POPULAR_CHANNEL_OUTPUT, mode='overwrite')


if __name__ == '__main__':
    main()
