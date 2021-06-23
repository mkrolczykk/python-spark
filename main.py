from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode, split, col, create_map, translate, collect_list, monotonically_increasing_id, expr, round, to_timestamp, when, lit, row_number, sum, max
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Capstone project 1") \
    .getOrCreate()

sqlContext = SQLContext(spark)

MOBILE_DATA_PATH = "capstone-dataset/mobile_app_clickstream/*.csv.gz"
PURCHASES_DATA_PATH = "capstone-dataset/user_purchases/*.csv.gz"

mobile_schema = StructType([
    StructField('userId', StringType(), True),
    StructField('eventId', StringType(), True),
    StructField('eventType', StringType(), True),
    StructField('eventTime', TimestampType(), True),
    StructField('attributes', StringType(), True)
])
user_schema = StructType([
    StructField('purchaseId', StringType(), True),
    StructField('purchaseTime', TimestampType(), True),
    StructField('billingCost', DoubleType(), True),
    StructField('isConfirmed', BooleanType(), True),
])

mobile_app_data = spark.read.csv(MOBILE_DATA_PATH, header=True, schema=mobile_schema) \
    .alias('mobile_app_data') \
    .cache()

purchases_structured_data = spark.read.csv(PURCHASES_DATA_PATH, header=True, schema=user_schema) \
    .alias('purchases_data')

def main():
    attributes_to_map_struct = mobile_app_data \
        .withColumn("attributes", explode(split(col("attributes"), ", "))) \
        .withColumn("key_temp", split(col("attributes"), ":").getItem(0)) \
        .withColumn("key", translate("key_temp", "{} '", "")) \
        .withColumn("value_temp", split(col("attributes"), ":").getItem(1)) \
        .withColumn("value", translate("value_temp", "{} '", "")) \
        .withColumn("attributes", create_map(col("key"), col("value"))) \
        .select(col("eventId"), col("attributes")) \
        .alias('attributes_to_map_struct')
    '''
    # sample data
    sample_data = spark.createDataFrame(
        [[1, '2020-01-01 10:33:00.000', 'search_product'], [1, '2020-01-01 12:31:00.000', 'app_open'], [1, '2020-01-01 12:31:00.000', 'app_open'],
         [1, '2020-01-01 12:32:00.000', 'search_product'], [1, '2020-01-01 12:33:00.000', 'search_product'], [1, '2020-01-01 13:00:00.000', 'view_product_details'],
         [1, '2020-01-01 13:01:00.000', 'purchase'], [1, '2020-01-01 13:02:00.000', 'app_close'], [1, '2020-01-01 13:03:00.000', 'app_open'],
         [1, '2020-01-01 13:06:00.000', 'search_product'], [1, '2020-01-01 13:09:00.000', 'app_close'], [2, '2020-01-01 12:31:00.000', 'app_open'],
         [2, '2020-01-01 12:31:00.000', 'app_open'], [2, '2020-01-01 12:32:00.000', 'search_product'], [2, '2020-01-01 12:33:00.000', 'search_product'],
         [2, '2020-01-01 13:00:00.000', 'view_product_details'],[2, '2020-01-01 13:09:00.000', 'app_close']],
        ['userId', 'eventTime', 'eventType']
    )
    '''
    w5 = Window.orderBy("userId", "eventTime")
    w6 = Window.partitionBy("session_id").orderBy("userId", "eventTime")
    w7 = Window.partitionBy("session_id")

    structured_mobile_data = mobile_app_data.alias("m") \
        .join(attributes_to_map_struct.alias("a"), attributes_to_map_struct.eventId == mobile_app_data.eventId, "left") \
        .select("m.userId", "m.eventId", "m.eventType", "m.eventTime", "a.attributes") \
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
        .cache()

    # print("structured_mobile_data-------------------------------------")
    # structured_mobile_data.show(20, False)

    aggr_data = structured_mobile_data \
        .groupBy("userId", "session_id") \
        .agg(collect_list("eventType").alias("sessionId"), collect_list("attributes").alias("temp")) \
        .select("userId", "sessionId", expr(
        'aggregate(slice(temp, 2, size(temp)), temp[0], (acc, element) -> map_concat(acc, element))').alias(
        "attributes"))

    # print("aggr_data-------------------------------------")
    # aggr_data.show(20, False)

    joined_data = structured_mobile_data \
        .join(purchases_structured_data,
              structured_mobile_data.attributes.purchase_id == purchases_structured_data.purchaseId, "inner") \
        .select("userId", "eventTime", "purchaseId", "purchaseTime", "billingCost", "isConfirmed") \
        .sort(col("userId"), col("eventTime")) \
        .cache()

    # print("joined_data-------------------------------------")
    # joined_data.show(20, False)

    target_dataframe = joined_data \
        .filter(joined_data.purchaseId.isNotNull()) \
        .drop(col("eventTime")) \
        .join(aggr_data, joined_data.userId == aggr_data.userId, "inner") \
        .select("purchaseId",
                "purchaseTime",
                "billingCost",
                "isConfirmed",
                col("attributes")["campaign_id"].alias("campaignId"),
                col("attributes")["channel_id"].alias("channelIid")) \
        .withColumn("sessionId", monotonically_increasing_id() + 1) \
        .cache()

    print("target_dataframe-------------------------------------")
    target_dataframe.printSchema()
    target_dataframe.show(20, False)


    target_dataframe.show(10, False)
    target_dataframe.printSchema()


    biggest_revenue = target_dataframe \
        .filter(target_dataframe.isConfirmed == True) \
        .groupBy("campaignId") \
        .agg(collect_list(col("billingCost")).alias("billings")) \
        .select(col("campaignId"), round(expr('AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x)'), 2).alias('revenue')) \
        .orderBy(col("revenue").desc()) \

    biggest_revenue.show(10, False)


    w3 = Window.partitionBy("campaignId").orderBy(col("count").desc())
    mst_popular_channel = target_dataframe \
        .groupBy("campaignId", "channelIid") \
        .count() \
        .withColumn("row", row_number().over(w3)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed("count", "unique_sessions")

    mst_popular_channel.show(10, False)



if __name__ == '__main__':
    main()