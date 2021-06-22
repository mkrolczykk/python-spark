from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode, split, col, create_map, translate, collect_list, monotonically_increasing_id, expr
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

attributes_to_map_struct = mobile_app_data \
    .withColumn("attributes", explode(split(col("attributes"), ", "))) \
    .withColumn("key_temp", split(col("attributes"), ":").getItem(0)) \
    .withColumn("key", translate("key_temp", "{} '","")) \
    .withColumn("value_temp", split(col("attributes"), ":").getItem(1)) \
    .withColumn("value", translate("value_temp", "{} '","")) \
    .withColumn("attributes", create_map(col("key"), col("value"))) \
    .select(col("eventId"), col("attributes")) \
    .alias('attributes_to_map_struct')

structured_mobile_data = mobile_app_data.alias("m") \
    .join(attributes_to_map_struct.alias("a"), attributes_to_map_struct.eventId == mobile_app_data.eventId, "left") \
    .select("m.userId", "m.eventId", "m.eventType", "m.eventTime", "a.attributes") \
    .sort("m.userId", "m.eventTime")

joined_data = structured_mobile_data \
    .join(purchases_structured_data, structured_mobile_data.attributes.purchase_id == purchases_structured_data.purchaseId, "left") \
    .sort(col("userId"), col("eventTime")) \
    .cache()

aggr_data = joined_data \
    .groupBy("userId") \
    .agg(collect_list("eventType").alias("sessionId"), collect_list("attributes").alias("temp")) \
    .select("userId", "sessionId", expr('aggregate(slice(temp, 2, size(temp)), temp[0], (acc, element) -> map_concat(acc, element))').alias("attributes"))

target_dataframe = joined_data \
    .filter(joined_data.purchaseId.isNotNull()) \
    .select("userId", "purchaseId", "purchaseTime", "billingCost", "isConfirmed") \
    .join(aggr_data, joined_data.userId == aggr_data.userId, "inner") \
    .select("purchaseId",
            "purchaseTime",
            "billingCost",
            "isConfirmed",
            col("attributes")["campaign_id"].alias("campaignId"),
            col("attributes")["channel_id"].alias("channelIid")) \
    .withColumn("sessionId", monotonically_increasing_id() + 1) \
    .cache()



def main():
    target_dataframe.show(10, False)
    target_dataframe.printSchema()

if __name__ == '__main__':
    main()