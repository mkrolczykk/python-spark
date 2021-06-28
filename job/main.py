from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

from dependencies.spark import start_spark
from dependencies.configurations import *
from sql_queries import biggest_revenue_query, most_popular_channel_query



def main():
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
    # start spark session
    spark, spark_logger, spark_config = start_spark(
        app_name='Capstone project 1',
        files=['../configs/config.json'])

    # get .csv data
    mobile_app_data = load_csv_data(spark, MOBILE_DATA_PATH, MOBILE_SCHEMA).alias("mobile_app_data")
    purchases_structured_data = load_csv_data(spark, PURCHASES_DATA_PATH, USER_SCHEMA).alias("purchases_data")

    # generate sql map struct from mobile data attributes
    attributes_to_map_struct = create_map_struct(mobile_app_data)

    # create structured mobile dataframe and split data into unique sessions (session starts with app_open event and finishes with app_close)
    structured_mobile_data = prepare_mobile_data_and_generate_sessions(mobile_app_data, attributes_to_map_struct).cache()

    # structured_mobile_data.show(20, False)

    aggr_data = structured_mobile_data \
        .groupBy("userId", "session_id") \
        .agg(collect_list("eventType").alias("sessionId"), collect_list("attributes").alias("temp")) \
        .select("userId", "sessionId", expr('aggregate(slice(temp, 2, size(temp)), temp[0], (acc, element) -> map_concat(acc, element))').alias("attributes"))

    # aggr_data.show(20, False)

    joined_data = structured_mobile_data \
        .join(purchases_structured_data,
              structured_mobile_data.attributes.purchase_id == purchases_structured_data.purchaseId, "inner") \
        .select("userId", "eventTime", "purchaseId", "purchaseTime", "billingCost", "isConfirmed") \
        .sort(col("userId"), col("eventTime")) \
        .cache()

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
        .cache() \


    print("-------------target_dataframe-------------")
    # target_dataframe.printSchema()
    target_dataframe.show(10, False)
    target_dataframe.registerTempTable("target_dataframe")  # comment or uncomment depending on using pyspark dataframe API or plain sql

    print("-------------task_2.1-------------")

    # SQL version
    biggest_revenue = spark.sql(biggest_revenue_query)
    biggest_revenue.show()

    ''' 
    # dataframe API version
    biggest_revenue = target_dataframe \
        .filter(target_dataframe.isConfirmed == True) \
        .groupBy("campaignId") \
        .agg(collect_list(col("billingCost")).alias("billings")) \
        .select(col("campaignId"), round(expr('AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x)'), 2).alias('revenue')) \
        .orderBy(col("revenue").desc()) \
        .limit(10)

    biggest_revenue.show()
    '''
    print("-------------task_2.2-------------")

    # SQL version
    mst_popular_channel = spark.sql(most_popular_channel_query)
    mst_popular_channel.show(10, False)

    '''
    # dataframe API version
    w3 = Window.partitionBy("campaignId").orderBy(col("count").desc())
    
    mst_popular_channel = target_dataframe \
        .groupBy("campaignId", "channelIid") \
        .count() \
        .withColumn("row", row_number().over(w3)) \
        .filter(col("row") == 1).drop("row") \
        .withColumnRenamed("count", "unique_sessions")

    mst_popular_channel.show(10, False)
    '''

    # save results as parquet files
    target_dataframe.write.parquet(TARGET_DATAFRAME_OUTPUT, mode='overwrite')
    target_dataframe.write.parquet(BIGGEST_REVENUE_OUTPUT, mode='overwrite')
    target_dataframe.write.parquet(MST_POPULAR_CHANNEL_OUTPUT, mode='overwrite')


def load_csv_data(spark, data_path, data_schema):
    df = (
        spark
            .read
            .csv(data_path, header=True, schema=data_schema))

    return df

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

def prepare_mobile_data_and_generate_sessions(df, attributes):
    w5 = Window.orderBy("userId", "eventTime")
    w6 = Window.partitionBy("session_id").orderBy("userId", "eventTime")
    w7 = Window.partitionBy("session_id")

    result = df.alias("m") \
        .join(attributes.alias("a"), attributes.eventId == df.eventId, "left") \
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

    return result

def create_sample_data(spark):
    sample_data = spark.createDataFrame(
        [[1, '2020-01-01 10:33:00.000', 'search_product'], [1, '2020-01-01 12:31:00.000', 'app_open'], [1, '2020-01-01 12:31:00.000', 'app_open'],
         [1, '2020-01-01 12:32:00.000', 'search_product'], [1, '2020-01-01 12:33:00.000', 'search_product'], [1, '2020-01-01 13:00:00.000', 'view_product_details'],
         [1, '2020-01-01 13:01:00.000', 'purchase'], [1, '2020-01-01 13:02:00.000', 'app_close'], [1, '2020-01-01 13:03:00.000', 'app_open'],
         [1, '2020-01-01 13:06:00.000', 'search_product'], [1, '2020-01-01 13:09:00.000', 'app_close'], [2, '2020-01-01 12:31:00.000', 'app_open'],
         [2, '2020-01-01 12:31:00.000', 'app_open'], [2, '2020-01-01 12:32:00.000', 'search_product'], [2, '2020-01-01 12:33:00.000', 'search_product'],
         [2, '2020-01-01 13:00:00.000', 'view_product_details'],[2, '2020-01-01 13:09:00.000', 'app_close']],
        ['userId', 'eventTime', 'eventType']
    )

    return sample_data

if __name__ == '__main__':
    main()