from dependencies.spark import start_spark
from jobs.analyze_marketing_data import task1, task2

def main():
    # start spark session
    spark, spark_logger, spark_config = start_spark(
        app_name='Capstone project 1',
        files=['../configs/config.json'])

    task1.main(spark, spark_logger, spark_config)   # start task1
    task2.main(spark, spark_logger, spark_config)   # start task2

    # stop spark session
    spark.stop()


if __name__ == '__main__':
    main()

