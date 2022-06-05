import os
from jobs.analyze_marketing_data import task1, task2
from dependencies.spark import start_spark


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def main():
    print("Starting application...")
    # start spark session
    spark, spark_logger, spark_config = start_spark(
        app_name='Capstone project 1',
        files=['{}/configs/config.json'.format(ROOT_DIR)])

    task1.main(spark, spark_logger, spark_config)   # start task1
    task2.main(spark, spark_logger, spark_config)   # start task2

    # stop spark session
    spark.stop()


if __name__ == '__main__':
    main()

