import unittest

from dependencies.spark import start_spark

from jobs.analyze_marketing_data.task1 import (
    generate_structured_mobile_data,
    generate_sessions,
    aggregate_mobile_data,
    create_target_dataframe_from
)

TEST_MOBILE_DATA_PATH = 'test_data/mobile-app-clickstream_sample.tsv'
TEST_PURCHASES_DATA_PATH = 'test_data/purchases_sample.tsv'

class TestTask1(unittest.TestCase):
    def setUp(self):
        self.spark, self.spark_logger, self.spark_config = start_spark(
            app_name='Capstone project 1',
            files=['../configs/config.json']
        )

        self.mobile_app_data = self.spark.read.csv(TEST_MOBILE_DATA_PATH,
                                                   header=True,
                                                   sep='\t'
                                                   ).alias("mobile_app_data")

        self.purchases_data = self.spark.read.csv(TEST_PURCHASES_DATA_PATH,
                                                  header=True,
                                                  sep='\t'
                                                  ).alias("purchases_data")

    def test_create_map_struct(self):
        self.mobile_app_data.show(10, False)
        result = create_map_struct(self.mobile_app_data)
        result.show(10, False)


if __name__ == '__main__':
    print("")

