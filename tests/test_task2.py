from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType, LongType
from dependencies.spark import start_spark

import unittest
from chispa.dataframe_comparer import assert_df_equality

import datetime

from jobs.analyze_marketing_data.task2 import (
    calculate_campaigns_revenue,
    calculate_campaigns_revenue_sql,
    channels_engagement_performance,
    channels_engagement_performance_sql
)

class TestTask2(unittest.TestCase):
    def setUp(self):
        self.spark, self.spark_logger, self.spark_config = start_spark(
            app_name='Capstone project 1',
            files=['../configs/config.json']
        )

        self.target_dataframe = self.spark.createDataFrame(
            [['p1', datetime.datetime(2019, 1, 1, 0, 1, 5), 100.5, True, '2', 'cmp1', 'GoogleAds'],
             ['p2', datetime.datetime(2019, 1, 1, 0, 3, 10), 200.0, True, '4', 'cmp1', 'YandexAds'],
             ['p3', datetime.datetime(2019, 1, 1, 1, 12, 15), 300.0, False, '10', 'cmp1', 'GoogleAds'],
             ['p4', datetime.datetime(2019, 1, 1, 2, 13, 5), 50.2, True, '12', 'cmp2', 'YandexAds'],
             ['p5', datetime.datetime(2019, 1, 1, 2, 15, 5), 75.0, True, '12', 'cmp2', 'YandexAds'],
             ['p6', datetime.datetime(2019, 1, 2, 13, 3, 0), 99.0, False, '14', 'cmp2', 'YandexAds'],
             ],
            schema=StructType([
            StructField('purchaseId', StringType(), True),
            StructField('purchaseTime', TimestampType(), True),
            StructField('billingCost', DoubleType(), True),
            StructField('isConfirmed', BooleanType(), True),
            StructField('sessionId', StringType(), True),
            StructField('campaignId', StringType(), True),
            StructField('channelIid', StringType(), True)
            ])
        )

        self.expected_biggest_revenue_result = self.spark.createDataFrame(
            [['cmp1', 300.5],
             ['cmp2', 125.2]],
            schema=StructType([
                StructField('campaignId', StringType(), True),
                StructField('revenue', DoubleType(), True),
            ])
        )

        self.expected_channel_engagement_performance = self.spark.createDataFrame(
            [['cmp2', 'YandexAds', 3],
             ['cmp1', 'GoogleAds', 2]],
            schema=StructType([
            StructField('campaignId', StringType(), True),
            StructField('channelIid', StringType(), True),
            StructField('unique_sessions', LongType(), False),
            ])
        )

    """ check calculate_campaigns_revenue generated result correctness """
    def test_calculate_campaigns_revenue_result_correctness(self):
        # given
        expected_biggest_revenue_result = self.expected_biggest_revenue_result

        # when
        result = calculate_campaigns_revenue(self.target_dataframe)

        # then
        assert_df_equality(expected_biggest_revenue_result, result)

    """ check calculate_campaigns_revenue SQL version generated result correctness """
    def test_calculate_campaigns_revenue_sql_result_correctness(self):
        # given
        expected_biggest_revenue_result = self.expected_biggest_revenue_result

        # when
        self.target_dataframe.registerTempTable("target_dataframe")
        result = calculate_campaigns_revenue_sql(spark_context=self.spark)

        # then
        assert_df_equality(expected_biggest_revenue_result, result)

    """ check calculate_campaigns_revenue returns no more than top 10 campaigns """
    def test_calculate_campaigns_revenue_returns_max_10_rows(self):
        # given
        expected_max_row_number = 10

        # when
        result = calculate_campaigns_revenue(self.target_dataframe) \
            .count()

        # then
        self.assertLessEqual(result, expected_max_row_number)

    """ check calculate_campaigns_revenue SQL version returns no more than top 10 campaigns """
    def test_calculate_campaigns_revenue_sql_returns_max_10_rows(self):
        # given
        expected_max_row_number = 10

        # when
        self.target_dataframe.registerTempTable("target_dataframe")
        result = calculate_campaigns_revenue_sql(spark_context=self.spark).count()

        # then
        self.assertLessEqual(result, expected_max_row_number)

    """ check the behaviour of calculate_campaigns_revenue while processing no data """
    def test_calculate_campaigns_revenue_no_data(self):
        # given
        df = self.spark.createDataFrame(data=[], schema=self.target_dataframe.schema)

        # when
        result = calculate_campaigns_revenue(df)
        mvv_array = [int(row.mvv) for row in result.collect()]

        # then
        self.assertListEqual([], mvv_array)

    """ check channels_engagement_performance generated result correctness """
    def test_channels_engagement_performance_result_correctness(self):
        # given
        expected_channel_engagement_performance = self.expected_channel_engagement_performance

        # when
        result = channels_engagement_performance(self.target_dataframe)

        # then
        assert_df_equality(expected_channel_engagement_performance, result)

    """ check channels_engagement_performance SQL version generated result correctness """
    def test_channels_engagement_performance_result_sql_correctness(self):
        # given
        expected_channel_engagement_performance = self.expected_channel_engagement_performance

        # when
        result = channels_engagement_performance_sql(spark_context=self.spark)

        # then
        assert_df_equality(expected_channel_engagement_performance, result)

    """ check the behaviour of channels_engagement_performance while processing no data """
    def test_channels_engagement_performance_with_no_data(self):
        # given
        df = self.spark.createDataFrame(data=[], schema=self.target_dataframe.schema)

        # when
        result = channels_engagement_performance(df)
        mvv_array = [int(row.mvv) for row in result.collect()]

        # then
        self.assertListEqual([], mvv_array)


suite = unittest.TestLoader().loadTestsFromTestCase(TestTask2)
unittest.TextTestRunner(verbosity=2).run(suite)
