from pyspark import Row
from dependencies.spark import start_spark

import unittest

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
        self.target_dataframe = self.spark.createDataFrame(data=[
            Row(purchaseId='p1', purchaseTime='2019-01-01 00:01:05', billingCost=100.5, isConfirmed=True, sessionId='1', campaignId='cmp1', channelIid='GoogleAds'),
            Row(purchaseId='p2', purchaseTime='2019-01-01 00:03:10', billingCost=200.0, isConfirmed=True, sessionId='2', campaignId='cmp1', channelIid='YandexAds'),
            Row(purchaseId='p3', purchaseTime='2019-01-01 01:12:15', billingCost=300.0, isConfirmed=False, sessionId='3', campaignId='cmp1', channelIid='GoogleAds'),
            Row(purchaseId='p4', purchaseTime='2019-01-01 02:13:05', billingCost=50.2, isConfirmed=True, sessionId='4', campaignId='cmp2', channelIid='YandexAds'),
            Row(purchaseId='p5', purchaseTime='2019-01-01 02:15:05', billingCost=75.0, isConfirmed=True, sessionId='5', campaignId='cmp2', channelIid='YandexAds'),
            Row(purchaseId='p6', purchaseTime='2019-01-02 13:03:00', billingCost=99.0, isConfirmed=False, sessionId='6', campaignId='cmp2', channelIid='YandexAds')
        ])

        self.expected_biggest_revenue_result = [
            Row(campaignId='cmp1', revenue=300.5),
            Row(campaignId='cmp2', revenue=125.2)
        ]

        self.expected_channel_engagement_performance = [
            Row(campaignId='cmp2', channelIid='YandexAds', unique_sessions='3'),
            Row(campaignId='cmp1', channelIid='GoogleAds', unique_sessions='2')
        ]

    """ check calculate_campaigns_revenue generated result correctness """
    def test_calculate_campaigns_revenue_result_correctness(self):
        # given
        expected_biggest_revenue_result = self.expected_biggest_revenue_result

        # when
        result = calculate_campaigns_revenue(self.target_dataframe).collect()

        # then
        self.assertTrue([col in expected_biggest_revenue_result for col in result])

    """ check calculate_campaigns_revenue SQL version generated result correctness """
    def test_calculate_campaigns_revenue_sql_result_correctness(self):
        # given
        expected_biggest_revenue_result = self.expected_biggest_revenue_result

        # when
        self.target_dataframe.registerTempTable("target_dataframe")
        result = calculate_campaigns_revenue_sql(spark_context=self.spark).collect()

        # then
        self.assertTrue([col in expected_biggest_revenue_result for col in result])

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
        result = channels_engagement_performance(self.target_dataframe).collect()

        # then
        self.assertTrue([col in expected_channel_engagement_performance for col in result])

    """ check channels_engagement_performance SQL version generated result correctness """
    def test_channels_engagement_performance_result_correctness(self):
        # given
        expected_channel_engagement_performance = self.expected_channel_engagement_performance

        # when
        result = channels_engagement_performance_sql(spark_context=self.spark).collect()

        # then
        self.assertTrue([col in expected_channel_engagement_performance for col in result])

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
