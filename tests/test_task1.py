from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, DoubleType, BooleanType
from pyspark import Row
from dependencies.spark import start_spark

import unittest
from chispa.dataframe_comparer import assert_schema_equality

from jobs.analyze_marketing_data.task1 import (
    MOBILE_SCHEMA,
    USER_SCHEMA,
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
                                                   schema=MOBILE_SCHEMA,
                                                   sep='\t'
                                                   ).alias("mobile_app_data")

        self.purchases_data = self.spark.read.csv(TEST_PURCHASES_DATA_PATH,
                                                  header=True,
                                                  schema=USER_SCHEMA,
                                                  sep='\t'
                                                  ).alias("purchases_data")

    """ check if generated mobile data structure is correct """
    def test_generate_structured_mobile_data_struct(self):
        # given
        structure = StructType([
            StructField('userId', StringType(), True),
            StructField('eventId', StringType(), True),
            StructField('eventType', StringType(), True),
            StructField('eventTime', TimestampType(), True),
            StructField('attributes', MapType(StringType(), StringType()), True)]
        )
        expected_df_structure = self.spark.createDataFrame(data=[], schema=structure)

        # when
        result = generate_structured_mobile_data(self.mobile_app_data)

        # then
        assert_schema_equality(expected_df_structure.schema, result.schema)

    """ check columns correctness """
    def test_generate_structured_mobile_data_check_columns(self):
        # given
        expected_number = 5
        expected_columns = ['userId', 'eventId', 'eventType', 'eventTime', 'attributes']

        # when
        result = generate_structured_mobile_data(self.mobile_app_data)
        columns = result.columns

        # then
        self.assertEqual(expected_number, len(columns))
        self.assertCountEqual(expected_columns, columns)

    """ check the behaviour of generate_structured_mobile_data while processing no data """
    def test_generate_structured_mobile_data_no_data(self):
        # given
        df = self.spark.createDataFrame(data=[], schema=MOBILE_SCHEMA)
        # when
        result = generate_structured_mobile_data(df)
        mvv_array = [int(row.mvv) for row in result.collect()]
        # then
        self.assertListEqual([], mvv_array)

    """ check the behaviour of generate_structured_mobile_data with given data """
    def test_generate_structured_mobile_data_with_data(self):
        # given
        expected_rows_number = 46   # expected number of rows after process data and explode 'attributes' column
        # when
        result = generate_structured_mobile_data(self.mobile_app_data).count()

        # then
        self.assertEqual(expected_rows_number, result)

    """ check the number of generated sessions with correct organized data """
    def test_generate_sessions_with_organized_data(self):
        # given
        expected_generated_sessions = 7   # expected number of sessions (session starts with app_open event and finishes with app_close)
        df = generate_structured_mobile_data(self.mobile_app_data)

        # when
        result = generate_sessions(df)\
            .select("session_id")\
            .distinct()\
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check the number of generated sessions with not perfect organized data """
    def test_generate_sessions_with_non_organized_data(self):
        # given
        expected_generated_sessions = 3
        sample_data = self.spark.createDataFrame(
            [[4, 'u1_e1', 'search_product', '2020-01-01 10:33:00.000', None],
             [1, 'u1_e2', 'app_open', '2020-01-01 12:31:00.000', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'],
             [1, 'u1_e3', 'search_product', '2020-01-01 12:31:30.000', None],
             [1, 'u1_e4', 'view_product_details', '2020-01-01 12:32:00.000', None],
             [1, 'u1_e5', 'purchase', '2020-01-01 12:33:00.000', '{{"purchase_id": "p1"}}'],
             [1, 'u1_e5', 'app_close', '2020-01-01 12:35:00.000', None],
             [2, 'u2_e1', 'app_open', '2021-01-01 12:35:00.000', '{{"campaign_id": "cmp2",  "channel_id": "Google Ads"}}'],
             [2, 'u2_e2', 'search_product', '2021-01-01 12:36:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:39:00.000', '{{"purchase_id": "p2"}}'],
             [2, 'u2_e2', 'search_product', '2021-01-01 12:41:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:42:00.000', '{{"purchase_id": "p3"}}'],
             [2, 'u2_e4', 'app_close', '2021-01-01 12:49:00.000', None],
             [3, 'u3_e1', 'app_open', '2020-01-01 15:31:00.000', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'],
             [3, 'u3_e2', 'search_product', '2020-01-01 15:31:30.000', None],
             [3, 'u3_e3', 'view_product_details', '2020-01-01 15:32:00.000', None],
             [3, 'u3_e4', 'purchase', '2020-01-01 15:33:00.000', '{{"purchase_id": "p1"}}'],
             [3, 'u3_e4', 'purchase', '2020-01-01 15:34:00.000', '{{"purchase_id": "p4"}}'],
             [3, 'u3_e4', 'purchase', '2020-01-01 15:35:00.000', '{{"purchase_id": "p5"}}'],
             [3, 'u3_e5', 'app_close', '2020-01-01 15:39:00.000', None],
             ],
            ['userId', 'eventId', 'eventType', 'eventTime', 'attributes']
        )
        df = generate_structured_mobile_data(sample_data)

        # when
        result = generate_sessions(df)\
            .select("session_id")\
            .distinct()\
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check generate_sessions return dataframe columns correctness """
    def test_generate_sessions_check_columns(self):
        # given
        expected_number = 6
        expected_columns = ['userId', 'eventId', 'eventType', 'eventTime', 'attributes', 'session_id']
        df = generate_structured_mobile_data(self.mobile_app_data)

        # when
        result = generate_sessions(df)
        columns = result.columns

        # then
        self.assertEqual(expected_number, len(columns))
        self.assertCountEqual(expected_columns, columns)

    """ check aggregate_mobile_data return dataframe columns correctness """
    def test_aggregate_mobile_data_check_columns(self):
        # given
        expected_number = 4
        expected_columns = ['userId', 'sessionId', 'campaign', 'purchases']
        df = generate_sessions(generate_structured_mobile_data(self.mobile_app_data))

        # when
        result = aggregate_mobile_data(df)
        columns = result.columns

        # then
        self.assertEqual(expected_number, len(columns))
        self.assertCountEqual(expected_columns, columns)

    """ check aggregate_mobile_data number of rows with success campaign transactions """
    def test_aggregate_mobile_data_number_of_rows_with_campaign_success(self):
        # given
        expected_number_of_rows = 6
        df = generate_sessions(generate_structured_mobile_data(self.mobile_app_data))

        # when
        result = aggregate_mobile_data(df) \
            .count()

        # then
        self.assertEqual(expected_number_of_rows, result)

    """ check if generated target dataframe structure is correct """
    def test_create_target_dataframe_schema(self):
        # given
        expected_structure = StructType([
            StructField('purchaseId', StringType(), True),
            StructField('purchaseTime', TimestampType(), True),
            StructField('billingCost', DoubleType(), True),
            StructField('isConfirmed', BooleanType(), True),
            StructField('sessionId', StringType(), False),
            StructField('campaignId', StringType(), True),
            StructField('channelIid', StringType(), True)]
        )
        expected_df_structure = self.spark.createDataFrame(data=[], schema=expected_structure)
        df = aggregate_mobile_data(generate_sessions(generate_structured_mobile_data(self.mobile_app_data)))

        # when
        result = create_target_dataframe_from(df, self.purchases_data)

        # then
        assert_schema_equality(expected_df_structure.schema, result.schema)

    """ check create_target_dataframe return dataframe columns correctness """
    def test_create_target_dataframe_check_columns(self):
        # given
        expected_number = 7
        expected_columns = ['purchaseId', 'purchaseTime', 'billingCost', 'isConfirmed', 'sessionId', 'campaignId', 'channelIid']
        df = aggregate_mobile_data(generate_sessions(generate_structured_mobile_data(self.mobile_app_data)))

        # when
        result = create_target_dataframe_from(df, self.purchases_data)
        columns = result.columns

        # then
        self.assertEqual(expected_number, len(columns))
        self.assertCountEqual(expected_columns, columns)

    """ check create_target_dataframe purchases attribution projection correctness """
    def test_create_target_dataframe_purchases_attribution_projection_correctness(self):
        # given
        expected_number_of_rows = 6
        df = aggregate_mobile_data(generate_sessions(generate_structured_mobile_data(self.mobile_app_data)))

        # when
        result = create_target_dataframe_from(df, self.purchases_data) \
            .count()

        # then
        self.assertEqual(expected_number_of_rows, result)

    """ check create_target_dataframe generated data correctness """
    def test_create_target_dataframe_data_correctness(self):
        # given
        expected_target_df = [
            Row(purchaseId='p1', purchaseTime='2019-01-01 00:01:05', billingCost=100.5, isConfirmed=True, sessionId='1', campaignId='cmp1', channelIid='GoogleAds'),
            Row(purchaseId='p2', purchaseTime='2019-01-01 00:03:10', billingCost=200.0, isConfirmed=True, sessionId='2', campaignId='cmp1', channelIid='YandexAds'),
            Row(purchaseId='p3', purchaseTime='2019-01-01 01:12:15', billingCost=300.0, isConfirmed=False, sessionId='3', campaignId='cmp1', channelIid='GoogleAds'),
            Row(purchaseId='p4', purchaseTime='2019-01-01 02:13:05', billingCost=50.2, isConfirmed=True, sessionId='4', campaignId='cmp2', channelIid='YandexAds'),
            Row(purchaseId='p5', purchaseTime='2019-01-01 02:15:05', billingCost=75.0, isConfirmed=True, sessionId='5', campaignId='cmp2', channelIid='YandexAds'),
            Row(purchaseId='p6', purchaseTime='2019-01-02 13:03:00', billingCost=99.0, isConfirmed=False, sessionId='6', campaignId='cmp2', channelIid='YandexAds')
        ]
        df = aggregate_mobile_data(generate_sessions(generate_structured_mobile_data(self.mobile_app_data)))

        # when
        result = create_target_dataframe_from(df, self.purchases_data).collect()

        # then
        self.assertTrue([col in expected_target_df for col in result])


suite = unittest.TestLoader().loadTestsFromTestCase(TestTask1)
unittest.TextTestRunner(verbosity=2).run(suite)
