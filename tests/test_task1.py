from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, DoubleType, BooleanType
from dependencies.spark import start_spark

import unittest
from chispa.dataframe_comparer import assert_schema_equality, assert_df_equality

import datetime

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
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.spark_logger, cls.spark_config = start_spark(
            app_name='Capstone project 1',
            files=['../configs/config.json']
        )

        cls.mobile_app_data = cls.spark.read.csv(TEST_MOBILE_DATA_PATH,
                                                 header=True,
                                                 schema=MOBILE_SCHEMA,
                                                 sep='\t'
                                                 ).alias("mobile_app_data")

        cls.purchases_data = cls.spark.read.csv(TEST_PURCHASES_DATA_PATH,
                                                header=True,
                                                schema=USER_SCHEMA,
                                                sep='\t'
                                                ).alias("purchases_data")

        cls.TARGET_DATAFRAME_SCHEMA = StructType([
            StructField('purchaseId', StringType(), True),
            StructField('purchaseTime', TimestampType(), True),
            StructField('billingCost', DoubleType(), True),
            StructField('isConfirmed', BooleanType(), True),
            StructField('sessionId', StringType(), True),
            StructField('campaignId', StringType(), True),
            StructField('channelIid', StringType(), True)
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

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
        expected_rows_number = 46  # expected number of rows after process data and explode 'attributes' column
        # when
        result = generate_structured_mobile_data(self.mobile_app_data).count()

        # then
        self.assertEqual(expected_rows_number, result)

    """ check generate_structured_mobile_data generated data completeness """
    def test_generate_structured_mobile_data_completeness(self):
        # given
        expected_rows_number = 46
        expected_attributes_number = 20
        expected_empty_attributes_number = 26

        # when
        result = generate_structured_mobile_data(self.mobile_app_data)
        number_of_rows = result.count()
        non_empty_attributes = result.filter(result['attributes'].isNotNull()).count()
        empty_attributes = result.filter(result['attributes'].isNull()).count()

        # then
        self.assertEqual(expected_rows_number, number_of_rows)
        self.assertEqual(expected_attributes_number, non_empty_attributes)
        self.assertEqual(expected_empty_attributes_number, empty_attributes)

    """ check the number of generated sessions with correct organized data """
    def test_generate_sessions_with_organized_data(self):
        # given
        expected_generated_sessions = 7  # expected number of sessions (session starts with app_open event and finishes with app_close)
        df = generate_structured_mobile_data(self.mobile_app_data)

        # when
        result = generate_sessions(df) \
            .select("sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check the number of generated sessions with not perfect organized data """
    def test_generate_sessions_with_non_organized_data(self):
        # given
        expected_generated_sessions = 3
        sample_data = self.spark.createDataFrame(
            [[4, 'u1_e1', 'search_product', '2020-01-01 10:33:00.000', None],
             [1, 'u1_e2', 'app_open', '2020-01-01 12:31:00.000',
              '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'],
             [1, 'u1_e3', 'search_product', '2020-01-01 12:31:30.000', None],
             [1, 'u1_e4', 'view_product_details', '2020-01-01 12:32:00.000', None],
             [1, 'u1_e5', 'purchase', '2020-01-01 12:33:00.000', '{{"purchase_id": "p1"}}'],
             [1, 'u1_e5', 'app_close', '2020-01-01 12:35:00.000', None],
             [2, 'u2_e1', 'app_open', '2021-01-01 12:35:00.000',
              '{{"campaign_id": "cmp2",  "channel_id": "Google Ads"}}'],
             [2, 'u2_e2', 'search_product', '2021-01-01 12:36:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:39:00.000', '{{"purchase_id": "p2"}}'],
             [2, 'u2_e2', 'search_product', '2021-01-01 12:41:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:42:00.000', '{{"purchase_id": "p3"}}'],
             [2, 'u2_e4', 'app_close', '2021-01-01 12:49:00.000', None],
             [3, 'u3_e1', 'app_open', '2020-01-01 15:31:00.000',
              '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'],
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
        result = generate_sessions(df) \
            .select("sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check the number of generated sessions with no given data """
    def test_generate_sessions_with_empty_data(self):
        # given
        expected_generated_sessions = 0
        df = self.spark.createDataFrame(data=[], schema=MOBILE_SCHEMA)

        # when
        result = generate_sessions(df) \
            .select("sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check the number of generated sessions when session doesn't start from 'app_open' """
    def test_generate_sessions_with_session_data_not_started_from_app_open(self):
        # given
        expected_generated_sessions = 0
        sample_data = self.spark.createDataFrame(
            [[1, 'u1_e3', 'search_product', '2020-01-01 12:31:30.000', None],
             [1, 'u1_e4', 'view_product_details', '2020-01-01 12:32:00.000', None],
             [1, 'u1_e5', 'purchase', '2020-01-01 12:33:00.000', '{{"purchase_id": "p1"}}'],
             [1, 'u1_e5', 'app_close', '2020-01-01 12:35:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:39:00.000', '{{"purchase_id": "p2"}}'],
             [2, 'u2_e2', 'search_product', '2021-01-01 12:41:00.000', None],
             [2, 'u2_e3', 'purchase', '2021-01-01 12:42:00.000', '{{"purchase_id": "p3"}}'],
             [2, 'u2_e4', 'app_close', '2021-01-01 12:49:00.000', None],
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
        result = generate_sessions(df) \
            .select("sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check generate_sessions return dataframe columns correctness """
    def test_generate_sessions_check_columns(self):
        # given
        expected_number = 6
        expected_columns = ['userId', 'eventId', 'eventType', 'eventTime', 'attributes', 'sessionId']
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
        expected_number = 5
        expected_columns = ['userId', 'session_eventTypes', 'sessionId', 'campaign', 'purchases']
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
        expected_structure = self.TARGET_DATAFRAME_SCHEMA
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
        expected_columns = ['purchaseId', 'purchaseTime', 'billingCost', 'isConfirmed', 'sessionId', 'campaignId',
                            'channelIid']
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
        expected_df = self.spark.createDataFrame(
            [['p1', datetime.datetime(2019, 1, 1, 0, 1, 5), 100.5, True, '2', 'cmp1', 'GoogleAds'],
             ['p2', datetime.datetime(2019, 1, 1, 0, 3, 10), 200.0, True, '4', 'cmp1', 'YandexAds'],
             ['p3', datetime.datetime(2019, 1, 1, 1, 12, 15), 300.0, False, '10', 'cmp1', 'GoogleAds'],
             ['p4', datetime.datetime(2019, 1, 1, 2, 13, 5), 50.2, True, '12', 'cmp2', 'YandexAds'],
             ['p5', datetime.datetime(2019, 1, 1, 2, 15, 5), 75.0, True, '12', 'cmp2', 'YandexAds'],
             ['p6', datetime.datetime(2019, 1, 2, 13, 3, 0), 99.0, False, '14', 'cmp2', 'YandexAds'],
             ],
            schema=self.TARGET_DATAFRAME_SCHEMA)
        df = aggregate_mobile_data(generate_sessions(generate_structured_mobile_data(self.mobile_app_data)))

        # when
        result = create_target_dataframe_from(df, self.purchases_data)

        # then
        assert_df_equality(expected_df, result)


suite = unittest.TestLoader().loadTestsFromTestCase(TestTask1)
unittest.TextTestRunner(verbosity=2).run(suite)
