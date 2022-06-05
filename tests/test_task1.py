import os

from pyspark.sql import Window
from pyspark.sql.functions import monotonically_increasing_id, col, sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, DoubleType, BooleanType
from dependencies.spark import start_spark

import unittest
from chispa.dataframe_comparer import assert_schema_equality, assert_df_equality
from chispa.column_comparer import assert_column_equality

import datetime

from jobs.analyze_marketing_data.task1 import (
    MOBILE_SCHEMA,
    USER_SCHEMA,
    generate_purchases_attribution_projection,
    prepare_attributes_udf,
    generate_sessions_udf,
    create_target_dataframe_udf
)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_MOBILE_DATA_PATH = ROOT_DIR + '/tests/test_data/mobile-app-clickstream_sample.tsv'
TEST_PURCHASES_DATA_PATH = ROOT_DIR + '/tests/test_data/purchases_sample.tsv'

class TestTask1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark, cls.spark_logger, cls.spark_config = start_spark(
            app_name='Capstone project 1',
            files=['{}/configs/config.json'.format(ROOT_DIR)]
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

        cls.generate_sessions_sample_data = \
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
             ]

    """ check create_target_dataframe generated data correctness """
    def test_generate_purchases_attribution_projection_correctness(self):
        # given
        expected_df = self.spark.createDataFrame(
            [['p1', datetime.datetime(2019, 1, 1, 0, 1, 5), 100.5, True, '2', 'cmp1', 'Google Ads'],
             ['p2', datetime.datetime(2019, 1, 1, 0, 3, 10), 200.0, True, '4', 'cmp1', 'Yandex Ads'],
             ['p3', datetime.datetime(2019, 1, 1, 1, 12, 15), 300.0, False, '10', 'cmp1', 'Google Ads'],
             ['p4', datetime.datetime(2019, 1, 1, 2, 13, 5), 50.2, True, '12', 'cmp2', 'Yandex Ads'],
             ['p5', datetime.datetime(2019, 1, 1, 2, 15, 5), 75.0, True, '12', 'cmp2', 'Yandex Ads'],
             ['p6', datetime.datetime(2019, 1, 2, 13, 3, 0), 99.0, False, '14', 'cmp2', 'Yandex Ads']
             ],
            schema=self.TARGET_DATAFRAME_SCHEMA)

        # when
        result = generate_purchases_attribution_projection(self.mobile_app_data, self.purchases_data)

        # then
        assert_df_equality(expected_df, result)

    """ check prepare_attributes_udf generated data correctness """
    def test_prepare_attributes_udf_data_correctness(self):
        # given
        data = [
            ('app_open', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{"campaign_id": "cmp1",  "channel_id": "Google Ads"}'),
            ('app_open', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}'),
            ('app_open', '', ''),
            ('purchase', '{{"purchase_id": "p3"}}', '{"purchase_id": "p3"}'),
            ('purchase', '{"purchase_id": "p3"}', '{"purchase_id": "p3"}'),
            ('purchase', '', ''),
        ]
        df = self.spark.createDataFrame(data, ['eventType', 'attributes', 'expected_attributes'])

        # when
        result = df\
            .withColumn("attr_func_result", prepare_attributes_udf(df['eventType'], df['attributes']))

        # then
        assert_column_equality(result, 'expected_attributes', 'attr_func_result')

    """ check prepare_attributes_udf event type reaction """
    def test_prepare_attributes_udf_event_reaction(self):
        # given
        data = [
            ('search_product', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'),
            ('view_product_details', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'),
            ('PRODUCT_DETAILS', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'),
            ('APP_CLOSE', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}'),
            ('app_open', '{{"campaign_id": "cmp1",  "channel_id": "Google Ads"}}', '{"campaign_id": "cmp1",  "channel_id": "Google Ads"}'),
            ('app_open', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}'),
            ('app_open', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}', '{"campaign_id": "cmp2",  "channel_id": "Facebook Ads"}'),
            ('purchase', '{{"purchase_id": "p3"}}', '{"purchase_id": "p3"}'),
            ('purchase', '{"purchase_id": "p3"}', '{"purchase_id": "p3"}'),
        ]
        df = self.spark.createDataFrame(data, ['eventType', 'attributes', 'expected_attributes'])

        # when
        result = df \
            .withColumn("attr_func_result", prepare_attributes_udf(df['eventType'], df['attributes']))

        # then
        assert_column_equality(result, 'expected_attributes', 'attr_func_result')

    """ check the number of generated sessions with correct organized data """
    def test_generate_sessions_udf_with_organized_data(self):
        # given
        expected_generated_sessions = 7
        df = self.mobile_app_data
        w1 = Window.partitionBy('userId').orderBy('eventTime')

        # when
        result = df \
            .withColumn('sessionId_temp', generate_sessions_udf(df['eventType'], monotonically_increasing_id() + 1)) \
            .withColumn('sessionId', sum(col('sessionId_temp')).over(w1)) \
            .select("sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check the number of generated sessions with not perfect organized data """
    def test_generate_sessions_udf_with_non_organized_data(self):
        # given
        expected_generated_sessions = 3
        df = self.spark.createDataFrame(self.generate_sessions_sample_data, ['userId', 'eventId', 'eventType', 'eventTime', 'attributes'])

        # when
        result = df \
            .withColumn('test_sessionId', generate_sessions_udf(df['eventType'], monotonically_increasing_id() + 1)) \
            .select("test_sessionId") \
            .distinct() \
            .count()

        # then
        self.assertEqual(expected_generated_sessions, result)

    """ check if generated udf target dataframe structure is as required """
    def test_create_target_dataframe_udf_schema(self):
        # given
        expected_df_structure = self.spark.createDataFrame(data=[], schema=self.TARGET_DATAFRAME_SCHEMA)
        df = self.mobile_app_data

        # when
        result = create_target_dataframe_udf(df, self.purchases_data)

        # then
        assert_schema_equality(expected_df_structure.schema, result.schema)

    """ check create_target_dataframe_udf generated data correctness """
    def test_create_target_dataframe_udf_data_correctness(self):
        # given
        expected_df = self.spark.createDataFrame(
            [['p1', datetime.datetime(2019, 1, 1, 0, 1, 5), 100.5, True, '1', 'cmp1', 'Google Ads'],
             ['p2', datetime.datetime(2019, 1, 1, 0, 3, 10), 200.0, True, '8', 'cmp1', 'Yandex Ads'],
             ['p3', datetime.datetime(2019, 1, 1, 1, 12, 15), 300.0, False, '38', 'cmp1', 'Google Ads'],
             ['p4', datetime.datetime(2019, 1, 1, 2, 13, 5), 50.2, True, '64', 'cmp2', 'Yandex Ads'],
             ['p5', datetime.datetime(2019, 1, 1, 2, 15, 5), 75.0, True, '64', 'cmp2', 'Yandex Ads'],
             ['p6', datetime.datetime(2019, 1, 2, 13, 3, 0), 99.0, False, '99', 'cmp2', 'Yandex Ads']
             ],
            schema=self.TARGET_DATAFRAME_SCHEMA)

        # when
        result = create_target_dataframe_udf(self.mobile_app_data, self.purchases_data)

        # then
        assert_df_equality(expected_df, result, ignore_row_order=True)


suite = unittest.TestLoader().loadTestsFromTestCase(TestTask1)
unittest.TextTestRunner(verbosity=2).run(suite)
