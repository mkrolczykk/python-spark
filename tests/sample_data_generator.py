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

