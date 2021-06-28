import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # root folder path

# Input
MOBILE_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/mobile_app_clickstream/*.csv.gz")
PURCHASES_DATA_PATH = os.path.join(ROOT_DIR, "capstone-dataset/user_purchases/*.csv.gz")

# Output
RESULT_FOLDER = os.path.join(ROOT_DIR, "result_data/")
TARGET_DATAFRAME_OUTPUT = RESULT_FOLDER + "data_frame.parquet"
BIGGEST_REVENUE_OUTPUT = RESULT_FOLDER + "biggest_revenue.parquet"
MST_POPULAR_CHANNEL_OUTPUT = RESULT_FOLDER + "most_popular_channel.parquet"