from pyspark.sql import SparkSession
from pyspark import SparkFiles
from dependencies import logger
from os import listdir, path
import json

def start_spark(app_name='Capstone project 1',
                master='local[*]',
                jar_packages=[],
                files=[],
                spark_config=None):

    if spark_config is None:
        spark_config = {
            "spark.executor.memory": "2048m",
            "spark.driver.memory": "2048m",
        }
    spark = SparkSession \
        .builder \
        .master(master) \
        .appName(app_name)

    # create Spark JAR packages string
    spark_jars_packages = ','.join(list(jar_packages))
    spark.config('spark.jars.packages', spark_jars_packages)

    spark_files = ','.join(list(files))
    spark.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark.getOrCreate()
    spark_logger = logger.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict

