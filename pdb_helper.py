# conda install pyspark
# delta-spark==2.1.1
import json
import pyspark
from delta import *
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("App") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.hadoop.hive.metastore.warehouse.dir", "hive")\
    .config("spark.sql.catalogImplementation", "hive")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# TODO: write dynamic class
def read_config() -> dict:
    with open("config.json") as f:
        config = json.load(f)
    config["tables"].append('extra')
    return config


def task(function):
    def res(*args, spark_context: SparkSession = None, config: dict = None, **kwargs):
        spark_context = spark_context or spark
        config = config or read_config()

        return function(*args, spark_context=spark_context, config=config, **kwargs)
    return res

