# conda install pyspark
# delta-spark==2.1.1
import json
import pyspark
from delta import *
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.hadoop.hive.metastore.warehouse.dir", "hive")\
    .config("spark.sql.catalogImplementation", "hive")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


def read_config() -> dict:
    with open("config.json") as f:
        config = json.load(f)
    config["tables"].append('extra')
    return config

def task(function):
    def res(*args, spark_context: SparkSession = None, **kwargs):
        if not spark_context: 
            spark_context = spark
        return function(*args, spark_context = spark_context, **kwargs)
    return res