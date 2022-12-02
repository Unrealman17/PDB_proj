# conda install pyspark
# delta-spark==2.1.1
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.hadoop.hive.metastore.warehouse.dir","hive")\
    .config("spark.sql.catalogImplementation","hive")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
