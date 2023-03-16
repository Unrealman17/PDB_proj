import os
import sys
import urllib.request
import gzip
import shutil
from pyspark.sql import SQLContext
sys.path.append(os.getcwd())

from pdb_helper import spark, read_config
from installer import reinstall
from fill_config_table import fill
from configure import configure
from download_unzip_all import download_unzip_all_fn
from bronze import bronze


def main():

    config = read_config()

    reinstall(spark_session = spark, config = config)
    fill(spark_session = spark, config = config)
    configure(spark_session = spark, config = config)

    download_unzip_all_fn(spark_session = spark, config = config)

    bronze(spark_session = spark, config = config)

if __name__ == "__main__":
    main()