import os
import sys
sys.path.append(os.getcwd())

from pdb_helper import spark, read_config
from installer import reinstall
from fill_config_table import fill
from configure import configure
from download_file import download_unzip
from main import main

config = read_config()
download_thread = config['download_thread']

reinstall(spark_context = spark)
fill(spark_context = spark)
configure(spark_context = spark)

for i in range(download_thread):
    download_unzip(i)

main(spark_context = spark)

