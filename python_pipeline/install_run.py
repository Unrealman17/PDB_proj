import os
import sys
sys.path.append(os.getcwd())

from pdb_helper import spark, read_config
from install import install
from fill_config_table import fill
from configure import configure
from download_file import download_unzip

config = read_config()
download_thread = config['download_thread']

install(spark_context = spark)
fill(spark_context = spark)
configure(spark_context = spark)

for i in range(download_thread):
    download_unzip(i)

os.system('python main.py')
