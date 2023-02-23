# Databricks notebook source
# MAGIC %sh
# MAGIC pip install gemmi

# COMMAND ----------

import os
import sys
from pathlib import Path
os.chdir('..')
sys.path.append(os.getcwd())
print(os.getcwd())

# COMMAND ----------

from pdb_helper import spark, read_config
from installer import reinstall
from fill_config_table import fill
from configure import configure
from download_file import download_unzip
from main import main
from mount_storage_account import mount_storage_account

# COMMAND ----------

config = read_config()
download_thread = config['download_thread']


# COMMAND ----------

reinstall(spark_context = spark)


# COMMAND ----------

fill(spark_context = spark)


# COMMAND ----------

configure(spark_context = spark)



# COMMAND ----------

for i in range(download_thread):
    download_unzip(i)


# COMMAND ----------

main(spark_context = spark)
