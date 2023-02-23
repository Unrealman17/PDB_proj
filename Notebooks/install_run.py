# Databricks notebook source
from pathlib import Path
path = 
print(path)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC pip install gemmi

# COMMAND ----------

import os
import sys
os.chdir('..')
sys.path.append(os.getcwd())
print(os.getcwd())
from pdb_helper import spark, read_config
from installer import reinstall
from fill_config_table import fill
from configure import configure
from download_file import download_unzip
from main import main

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
