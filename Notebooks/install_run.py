# Databricks notebook source
# MAGIC %sh
# MAGIC pip install gemmi

# COMMAND ----------

import os
import sys
import json
from pathlib import Path
os.chdir('..')
sys.path.append(os.getcwd())
# print(os.getcwd())
##################
from pdb_helper import spark, read_config
from installer import reinstall
from fill_config_table import fill
from configure import configure
from download_file import download_unzip
from main import main
from mount_storage_account import mount_storage_account

# COMMAND ----------

mount_storage_account(dbutils = dbutils)

# COMMAND ----------

config = read_config()
download_thread = config['download_thread']
print('download_thread:',download_thread)
print('config:',json.dumps(config,indent=4))

# COMMAND ----------

reinstall(spark_context = spark, dbutils=dbutils)

# COMMAND ----------

os.path.exists('/dbfs/mnt/testblobstorage/data/config_pdb_actualizer')

# COMMAND ----------

dbutils.fs.head('dbfs:/mnt/testblobstorage/data/bronze_entity_poly_seq')

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/testblobstorage/data/bronze_entity_poly_seq')


# COMMAND ----------

fill(spark_context = spark)

# COMMAND ----------

configure(spark_context = spark)

# COMMAND ----------

for i in range(download_thread):
    r = download_unzip(i)

# COMMAND ----------

main(spark_context = spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity;

# COMMAND ----------


