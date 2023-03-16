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
from download_unzip_all import download_unzip_al
from bronze import bronze
from mount_storage_account import mount_storage_account

# COMMAND ----------

mount_storage_account(dbutils = dbutils)

# COMMAND ----------

config = read_config()
download_thread = config['download_thread']
print('download_thread:',download_thread)
print('config:',json.dumps(config,indent=4))

# COMMAND ----------

reinstall(spark_session = spark, dbutils=dbutils)

# COMMAND ----------

fill(spark_session = spark)

# COMMAND ----------

configure(spark_session = spark)

# COMMAND ----------

for i in range(download_thread):
    r = download_unzip_al(i)

# COMMAND ----------

bronze(spark_session = spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_entity;
