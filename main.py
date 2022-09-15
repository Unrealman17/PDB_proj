# Databricks notebook source
# MAGIC %sql
# MAGIC drop TABLE IF EXISTS config_pdb_actualizer;
# MAGIC CREATE TABLE config_pdb_actualizer as select '4HHB' as data union select '4HHA' as data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM config_pdb_actualizer;

# COMMAND ----------

with open('/tmp/test.txt','w') as f:
    f.write('123')

# COMMAND ----------

onlyfiles =  os.listdir('/tmp')
print(onlyfiles)

# COMMAND ----------

import os
import urllib.request
import gzip
import shutil

#print(os.getcwd())

molecule_df = spark.read.table('config_pdb_actualizer')
for m in molecule_df.select('data').collect():
    molecule = m.data.lower()
    print(molecule)
    file_name = f"/tmp/{molecule}.cif.gz"
    url = f'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/{molecule}.cif.gz'
    urllib.request.urlretrieve(url, file_name)
    with gzip.open(file_name, 'rb') as f_in:
        with open(file_name[:-len('.gz')], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    

    
    

# COMMAND ----------


