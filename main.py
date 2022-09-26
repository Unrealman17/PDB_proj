# Databricks notebook source
!pip install gemmi

# COMMAND ----------

!pip install biopython

# COMMAND ----------

# MAGIC %sql
# MAGIC drop TABLE IF EXISTS config_pdb_actualizer;
# MAGIC CREATE TABLE config_pdb_actualizer as 
# MAGIC   select '4HHB' as data union select '4HHA' as data;
# MAGIC SELECT * FROM config_pdb_actualizer;

# COMMAND ----------




folder_path = "/dbfs/user/evgeniy.varganov@quantori.com/"
path = folder_path+'4hhb.cif'


print(block.find_pair_item('_entry.id').pair[1])
print(block.find_pair_item('_exptl.method').pair)

for col_name in block.get_mmcif_category('_chem_comp'):
    print(col_name)

for row in block.find_mmcif_category('_chem_comp'):
    for cell in row:
        if cif.is_null(cell):
            print('None', end = '\t')
        else:
            print(cell, end = '\t')
    print()




# COMMAND ----------

import os
import urllib.request
import gzip
import shutil
#from Bio.PDB.MMCIFParser import MMCIFParser
#from Bio.PDB import MMCIF2Dict;
import json
from gemmi import cif
from pyspark.sql.types import StructType, StructField, StringType


#parser = MMCIFParser()
#print(os.getcwd())
folder_path = "/dbfs/user/evgeniy.varganov@quantori.com/"
molecule_df = spark.read.table('config_pdb_actualizer').select('data').collect()

fields = {  '_entity':[],
            '_pdbx_database_PDB_obs_spr':[],
            '_entity_poly_seq':[],
            '_chem_comp':[]}

df_dict = {}

extra_data = ['_exptl.entry_id','_exptl.method']

for m in molecule_df:
    molecule = m.data.lower()
    
    gz_file_name = f"{folder_path}{molecule}.cif.gz"
    file_name = gz_file_name[:-len('.gz')]
    url = f'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/{molecule}.cif.gz'
    urllib.request.urlretrieve(url, gz_file_name)
    with gzip.open(gz_file_name, 'rb') as f_in:
        with open(file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(gz_file_name)

    doc = cif.read_file(file_name)  # copy all the data from mmCIF file
    block = doc.sole_block()  # mmCIF has exactly one block
    
    for table in fields.keys():
        for col_name in block.get_mmcif_category(table):
            if col_name not in fields[table]:
                fields[table].append(col_name)
    
    for table, f in fields.items():      
        columns = ['experiment']
        columns.extend(f)
        df_data = []
        
        for row in block.find_mmcif_category(table):
            df_row = [molecule]
            for cell in row:
                if cif.is_null(cell):
                    df_row.append(None)
                else:
                    df_row.append(cell)
            df_data.append(df_row)
        
        schema = []
        for fld in columns:
            schema.append(StructField(fld, StringType(), True))
        df = spark.createDataFrame(df_data, schema=StructType(schema))
        if table not in df_dict:
            df_dict[table] = df
        else:
            df_dict[table] = df_dict[table].union(df)
        print(molecule,table)
    
    table = '_extra'
    schema = [StructField('experiment', StringType(), False)]
    df_row = [molecule]
    
    for field in extra_data:
        schema.append(StructField(field, StringType(), True))
        df_row.append(block.find_pair_item(field).pair[1])
    df = spark.createDataFrame([df_row], schema=StructType(schema))
    if table not in df_dict:
        df_dict[table] = df
    else:
        df_dict[table] = df_dict[table].union(df)

for k,v in df_dict.items():
    display(v)
    

# COMMAND ----------

from pathlib import Path
data_path = f"{folder_path}data/"
Path(path).mkdir(parents=True, exist_ok=True)
path_list = {}
for k,v in df_dict.items():
    path = data_path+k
    print(path)
    v.write.format("delta").mode("overwrite").save(path)
    path_list[k] = path

# COMMAND ----------

for table_name, path in path_list.items():
    #df = spark.read.format("delta").load(p)
    #display(df)
    spark.sql(f'CREATE TABLE IF NOT EXISTS bronze{table_name} USING delta OPTIONS (path "{p}");')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_extra

# COMMAND ----------


