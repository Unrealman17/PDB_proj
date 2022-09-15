# Databricks notebook source
!pip install biopython

# COMMAND ----------

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



# COMMAND ----------

from Bio.PDB.MMCIFParser import MMCIFParser
parser = MMCIFParser()
structure = parser.get_structure('4hhb', '/tmp/4hhb.cif')



# COMMAND ----------

_exptl_entry_id =structure.id

# COMMAND ----------

help(structure)
structure

# COMMAND ----------

import Bio
data = Bio.PDB.MMCIF2Dict.MMCIF2Dict('/tmp/4hhb.cif')
data = dict(data)
fields = {'_entity.':[],
'_pdbx_database_PDB_obs_spr.':[],
'_entity_poly_seq.':[],
'_chem_comp.':[]}

for k,v in data.items():
    for t in fields.keys():
        if k.startswith(t):
            fields[t].append(k)
df_list = []
for table, f in fields.items():
    columns = [x[len(table):] for x in f]
    df_data = []
    for i in range(len(data[f[0]])):
        row = []
        for j in f:
            row.append(data[j][i])
        df_data.append(row)
    df = spark.createDataFrame(df_data, columns)
    #display(df)
    df_list.append(df)

    
print(columns)
print(json.dumps(fields, indent=4))            


# COMMAND ----------

import json 
data = dict(data)
print(json.dumps(data, indent =4))

# COMMAND ----------

os.chdir('PDB_proj')
print(os.listdir(os.getcwd()))

# COMMAND ----------

print(os.getcwd())

# COMMAND ----------



# COMMAND ----------


