# Databricks notebook source
'''
TODO:
    1 |+| расчет контрольных сумм и их проверка
    2 |-| обработка всех типов из документации
    3 |-| прикрутить storage аккаунт
    4 |+| сделать чтение списка экспериментов из файла
    5 |-| создать отдельный джоб и запустить код на нем
'''
'''
    1 загрузка, разархивирование файлов
    2 считаем контро
'''

# COMMAND ----------


import os
import urllib.request
import gzip
import shutil
import json
from gemmi import cif
from pyspark.sql.types import StructType, StructField, StringType
import hashlib
from pathlib import Path


# COMMAND ----------

'''
    read file "input.txt" with experiment list to process
'''
input_path = '/dbfs/FileStore/input.txt'

with open(input_path)as f:
    txt = f.read()
experiments = txt.split('\n')
cmd = """
insert into config_pdb_actualizer(experiment) values\n('""" \
+ "'),\n('".join(txt.strip().split('\n')) \
+ "');"
spark.sql("truncate table config_pdb_actualizer;")
inserted = spark.sql(cmd).collect()[0].num_inserted_rows
print(f'Read {inserted} strings')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM config_pdb_actualizer;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into history_pdb_actualizer(begin,end)
# MAGIC   select now(), null;
# MAGIC SELECT * FROM history_pdb_actualizer;
# MAGIC -- insert into log_experiment_pdb_actualizer(id_log,experiment,checksum)
# MAGIC --   select (
# MAGIC --     select max(id) id_log
# MAGIC --       from history_pdb_actualizer
# MAGIC --       ) as id_log,
# MAGIC --     experiment,
# MAGIC --     null
# MAGIC --     from config_pdb_actualizer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete FROM log_experiment_pdb_actualizer where checksum is null;
# MAGIC SELECT * FROM log_experiment_pdb_actualizer;
# MAGIC --SELECT max(id) FROM history_pdb_actualizer;

# COMMAND ----------

'''
import os
folder_path = "/dbfs/user/evgeniy.varganov@quantori.com/"
for f in os.listdir(folder_path):
    p = folder_path+f
    print(f)
    #if os.path.isfile(p):
        # os.remove(p)
        
'''

# COMMAND ----------

folder_path = "/dbfs/user/evgeniy.varganov@quantori.com/"
molecule_df = spark.read.table('config_pdb_actualizer').select('experiment').collect()

fields = {  '_entity':[],
            '_pdbx_database_PDB_obs_spr':[],
            '_entity_poly_seq':[],
            '_chem_comp':[]}

df_dict = {}

extra_data = ['_exptl.entry_id','_exptl.method']

log_experiment_df = spark.sql('select id_history, experiment, checksum from log_experiment_pdb_actualizer;')
current_experiments = spark.createDataFrame([],schema=log_experiment_df.schema)
checksum_dict = {i.experiment: i.checksum for i in log_experiment_df.collect()}

print(checksum_dict)

for m in molecule_df:
    exprmnt = m.experiment
    tmp_path = f"{folder_path}downloads/"
    Path(tmp_path).mkdir(parents=True, exist_ok=True)
    
    gz_file_name = f"{tmp_path}{exprmnt}.cif.gz"
    file_name = gz_file_name[:-len('.gz')]
    url = f'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/{exprmnt.lower()}.cif.gz'
    urllib.request.urlretrieve(url, gz_file_name)
    with gzip.open(gz_file_name, 'rb') as f_in:
        with open(file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(gz_file_name)
    
    with open(file_name) as f:
        checksum_actual = f.read()
    checksum_actual = hashlib.md5(checksum_actual.encode()).hexdigest()
    print(exprmnt, end = ' ')
    if exprmnt in checksum_dict and checksum_dict[exprmnt] == checksum_actual:
        print('skipped')
        continue
    else:
        print('parsing . . .')
        
    doc = cif.read_file(file_name)  # copy all the data from mmCIF file
    block = doc.sole_block()  # mmCIF has exactly one block
    
    for table in fields.keys():
        for col_name in block.get_mmcif_category(table):
            if col_name not in fields[table]:
                fields[table].append(col_name)
    
    for table, f in fields.items():      
        columns = ['id_log_experiment']
        if len(f)==0:
            continue
        columns.extend(f)
        df_data = []
        
        for row in block.find_mmcif_category(table):
            df_row = [exprmnt]
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
        if table in df_dict:
            df_dict[table] = df_dict[table].union(df)
        else:
            df_dict[table] = df
            
        
        
    table = '_extra'
    schema = [StructField('experiment', StringType(), False)]
    df_row = [exprmnt]
    
    for field in extra_data:
        schema.append(StructField(field, StringType(), True))
        df_row.append(block.find_pair_item(field).pair[1])
    df = spark.createDataFrame([df_row], schema=StructType(schema))
    if table not in df_dict:
        df_dict[table] = df
    else:
        df_dict[table] = df_dict[table].union(df)
    
    checksum_dict[exprmnt] = checksum_actual

id_history = spark.sql('SELECT max(id) as id FROM history_pdb_actualizer;').collect()[0].id
df_data = [[id_history,k,v] for k,v in checksum_dict.items()]
spark.createDataFrame(df_data,schema=log_experiment_df.schema).createOrReplaceTempView('tmp_log_experiment_pdb_actualizer')
print(checksum_dict)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO log_experiment_pdb_actualizer m
# MAGIC USING tmp_log_experiment_pdb_actualizer t
# MAGIC ON m.experiment = t.experiment
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     id_history = t.id_history,
# MAGIC     checksum = t.checksum,
# MAGIC     experiment = t.experiment
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *;
# MAGIC   
# MAGIC drop view tmp_log_experiment_pdb_actualizer;
# MAGIC 
# MAGIC SELECT * FROM log_experiment_pdb_actualizer;

# COMMAND ----------

for k,v in df_dict.items():
    print(k)
    #print(v.columns)
    display(v)

# COMMAND ----------

data_path = f"{folder_path}bronze/"
Path(data_path).mkdir(parents=True, exist_ok=True)
path_list = {}
for k,v in df_dict.items():
    path = data_path+k
    print(path)
    # dbutils.fs.rm(path, recurse=True)
    v.write.format("delta").mode("overwrite").save(path)
    path_list[k] = path

# COMMAND ----------

for table_name, path in path_list.items():
    #df = spark.read.format("delta").load(p)
    print(table_name)
    spark.sql(f'DROP TABLE IF EXISTS bronze{table_name};')
    spark.sql(f'CREATE TABLE IF NOT EXISTS bronze{table_name} USING delta OPTIONS (path "{path}");')
    #spark.sql(f"insert into bronze{table_name} select * from tmp{table_name};")
    #spark.sql(f'DROP TABLE IF EXISTS tmp{table_name};')
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_entity

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_extra as 
# MAGIC   select * from bronze_extra;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver_extra as 
# MAGIC   select * from bronze_extra;

# COMMAND ----------

# MAGIC %sql
# MAGIC update history_pdb_actualizer 
# MAGIC   set end = now()
# MAGIC   where end is null
# MAGIC     and id = (select max(id) from history_pdb_actualizer);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM history_pdb_actualizer;

# COMMAND ----------

url = f'https://mmcif.wwpdb.org/dictionaries/ascii/mmcif_pdbx.dic'
f_name = f"{tmp_path}mmcif_pdbx.dic"
urllib.request.urlretrieve(url, f_name)
doc = cif.read_file(f_name)  # copy all the data from mmCIF file
block = doc.sole_block()  # mmCIF has exactly one block
for col_name in block.get_mmcif_category('_entity'):
    print(col_name)

# COMMAND ----------


