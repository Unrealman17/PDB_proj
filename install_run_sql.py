'''
создать пользователя
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

запустить airflow на порту 8090
airflow webserver -p 8090

airflow scheduler

hostname -I

export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=~/miniconda3/envs/PDB_proj/bin/python

ps -aux

cd $SPARK_HOME && ./sbin/start-master.sh
cd $SPARK_HOME && ./sbin/start-worker.sh spark://172.17.181.159:7077

----------------------------------------------------------
from pyspark.sql.functions import col, udf

def convertCase(str):
    return hash(str) % thread_num

# Converting function to UDF
UDF = udf(lambda z: convertCase(z), IntegerType())
experiment_df = experiment_df.withColumn("thread", UDF(
    col("experiment"))).collect()
----------------------------------------------------------

# using timestamp
SELECT * FROM tableName TIMESTAMP as of "operation timestamp from history"
# using version
SELECT * FROM tableName VERSION as of "VERSION NUMBER"

export PYSPARK_PYTHON=~/miniconda3/envs/PDB_proj/bin/python
export PYSPARK_DRIVER_PYTHON=~/miniconda3/envs/PDB_proj/bin/python
hostname -I
cd $SPARK_HOME 
cd $SPARK_HOME && ./sbin/stop-worker.sh
cd $SPARK_HOME && ./sbin/stop-master.sh
cd $SPARK_HOME && ./sbin/start-master.sh
cd $SPARK_HOME && ./sbin/start-worker.sh spark://172.18.100.81:7077

Read 426 strings
--- sql 31.132269382476807 seconds ---
'''
import gzip
import json
from installer import reinstall
from fill_config_table import fill
from pdb_helper import spark, read_config
from download_unzip_all import download
from gemmi import cif
import time
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import IntegerType, StringType, ArrayType, MapType, StructField, StructType

import os
print(os.getcwd())


config = read_config()

reinstall(spark_session=spark, config=config)

fill(spark_session=spark, config=config)
start_time = time.time()


spark.udf.register("download_gz", lambda x: download(x, config=config))
spark.sql('create or replace temp view gz_df as select download_gz(experiment) as experiment from config_pdb_actualizer')

categories = ["chem_comp",
              "entity_poly_seq",
              "pdbx_database_PDB_obs_spr",
              "entity", "exptl"]


def extract_from_cif(gz_file_name: str):
    categories = ["chem_comp",
                  "entity_poly_seq",
                  "pdbx_database_PDB_obs_spr",
                  "entity", "exptl"]
    with gzip.open(gz_file_name, 'r') as f:
        bytes = f.read()
    block = cif.read_string(bytes.decode('ascii')).sole_block()
    res = []
    for category in categories:
        cat = f'_{category}.'
        table = block.find_mmcif_category(cat)

        rows = [list(row) for row in table]
        header = list(table.tags)

        if rows:
            for row in rows:
                row.append(block.name)
            header.append(cat+'cif_file_name')

        res.extend([(category, row) for row in rows])
        res.append((f'header', header))

    return res


spark.udf.register("extract_from_cif",
                   extract_from_cif,
                   returnType=ArrayType(
                       StructType(
                           [
                               StructField('k', StringType()),
                               StructField("data", ArrayType(
                                   StringType()
                               ))
                           ]
                       )
                   )
                   )
spark.sql('create or replace temp view mixed_df_one_col as select explode(extract_from_cif(experiment)) from gz_df')
spark.sql("create or replace temp view mixed_df as select col.k,col.data from mixed_df_one_col")


header_df = spark.sql("SELECT distinct data FROM mixed_df where k = 'header'")
spark.sql("create or replace temp view mixed_rdd_without_headers as SELECT k, data FROM mixed_df where k != 'header'")


headers = header_df.rdd.map(lambda x: x[0]).collect()
schemas = {category:set() for category in categories}
for table_header in headers:
    for header in table_header:
        category, h = header.split('.')
        schemas[category[1:]].add(h)
for k in schemas.keys():
    schemas[k] = list(schemas[k])

data_df = {}

for category in categories:
    columns = [f'data[{i}] as {x}' for i, x in enumerate(schemas[category])]
    columns = ', '.join(columns)
    table_name = f'bronze_{category}'
    if columns:
        query = f"CREATE EXTERNAL TABLE {table_name} USING delta LOCATION '{config['external_table_path']}{table_name}' AS \
            select {columns} \
                from mixed_rdd_without_headers where k = '{category}'"
        spark.sql(query)    



print("--- sql %s seconds ---" % (time.time() - start_time))
