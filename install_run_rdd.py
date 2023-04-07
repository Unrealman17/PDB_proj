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
'''
import gzip
import json
from installer import reinstall
from fill_config_table import fill
from pdb_helper import spark, read_config
from download_unzip_all import download
from gemmi import cif
import time

import os
print(os.getcwd())


config = read_config()

reinstall(spark_session = spark, config = config)

fill(spark_session = spark, config = config)
start_time = time.time()

experiment_rdd = spark.sql('select experiment from config_pdb_actualizer').rdd
experiment_rdd = experiment_rdd.map(lambda x: str(x[0]))
gz_rdd = experiment_rdd.map(lambda x: download(x, config=config))
categories = ["chem_comp",
              "entity_poly_seq",
              "pdbx_database_PDB_obs_spr",
              "entity", "exptl"]


def extract_from_cif(gz_file_name: str):
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
        res.append((f'header',header))

    return res

mixed_rdd = gz_rdd.flatMap(extract_from_cif)
# mixed_rdd = mixed_rdd.repartition(len(categories))

header_rdd = mixed_rdd.filter(lambda x: x[0] == 'header')
mixed_rdd_without_headers = mixed_rdd.filter(lambda x: x[0] != 'header')
mixed_rdd.cache()

headers = header_rdd.map(lambda x:x[1]).collect()
schemas = {category:[] for category in categories}
for table_header in headers:
    for header in table_header:
        category, h = header.split('.')
        schemas[category[1:]].append(h)

# print(json.dumps(schemas,indent=4))

def add_schema(rdd_kv):
    category, row = rdd_kv
    row = { schemas[category][i]:row[i] for i in range(len(row))}
    return (category,row)

mixed_rdd_with_schema = mixed_rdd_without_headers.map(add_schema).cache()

mixed_rdd.unpersist()
mixed_rdd_with_schema.cache()

data_df = {}

for category in categories:
    rdd = mixed_rdd_with_schema.filter(lambda x: x[0] == category).map(lambda x:x[1])
    if rdd.take(1):
        df = rdd.toDF()
        df.show(5)
        df.createOrReplaceTempView('tmp')
        table_name = f'bronze_{category}'
        if spark._jsparkSession.catalog().tableExists('default', table_name):
            pass
        else:
            spark.sql(f"CREATE EXTERNAL TABLE {table_name} USING delta LOCATION '{config['external_table_path']}{table_name}' AS select * from tmp")

mixed_rdd_with_schema.unpersist()


# mixed_rdd.map(lambda x: {'category'    : x[0],
#                         'data': x[1]
#                            }).toDF().show(100)

print("--- %s seconds ---" % (time.time() - start_time))


