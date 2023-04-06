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


cd $SPARK_HOME 
./sbin/stop-worker.sh
./sbin/stop-master.sh
./sbin/start-master.sh
./sbin/start-worker.sh spark://172.18.100.81:7077
'''
import gzip
import json
from pdb_helper import spark, read_config
from download_unzip_all import download
from gemmi import cif

import os

print(os.getcwd())

config = read_config()

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
        res.extend([(category, row) for row in rows])
        header = list(table.tags)
        res.append((f'header',header))
    return res

mixed_rdd = gz_rdd.flatMap(extract_from_cif)
# mixed_rdd = mixed_rdd.repartition(len(categories))

header_rdd = mixed_rdd.filter(lambda x: x[0] == 'header')
mixed_rdd_without_headers = mixed_rdd.filter(lambda x: x[0] != 'header')

headers = header_rdd.map(lambda x:x[1]).collect()
schemas = {category:[] for category in categories}
for table_header in headers:
    for header in table_header:
        category, h = header.split('.')
        schemas[category[1:]].append(h)

def add_schema(rdd_kv):
    category, row = rdd_kv
    row = { schemas[category][i]:row[i] for i in range(len(row))}
    return (category,row)

mixed_rdd_with_schema = mixed_rdd_without_headers.map(add_schema)

data_rdds = {}
for category in categories:
    data_rdds[category] = mixed_rdd_with_schema.filter(lambda x: x[0] == category).map(lambda x:x[1])
    if data_rdds[category].take(1):
        data_rdds[category].toDF().show(5)

# mixed_rdd.map(lambda x: {'category'    : x[0],
#                         'data': x[1]
#                            }).toDF().show(100)

print(1)

# ===== words analyze ===================================
# letters = set("qwertyuiopasdfghjklzxcvbnm")
# rdd = spark.sparkContext.textFile('123.txt')
# lower = rdd.map(lambda x: x.lower())
# symbols = lower.map(lambda x: set(x) - letters).reduce(lambda x,y: x | y)
# print(len(symbols))

# def super_split(x:str):
#     for s in symbols:
#         x = x.replace(s,' ')
#     return x.split()

# add = lambda x, y : x+y
# value_desc = lambda x: -x[1]

# words = lower.flatMap(super_split)
# kv = words.map(lambda x: (x,1))
# counted = kv.reduceByKey(add)
# res = counted.sortBy(value_desc)
# print(res.count())
# res.toDF().limit(10).show()

# #spark.sparkContext.parallelize(words.countByValue()).sortBy(value_desc).toDF().limit(10).show()

# letters = counted.flatMap(lambda x: [(l, x[1]) for l in x[0]])
# counted_letters = letters.reduceByKey(add).sortBy(value_desc)
# total = counted_letters.map(lambda x:x[1]).reduce(add)

# fr_letters = counted_letters.map(lambda x: (x[0],f'{round(x[1]/total*100,2)}%'))

# print(fr_letters.count())
# fr_letters.toDF().show(n=30)
# print()

# ===== words analyze ===================================

# ===== transactions analyze ===================================
# '''
#     1 Get the total amount spent by each user
#     2 Get the total amount spent by each user for each of their cards
#     3 Get the total amount spent by each user for each of their cards on each category

#     4 Get the distinct list of categories in which the user has made expenditure
#     5 Get the category in which the user has made the maximum expenditure
# '''

# rdd = spark.sparkContext.textFile('card_transactions.json').map(lambda x: json.loads(x))


# def get_category_cards_user_amount(d: dict):
#     return ((d['user_id'], d['category'], d['card_num']), d['amount'])


# res3 = rdd.map(get_category_cards_user_amount)\
#         .combineByKey(lambda x: x, lambda x, y: x+y, lambda x, y: x+y)\
#         .map(lambda x: {'user'    : x[0][0],
#                         'category': x[0][1],
#                         'card'    : x[0][2],
#                         'amount'  : x[1]
#                            })
# #
# res3.cache()

# res2 = res3.map(lambda x: ((x['user'], x['card']),x['amount']))\
#         .combineByKey(lambda x: x, lambda x, y: x+y, lambda x, y: x+y)\
#         .map(lambda x: {'user'    : x[0][0],
#                         'card'    : x[0][1],
#                         'amount'  : x[1]
#                         })

# res1 = res2.map(lambda x: (x['user'],x['amount']))\
#         .combineByKey(lambda x: x, lambda x, y: x+y, lambda x, y: x+y)\
#         .map(lambda x: {'user'    : x[0],
#                         'amount'  : x[1]
#                         })


# res1.toDF().show(100)
# res2.toDF().show(100)
# res3.toDF().show(100)
# res3.unpersist()

# res3.unpersist()
# res45 = rdd.map(lambda d: ((d['user_id'], d['category']), d['amount']))\
#             .combineByKey(lambda x: x, lambda x, y: x+y, lambda x, y: x+y)\
#             .map(lambda x: {'user'    : x[0][0],
#                             'category': x[0][1],
#                             'amount'  : x[1]
#                             })\
#             .cache()

# res4 = res45.map(lambda d:(d['user'], d['category']))\
#         .combineByKey(lambda x: {x},lambda x,y:x | {y},lambda x,y: x | y)\
#         .map(lambda x: {'user':x[0], 'categories':', '.join(x[1])})

# res5 = res45.map(lambda d: (d['user'],(d['category'],d['amount'])))\
#         .combineByKey(lambda x: x,lambda x,y: x if x[1]>y[1] else y, lambda x,y: x if x[1]>y[1] else y)\
#         .map(lambda x: {'user':x[0],
#                         'category':x[1][0],
#                         'amount':x[1][1]})

# res4.toDF().show()
# res5.toDF().show()

# res45.unpersist()

# print(1)

# ===== transactions analyze ===================================


# spark.sql(f"DESCRIBE HISTORY register_pdb_actualizer").show()

# spark.sql(f"SELECT * FROM register_pdb_actualizer").show()
#spark.sql(f"Update register_pdb_actualizer set checksum = concat('a',checksum);").show()
#spark.sql(f"SELECT * FROM register_pdb_actualizer").show()

# spark.sql(f"RESTORE TABLE register_pdb_actualizer TO VERSION AS OF 2").show()
# spark.sql(f"SELECT * FROM register_pdb_actualizer version as of 2").show()


# spark.sql(f"create table blah(name string) USING csv;")
# spark.sql(f"insert into blah(name) select '32145';")
# spark.sql(f"truncate table blah;")

# spark.sql(f"drop table blah;")
# spark.sql(f"create table blah(name string) USING delta;")
# spark.sql(f"insert into blah(name) select '32145';")
# spark.sql(f"truncate table blah;")
# spark.sql(f"drop table blah;")
