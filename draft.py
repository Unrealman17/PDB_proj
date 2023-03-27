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
'''
import os
print(os.getcwd())
from pdb_helper import spark

letters = set("qwertyuiopasdfghjklzxcvbnm")
rdd = spark.sparkContext.textFile('123.txt')
lower = rdd.map(lambda x: x.lower())
symbols = lower.map(lambda x: set(x) - letters).reduce(lambda x,y: x | y)
print(len(symbols))

def super_split(x:str):
    for s in symbols:
        x = x.replace(s,' ')
    return x.split()

add = lambda x, y : x+y
value_desc = lambda x: -x[1]

words = lower.flatMap(super_split)
kv = words.map(lambda x: (x,1))
counted = kv.reduceByKey(add)
res = counted.sortBy(value_desc)
print(res.count())
res.toDF().limit(10).show()

#spark.sparkContext.parallelize(words.countByValue()).sortBy(value_desc).toDF().limit(10).show()

letters = counted.flatMap(lambda x: [(l, x[1]) for l in x[0]])
counted_letters = letters.reduceByKey(add).sortBy(value_desc)
total = counted_letters.map(lambda x:x[1]).reduce(add)

fr_letters = counted_letters.map(lambda x: (x[0],f'{round(x[1]/total*100,2)}%'))

print(fr_letters.count())
fr_letters.toDF().show(n=30)
print()
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


