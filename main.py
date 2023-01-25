# Databricks notebook source
'''
TODO:
    1 |+| расчет контрольных сумм и их проверка
    2 |-| обработка всех типов из документации
    3 |0| прикрутить storage аккаунт
    4 |+| сделать чтение списка экспериментов из файла
    5 |0| создать отдельный джоб и запустить код на нем
    6 |0| использовать заголовок ETag через HEAD запрос
'''
'''
    1 загрузка, разархивирование файлов
    2 считаем контро
    change data capture протестить SELECT * FROM table_change('table_name',3,4)
        describe history table_name
'''
import time
from datetime import datetime
from install import Tables_config
start_time = time.time()
start_ts = datetime.now()
from pyspark.sql import functions as pyspark_f
import os
import urllib.request
import gzip
import shutil
import json
from gemmi import cif
from pyspark.sql.types import StructType, StructField, StringType
import hashlib
from pathlib import Path
from pathlib import PurePath
from pyspark.sql import SparkSession

from pdb_helper import read_config, task

def calculate_checksum(file_path):
    with open(file_path) as f:
        content = f.read()
    return hashlib.md5(content.encode()).hexdigest()
    


@task
def main(spark_context: SparkSession):
    config = read_config()
    downloads_path = config["downloads_path"]
    '''
        read file "input.txt" with experiment list to process
    '''
    tables_config = Tables_config(spark_context = spark_context).bronze_tables()

    experiment_df = spark_context.read.table('config_pdb_actualizer')\
                        .select('experiment').collect()

    # fields = {f'_{name}':[] for name in config["tables"]}
    extra_data = config["extra"]

    df_dict = {}
    schema_dict = {}

    register_df = spark_context.sql('''select history_begin, 
                                              experiment, 
                                              checksum 
                                            from register_pdb_actualizer;''')
    #current_experiments = spark_context.createDataFrame([],schema=register_df.schema)
    checksum_dict = {i.experiment: i.checksum for i in register_df.collect()}

    print(checksum_dict)

    for m in experiment_df:
        exprmnt = m.experiment
        
        file_name =  f"{downloads_path}{exprmnt}.cif"
        
        checksum_actual = calculate_checksum(file_name)
        print(exprmnt, end = ' ')
        if exprmnt in checksum_dict and checksum_dict[exprmnt] == checksum_actual:
            print('skipped')
            continue
        else:
            print('parsing . . .')
            
        doc = cif.read_file(file_name)  # copy all the data from mmCIF file
        block = doc.sole_block()  # mmCIF has exactly one block
        
        tables_config.init_bronze_schema(block=block)
        
        for table, f in tables_config.bronze_tables():      
            if len(table.schema)==0:
                continue
            df_data = []
            
            for row in block.find_mmcif_category(table):
                df_row = [exprmnt]
                for cell in row:
                    if cif.is_null(cell):
                        df_row.append(None)
                    else:
                        df_row.append(cell)
                df_data.append(df_row)
            
            if table not in schema_dict:
                schema = []
                for fld in columns:
                    schema.append(StructField(fld, StringType(), True))
                schema_dict[table] = StructType(schema)
                df_dict[table] = []

            df_dict[table].extend(df_data)
                
        table = '_extra'
        if table not in schema_dict:
            schema = [StructField('experiment', StringType(), False)]
            for fld in extra_data:
                schema.append(StructField(fld, StringType(), True))
            schema_dict[table] = StructType(schema)
            df_dict[table] = []

        df_data = [exprmnt]
        for field in extra_data:
            df_data.append(block.find_pair_item(field).pair[1])
        df_dict[table].append(df_data)
        # df = spark.createDataFrame([df_row], schema=StructType(schema))
        checksum_dict[exprmnt] = checksum_actual


    for k,v in schema_dict.items():
        #print(k, df_dict[k][:1])
        df_dict[k] = spark_context.createDataFrame(df_dict[k], schema=v)



    df_data = [[start_ts,k,v] for k,v in checksum_dict.items()]
    spark_context.createDataFrame(df_data,schema=register_df.schema).createOrReplaceTempView('tmp_register_pdb_actualizer')
    print(checksum_dict)

    # COMMAND ----------

    spark_context.sql('''
                MERGE INTO register_pdb_actualizer m
                USING tmp_register_pdb_actualizer t
                ON m.experiment = t.experiment
                WHEN MATCHED THEN
                UPDATE SET
                    history_begin = t.history_begin,
                    checksum = t.checksum,
                    experiment = t.experiment
                WHEN NOT MATCHED
                THEN INSERT *;''')
    
    spark_context.sql('DROP VIEW tmp_register_pdb_actualizer;')

    # переделать на регистрацию temp view
    # data_path = f"{folder_path}bronze/"
    # Path(data_path).mkdir(parents=True, exist_ok=True)
    # path_list = {}
    for k,v in df_dict.items():
        # path = data_path+k
        # print(path)
        table_name = f'defaul.tmp_bronze{k}'
        v.createOrReplaceTempView(table_name)
        # почему-то не работает: Table or view 'tmp_bronze_chem_comp' not found in database 'default'
        # spark.sql(f'SELECT * FROM {table_name};').show()
        # spark.sql(f'OPTIMIZE {table_name};')
        # print('123')


        # dbutils.fs.rm(path, recurse=True)
        # v.repartition(1).write.format("delta").mode("overwrite").save(path)
        # path_list[k] = path

    # COMMAND ----------
    # TODO использовать одну структуру хранения информации о таблицах
    for table_name in df_dict.keys():
        #df = spark.read.format("delta").load(p)
        print(table_name)
        spark_context.sql(f'DROP TABLE IF EXISTS bronze{table_name};')
        spark_context.sql(f'''
                    CREATE TABLE IF NOT EXISTS bronze{table_name} 
                    as
                    SELECT * FROM tmp_bronze{table_name};''')
        spark_context.sql(f'DROP VIEW IF EXISTS tmp_bronze{table_name};')

    print("--- %s seconds ---" % (time.time() - start_time))


    # COMMAND ----------

    # MAGIC %run ./silver

    # COMMAND ----------

    # MAGIC %sql
    # MAGIC update history_pdb_actualizer
    # MAGIC   set end = now()
    # MAGIC   where end is null
    # MAGIC     and id = (select max(id) from history_pdb_actualizer);
    # MAGIC SELECT * FROM history_pdb_actualizer ORDER BY ID DESC limit 3;
    # MAGIC --2022-10-17T14:44:54.274+0000 2022-10-17T15:48:32.979+0000

    # COMMAND ----------

    # print("--- %s seconds ---" % (time.time() - start_time))

    # COMMAND ----------

    # MAGIC %sql
    # MAGIC SELECT count(distinct experiment) FROM silver_entity

    # COMMAND ----------


if __name__ == "__main__":
    main()
