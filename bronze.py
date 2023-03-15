'''
TODO:
    1 |+| расчет контрольных сумм и их проверка
    2 |-| обработка всех типов из документации
    3 |+| прикрутить storage аккаунт
    4 |+| сделать чтение списка экспериментов из файла
    5 |0| создать отдельный джоб и запустить код на нем
    6 |0| использовать заголовок ETag через HEAD запрос
    7 |0| добавить размер памяти на датафрейм для сброса данных в таблицу аналогично buffer_size_per_table
'''
'''
    1 загрузка, разархивирование файлов
    2 считаем контро
    change data capture протестить SELECT * FROM table_change('table_name',3,4)
        describe history table_name
'''
from pdb_helper import task
from pyspark.sql import SparkSession
import hashlib
from gemmi import cif
import time
from datetime import datetime
from installer import Tables_config, CONFIG
start_time = time.time()
start_ts = datetime.now()


def calculate_checksum(file_path):
    with open(file_path) as f:
        content = f.read()
    return hashlib.md5(content.encode()).hexdigest()


@task
def bronze(spark_context: SparkSession, config: dict):
    downloads_path = config["downloads_path"]
    '''
        read file "input.txt" with experiment list to process
    '''
    tables_config = Tables_config(spark_context=spark_context, config=config)

    experiment_df = spark_context.read.table('config_pdb_actualizer')\
        .select('experiment').collect()

    # fields = {f'_{name}':[] for name in config["tables"]}
    extra_data = config["extra"]

    register_df = spark_context.sql('''select history_begin, 
                                              experiment, 
                                              checksum 
                                            from register_pdb_actualizer;''')
    #current_experiments = spark_context.createDataFrame([],schema=register_df.schema)
    checksum_dict = {i.experiment: i.checksum for i in register_df.collect()}

    print(checksum_dict)
    file_counter = 0
    for m in experiment_df:
        exprmnt = m.experiment
        file_counter += 1
        file_name = f"{downloads_path}{exprmnt}.cif"

        checksum_actual = calculate_checksum(file_name)
        print(exprmnt, end=' ')
        if exprmnt in checksum_dict and checksum_dict[exprmnt] == checksum_actual:
            print('skipped')
            continue
        else:
            print('parsing . . .')

        doc = cif.read_file(file_name)  # copy all the data from mmCIF file
        block = doc.sole_block()  # mmCIF has exactly one block

        tables_config.init_bronze_schema(block=block)

        for table in tables_config.bronze_category_tables():
            if not table.schema or len(table.schema) == 0:
                continue

            for row in block.find_mmcif_category(table.category):
                df_row = []
                for cell in row:
                    if cif.is_null(cell):
                        df_row.append(None)
                    else:
                        df_row.append(cell)
                df_row.append(exprmnt)

                table.append(df_row)

        table = tables_config.bronze_extra

        df_row = []
        for field in extra_data:
            df_row.append(block.find_pair_item(field).pair[1])
        df_row.append(exprmnt)
        table.append(df_row)
        # df = spark.createDataFrame([df_row], schema=StructType(schema))
        checksum_dict[exprmnt] = checksum_actual

    for table in tables_config.bronze_tables():
        table.write()

    df_data = [[start_ts, k, v] for k, v in checksum_dict.items()]
    spark_context.createDataFrame(df_data, schema=register_df.schema).createOrReplaceTempView(
        'tmp_register_pdb_actualizer')
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
    # for k,v in df_dict.items():
    #     # path = data_path+k
    #     # print(path)
    #     table_name = f'defaul.tmp_bronze{k}'
    #     v.createOrReplaceTempView(table_name)
    #     # почему-то не работает: Table or view 'tmp_bronze_chem_comp' not found in database 'default'
    #     # spark.sql(f'SELECT * FROM {table_name};').show()
    #     # spark.sql(f'OPTIMIZE {table_name};')
    # print('123')

    # dbutils.fs.rm(path, recurse=True)
    # v.repartition(1).write.format("delta").mode("overwrite").save(path)
    # path_list[k] = path

    # TODO использовать одну структуру хранения информации о таблицах
    # for table_name in df_dict.keys():
    #     #df = spark.read.format("delta").load(p)
    #     print(table_name)
    #     spark_context.sql(f'DROP TABLE IF EXISTS bronze{table_name};')
    #     spark_context.sql(f'''
    #                 CREATE TABLE IF NOT EXISTS bronze{table_name}
    #                 as
    #                 SELECT * FROM tmp_bronze{table_name};''')
    #     spark_context.sql(f'DROP VIEW IF EXISTS tmp_bronze{table_name};')

    print("--- %s seconds ---" % (time.time() - start_time))


    # MAGIC %run ./silver


    # MAGIC %sql
    # MAGIC update history_pdb_actualizer
    # MAGIC   set end = now()
    # MAGIC   where end is null
    # MAGIC     and id = (select max(id) from history_pdb_actualizer);
    # MAGIC SELECT * FROM history_pdb_actualizer ORDER BY ID DESC limit 3;
    # MAGIC --2022-10-17T14:44:54.274+0000 2022-10-17T15:48:32.979+0000


    # print("--- %s seconds ---" % (time.time() - start_time))


    # MAGIC %sql
    # MAGIC SELECT count(distinct experiment) FROM silver_entity


if __name__ == "__main__":
    bronze()
