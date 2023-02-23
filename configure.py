import shutil
from pathlib import Path
from pyspark.storagelevel import StorageLevel
from pdb_helper import read_config, task
from fill_config_table import fill
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

@task
def configure(spark_context: SparkSession):
    config = read_config()
    thread_num = config["download_thread"]
    downloads_path = config["downloads_path"]
    # start pipeline
    spark_context.sql("""insert into history_pdb_actualizer(begin,end)
                    select now(), null;""")
    try:
        shutil.rmtree(downloads_path)
    except FileNotFoundError:
        print(f'{downloads_path} does not exists.')
    Path(downloads_path).mkdir(parents=True)

    # experiment_df = spark_context.read.table(
    #     'config_pdb_actualizer').select('experiment')

    experiment_df = spark_context.sql("select experiment from config_pdb_actualizer;")

    # experiment_df.show()

    def convertCase(str):
        return hash(str) % thread_num

    # Converting function to UDF
    UDF = udf(lambda z: convertCase(z), IntegerType())
    experiment_df = experiment_df.withColumn("thread", UDF(
        col("experiment"))).collect()
    experiment_df = spark_context.createDataFrame(experiment_df)
    
    #spark_context.sql("truncate table config_pdb_actualizer;")
    
    experiment_df.createTempView("new_config_pdb_actualizer")
    # spark_context.sql("select experiment,thread from new_config_pdb_actualizer;").show()
    # spark_context.sql("select experiment,thread from new_config_pdb_actualizer;").show()
    spark_context.sql("insert overwrite config_pdb_actualizer(experiment,thread) \
                select experiment,thread \
                    from new_config_pdb_actualizer;").collect()
    spark_context.sql("DROP VIEW new_config_pdb_actualizer;")
    spark_context.sql("select experiment,thread from config_pdb_actualizer;").show()


if __name__ == "__main__":
    configure()
