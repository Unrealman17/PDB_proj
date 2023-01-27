from installer import Table, Tables_config
from pyspark.sql import SparkSession
from pdb_helper import task


@task
def fill(spark_context: SparkSession, input_path='input.txt'):
    config_pdb_actualizer = Tables_config(spark_context=spark_context).config_table
    with open(input_path)as f:
        txt = f.read()
    cmd = f"""
        insert overwrite {config_pdb_actualizer.name}(experiment,thread) values\n('""" \
        + "',null),\n('".join(txt.strip().split('\n')) \
        + "',null);"

    #spark.sql("truncate table config_pdb_actualizer force;")
    # config_pdb_actualizer.drop()
    # config_pdb_actualizer.create()


    inserted = spark_context.sql(cmd).collect()
    inserted = spark_context.sql(
        "select count(*) from config_pdb_actualizer;").collect()[0][0]
    print(f'Read {inserted} strings')


if __name__ == "__main__":
    fill()
