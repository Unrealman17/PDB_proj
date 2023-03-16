from installer import Table, Tables_config
from pyspark.sql import SparkSession
from pdb_helper import task


@task
def fill(spark_session: SparkSession, config: dict):
    '''
        read data from input.txt and insert into table config_pdb_actualizer
    '''
    config_pdb_actualizer = Tables_config(
        spark_session=spark_session, config=config).config_table
    with open(config["input_file"])as f:
        txt = f.read()
    cmd = f"""
        insert overwrite {config_pdb_actualizer.name}(experiment) values\n('""" \
        + "'),\n('".join(txt.strip().split('\n')) \
        + "');"

    #spark.sql("truncate table config_pdb_actualizer force;")
    # config_pdb_actualizer.drop()
    # config_pdb_actualizer.create()

    inserted = spark_session.sql(cmd).collect()
    inserted = spark_session.sql(
        "select count(*) from config_pdb_actualizer;").collect()[0][0]
    print(f'Read {inserted} strings')


if __name__ == "__main__":
    fill()
