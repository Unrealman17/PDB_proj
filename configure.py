import shutil
from pathlib import Path
from pyspark.storagelevel import StorageLevel
from pdb_helper import read_config, task
from fill_config_table import fill
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

@task
def configure(spark_session: SparkSession, config:dict):
    '''
        Mark pipeline as started.
    '''
    # start pipeline
    spark_session.sql("""insert into history_pdb_actualizer(begin,end)
                    select now(), null;""")

    spark_session.sql("select experiment from config_pdb_actualizer;").show()


if __name__ == "__main__":
    configure()
