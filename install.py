# from helper import spark
import json
from pyspark.sql import SparkSession
import os
from pdb_helper import read_config, task
from gemmi import cif
from pyspark.sql.types import StructType, StructField, StringType

# dbx execute --cluster-id=1108-081632-vxq32gz9 install --no-package
# dbx sync dbfs --source .

class Table:

    def __init__(self, 
                    name:str, 
                    dml: str = None, 
                    exist: bool = None, 
                    spark_context: SparkSession = None,
                    external_location = None
    ) -> None:
        self.name = name
        self.external_location = external_location
        if self.external_location and dml:
            create_table = 'create table '
            lower = dml.lower()
            if create_table in lower:
                start = lower.find(create_table)
                finish = start + len(create_table)
                end = dml.rfind(';')
                
                dml = dml[:start] \
                        + 'CREATE EXTERNAL TABLE '\
                        + dml[finish:end]\
                        + f" LOCATION '{external_location}{os.sep}{self.name}';"

        self.dml = dml
        self.exist = exist
        self._spark_context = spark_context
        self.schema = None

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)

    def create(self):
        if self.dml:
            if self.spark_context:
                self.spark_context.sql(self.dml)
                self.exist = True
                return
            raise Exception('spark is not initialized')
        raise Exception('DML is empty')

    def drop(self):
        if self.spark_context:
            self.spark_context.sql(f'DROP TABLE IF EXISTS {self.name};')
            self.exist = False

    @property
    def spark_context(self) -> SparkSession: 
        return self._spark_context

    @spark_context.setter
    def spark_context(self, spark: SparkSession):
        self._spark_context = spark
        self.exist = None


class Tables_config:
    # reads "tables" folder and "config.json"
    def __init__(self, tables_path='tables', spark_context: SparkSession = None,) -> None:
        
        config = read_config()

        self.service = []
        for file_name in os.listdir(tables_path):
            file_path = os.path.join(tables_path, file_name)
            if os.path.isfile(file_path):
                if file_name.endswith('.sql'):
                    with open(file_path) as f:
                        dml = f.read()
                    table = Table(file_name[:-len('.sql')], dml=dml, external_location=config['table_path'], spark_context=spark_context)
                    self.service.append(table)

                    if table.name.startswith('config'):
                        self.config_table = table
                    elif table.name.startswith('history'):
                        self.history_table = table
                    elif table.name.startswith('register'):
                        self.register_table = table

        self.data = [Table(f'{l}_{t}', spark_context=spark_context)
                     for l in ["bronze", "silver"]
                     for t in config["tables"]]

    def tables(self) -> list:
        for table in self.service:
            yield table
        for table in self.data:
            yield table
    
    def bronze_tables(self) -> list[Table]:
        for table in self.data:
            if table.name.startswith("bronze"):
                yield table

    def silver_tables(self) -> list[Table]:
        for table in self.data:
            if table.name.startswith("silver"):
                yield table

    def init_bronze_schema(self, file_name:str = None, block = None):

        if file_name:
            block = cif.read_file(file_name).sole_block()  # mmCIF has exactly one block

        for table in self.bronze_tables():
            table.schema = StructType([StructField(fld, StringType(), True) 
                        for fld in block.get_mmcif_category(table.name)])
            if len(table.schema) != 0:
                table.schema.add('id_log_experiment',StringType(), True)





@task
def install(spark_context: SparkSession):

    tables_config = Tables_config(spark_context = spark_context)

    for table in tables_config.tables():
        table.drop()

    for table in tables_config.service:
        table.create()

    spark_context.sql('SHOW TABLES;').show()

if __name__ == "__main__":
    install()
