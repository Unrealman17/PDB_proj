# from helper import spark
import json
from pyspark.sql import SparkSession
import os
import sys
from pdb_helper import read_config, task
from gemmi import cif
from pyspark.sql.types import StructType, StructField, StringType
import shutil
from pathlib import Path

# dbx execute --cluster-id=1108-081632-vxq32gz9 install --no-package
# dbx sync dbfs --source .

config = read_config()

def path_exists(path, dbutils):
    if dbutils:
        try:
            dbutils.fs.ls(path)
            return True
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                return False
            else:
                raise    
    else:
        return os.path.exists(path)


def delete_mounted_dir(dirname, dbutils):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path, dbutils)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, recurse=True)

def rmdir(path, dbutils = None):
    if path_exists(path, dbutils = dbutils):
        print(f'removing {path} . . .', end ='\t')
        if dbutils:
            crutch = 'dbfs:'+self.external_location
            delete_mounted_dir(dirname = crutch, dbutils = dbutils)
        else:
            shutil.rmtree(path)
        print('success')
    else:
        print(f'path "{path}" not found')




class Table:

    buffer_size_per_table = config["buffer_size_per_table"]

    def __init__(self, 
                    name:str, 
                    dml: str = None, 
                    exist: bool = None, 
                    spark_context: SparkSession = None,
                    external_location = None,
                    category: str = None,
    ) -> None:
        self.name = name
        self.external_location = external_location + self.name
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
                        + f" LOCATION '{self.external_location}';"

        self.dml = dml
        self.exist = exist
        self._spark_context = spark_context
        self.schema = None
        self.category = category
        self.data = []
        self.dataframe = None

    def append(self, row: list):
        self.data.append(row)
        if Table.buffer_size_per_table < sys.getsizeof(self.data):
            self.build_dataframe()

    def build_dataframe(self):
        if not self.dataframe:
            self.dataframe = self._spark_context.createDataFrame(self.data, schema=self.schema)
        else:
            new_df = self._spark_context.createDataFrame(self.data, schema=self.schema)
            self.dataframe = self.dataframe.union(new_df)
        self.data = []

    def write(self):
        print(f'write table {self.name}',end='\t')
        if self.data or (self.dataframe and self.dataframe.count() != 0 ):
            spark = self.spark_context
            table_name = self.name
            self.build_dataframe()
            self.dataframe.createOrReplaceTempView(f'tmp_{table_name}')
            spark.sql(f'DROP TABLE IF EXISTS {table_name};')
            if self.external_location:
                cmd = f'''CREATE EXTERNAL TABLE {table_name}
                        LOCATION '{self.external_location}'
                        as SELECT * FROM tmp_{table_name};'''
            else:
                cmd = f'''CREATE TABLE {table_name} 
                        as
                        SELECT * FROM tmp_{table_name};'''

            spark.sql(cmd)
            spark.sql(f'DROP VIEW IF EXISTS tmp_{table_name};')
            print('success')
        else:
            print('skipped')

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
            cmd = f'DROP TABLE IF EXISTS {self.name};'
            print(cmd)
            self.spark_context.sql(cmd).collect()
            self.exist = False

            #if self.external_location:     
            #    if os.path.exists(self.external_location):
            #        print(f'remove {self.external_location}')
            #        shutil.rmtree(self.external_location)
            #    else: 
            #        # for databicks
            #        crutch = 'dbfs:'+self.external_location
            #        if path_exists(crutch, dbutils = self.dbutils):
            #            print(f'remove {crutch}')
            #            delete_mounted_dir(dirname = crutch, dbutils = self.dbutils)
            #        else:
            #            print(f'path "{self.external_location}" not found')


    @property
    def spark_context(self) -> SparkSession: 
        return self._spark_context

    @spark_context.setter
    def spark_context(self, spark: SparkSession):
        self._spark_context = spark
        self.exist = None


class Tables_config:
    # reads "tables" folder and "config.json"
    def __init__(self, tables_path='tables', spark_context: SparkSession = None, dbutils = None) -> None:
        
        config = read_config()
        self.config = config
        self.service = []
        self.dbutils = dbutils
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

        self.data = [Table(f'{l}_{t}', spark_context=spark_context, category='_'+t, external_location=config['table_path'])
                     for l in ["bronze", "silver"]
                     for t in config["tables"]]

        for table in self.data:
            if table.name == 'bronze_extra':
                table.schema = StructType([StructField(fld, StringType(), True) 
                            for fld in config['extra']])
                table.schema.add('experiment',StringType(), True)
                self.bronze_extra = table
                break

    def tables(self) -> list[Table]:
        for table in self.service:
            yield table
        for table in self.data:
            yield table
    
    def bronze_category_tables(self) -> list[Table]:
        for table in self.bronze_tables():
            if self.bronze_extra.name != table.name:
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
        
        for table in self.bronze_category_tables():
            if not table.schema:
                schema = StructType([StructField(fld, StringType(), True) 
                            for fld in block.get_mmcif_category(table.category)])
                if len(schema) != 0:
                    schema.add('id_log_experiment',StringType(), True)
                    
                    table.schema = schema



def prepare_path(path):
    if path.startwith('/dbfs/'):
                    
@task
def remove(spark_context: SparkSession, tables_config: Tables_config):
    
    if not tables_config:
        tables_config = Tables_config(spark_context = spark_context)

    for table in tables_config.tables():
        table.drop()

    spark_context.sql('SHOW TABLES;').show()
    
    downloads_path = tables_config.config['downloads_path']
    rmdir(downloads_path, dbutils = tables_config.dbutils)
    
    table_path = crutch = 'dbfs:'+tables_config.config['table_path']
    rmdir(table_path, dbutils = tables_config.dbutils)
    
    


@task
def install(spark_context: SparkSession, tables_config: Tables_config = None):

    if not tables_config:
        tables_config = Tables_config(spark_context = spark_context, dbutils=dbutils)

    for table in tables_config.service:
        table.create()

    spark_context.sql('SHOW TABLES;').show()
    
    downloads_path = tables_config.config['downloads_path']
        
    Path(downloads_path).mkdir(parents=True)
    
@task
def reinstall(spark_context: SparkSession, dbutils=None):
    tables_config = Tables_config(spark_context = spark_context, dbutils=dbutils)
    remove(spark_context = spark_context, tables_config = tables_config)
    install(spark_context = spark_context, tables_config = tables_config)


if __name__ == "__main__":
    reinstall()
