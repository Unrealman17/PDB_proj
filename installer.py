# from helper import spark
import json
from pyspark.sql import SparkSession
import os
import sys
from pdb_helper import read_config, task
from pyspark.sql.types import StructType, StructField, StringType
import shutil
from pathlib import Path
import sqlparse

# dbx execute --cluster-id=1108-081632-vxq32gz9 install --no-package
# dbx sync dbfs --source .

CONFIG = read_config()

def dbr_path_exists(path, dbutils):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise    

def path_exists(path, dbutils=None):
    if dbutils:
        path = dbr_prepare_path(path)
        return dbr_path_exists(path, dbutils)
    else:
        return os.path.exists(path)

def find_selected_columns(query:str) -> list[str]:
    tokens = sqlparse.parse(query)[0].tokens
    found_select = False
    for token in tokens:
        if found_select:
            if isinstance(token, sqlparse.sql.IdentifierList):
                return [
                    col.value.split("--")[0].strip().split(" ")[-1].strip("`").rpartition('.')[-1]
                    for col in token.tokens
                    if isinstance(col, sqlparse.sql.Identifier)
                ]
        else:
            found_select = token.match(sqlparse.tokens.Keyword.DML, ["select", "SELECT"])
    raise Exception("Could not find a select statement. Weired query :)")


def dbr_prepare_path(path: str):
    case1 = '/dbfs/'
    if path.startswith(case1):
        path = 'dbfs:' + path[len(case1)-1:]
    elif path.startswith('/') and not path.startswith('/dbfs/'):
        path = 'dbfs:' + path
    return path


def delete_mounted_dir(dirname, dbutils):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path, dbutils)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, recurse=True)

    
def rmdir(path, dbutils = None):
    check = path_exists(path, dbutils)
    if dbutils:
        path = dbr_prepare_path(path)
        delete_funct = lambda arg: delete_mounted_dir(dirname = arg, dbutils = dbutils)
    else:
        delete_funct = shutil.rmtree
        
    if check:
        print(f'removing "{path}" . . .', end ='\t')
        delete_funct(path)
        print('success')
    else:
        print(f'path "{path}" not found')


class Table:

    def __init__(self, 
                    name:str, 
                    spark_context: SparkSession,
                    external_location = None,
                    format = 'delta',
    ) -> None:
        self.name = name
        self.external_location = external_location + self.name
        self._spark_context = spark_context
        self.format = format
        table_exist = spark_context._jsparkSession.catalog().tableExists('default', name)
        
        self.exist = table_exist
        if self.external_location and not table_exist:
            p_exist = path_exists(self.external_location)
            if p_exist:
                cmd = f"create EXTERNAL table {self.name}\n"\
                        +f"LOCATION '{self.external_location}';"
                print(cmd)
                spark_context.sql(cmd)
                self.exist = True
            else: # not p_exist
                self.exist = False

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)

    def head(self, n=5):
        self.spark_context.sql(f'SELECT * FROM {self.name} LIMIT {n}').show()

    def drop(self):        
        if self.spark_context:
            cmd = f'DROP TABLE IF EXISTS {self.name};'
            print(cmd)
            self.spark_context.sql(cmd).collect()
            self.exist = False

    @property
    def spark_context(self) -> SparkSession: 
        return self._spark_context

    @spark_context.setter
    def spark_context(self, spark: SparkSession):
        self._spark_context = spark
        self.exist = None

'''
             Table
           //     \\
ServiceTable       DTable
                  //     \\   
               BTable    STable
'''

class ServiceTable(Table):
    """
        Service Table
    """
    def __init__(self, 
                    name:str, 
                    spark_context: SparkSession,
                    dml: str, 
                    external_location = None,
    ) -> None:
        Table.__init__(self,
                        name=name,
                        spark_context=spark_context,
                        external_location=external_location
                    )
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
    
    def create(self):
        if self.dml:
            if self.spark_context:
                print(self.dml)
                self.spark_context.sql(self.dml)
                self.exist = True
                return
            raise Exception('spark is not initialized')
        raise Exception('DML is empty')

class DTable(Table):
    """
        Data Table
    """
    def __init__(self, 
                    name:str, 
                    spark_context: SparkSession,
                    category: str,
                    external_location = None,
    ) -> None:
        Table.__init__(self,
                        name=name,
                        spark_context=spark_context,
                        external_location=external_location
                    )
        self.category = category
        self.columns = []
    
    def create_table_as(self)->str:
        cmd = f'CREATE '
        if self.external_location:
            cmd += 'EXTERNAL '
        cmd += f'TABLE {self.name}\n'
        cmd += f'USING {self.format}\n'
        if self.external_location:
            cmd += f"LOCATION '{self.external_location}'\n"
        cmd += f'AS\n'
        return cmd

    def insert_overwrite(self)->str:
        if not self.columns:
            raise ValueError('self.columns is empty')
        cmd = f'INSERT OVERWRITE {self.name}('
        cmd += ('\n'+(' '*20))
        cmd += (',\n'+(' '*20)).join(self.columns)
        cmd += ')\n' 
        return cmd

class BTable(DTable):
    """
        Bronze Table
    """
    buffer_size_per_table = CONFIG["buffer_size_per_table"]

    def __init__(self, 
                    name:str, 
                    spark_context: SparkSession,
                    category: str,
                    external_location = None,
    ) -> None:
        DTable.__init__(self,
                        name=name,
                        spark_context=spark_context,
                        external_location=external_location,
                        category=category,
                        )
        self._schema = None
        self.data = []
        self.dataframe = None

    @property
    def schema(self): 
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema
        self.columns = [ f"`{x}`" for x in schema.names]
        

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
            if self.exist:
                cmd = self.insert_overwrite()
            else:
                cmd = self.create_table_as()

            cmd += f'SELECT * FROM tmp_{table_name};'
            print(cmd)
            spark.sql(cmd)
            spark.sql(f'DROP VIEW IF EXISTS tmp_{table_name};')
            print('success')
        else:
            print('skipped')

    def append(self, row: list):
        self.data.append(row)
        if BTable.buffer_size_per_table < sys.getsizeof(self.data):
            self.build_dataframe()


class STable(DTable):
    """
        Silver Table
    """
    def __init__(self, 
                    name:str, 
                    spark_context: SparkSession ,
                    category: str,
                    query:str,
                    external_location = None,
    ) -> None:
        #TODO: replace arg, kwarg
        DTable.__init__(self,
                        name=name,
                        spark_context=spark_context,
                        external_location=external_location,
                        category=category,
                        )
        self.query = query
        self.columns = find_selected_columns(query)

    def create_or_replace(self):
        spark = self.spark_context
        if self.exist:
            cmd = self.insert_overwrite()
        else:
            cmd = self.create_table_as()
        cmd += self.query
        print(cmd)
        spark.sql(cmd)



class Tables_config:
    # reads "tables" folder and "config.json"
    def __init__(self, tables_path='tables', spark_context: SparkSession = None, dbutils = None) -> None:
        self.config = CONFIG
        self.service = []
        self.dbutils = dbutils
        service_path = os.path.join(tables_path,'service')
        for file_name in os.listdir(service_path):
            file_path = os.path.join(service_path, file_name)
            if os.path.isfile(file_path):
                if file_name.endswith('.sql'):
                    with open(file_path) as f:
                        dml = f.read()
                    table = ServiceTable(name=file_name[:-len('.sql')], 
                                        dml=dml, 
                                        external_location=self.config['table_path'], 
                                        spark_context=spark_context
                                    )
                    self.service.append(table)

                    if table.name.startswith('config'):
                        self.config_table = table
                    elif table.name.startswith('history'):
                        self.history_table = table
                    elif table.name.startswith('register'):
                        self.register_table = table
        
        self.bronze = [BTable(
                                name=f'bronze_{t}', 
                                spark_context=spark_context, 
                                category='_'+t, 
                                external_location=self.config['table_path']
                            )
                        for t in self.config["tables"]]

        for table in self.bronze:
            if table.name == 'bronze_extra':
                schema = StructType([StructField(fld, StringType(), True) 
                            for fld in self.config['extra']])
                schema.add('experiment',StringType(), True)
                table.schema = schema
                self.bronze_extra = table
                break

        self.silver = []
        silver_path = os.path.join(tables_path,'silver_query')
        for file_name in os.listdir(silver_path):
            file_path = os.path.join(silver_path, file_name)
            if os.path.isfile(file_path):
                if file_name.endswith('.sql'):
                    with open(file_path) as f:
                        query = f.read()
                    category = file_name[:-len('.sql')]
                    table = STable(name='silver_'+category, 
                                        query=query, 
                                        external_location=self.config['table_path'], 
                                        category='_'+category,
                                        spark_context=spark_context
                                    )
                    self.silver.append(table)

    
    def bronze_tables(self) -> list[BTable]:
        for table in self.bronze:
            yield table

    def silver_tables(self) -> list[STable]:
        for table in self.silver:
            yield table

    def data(self)-> list[DTable]:
        for table in self.bronze_tables():
            yield table
        for table in self.silver_tables():
            yield table

    def tables(self) -> list[Table]:
        for table in self.service:
            yield table
        for table in self.data():
            yield table
    
    def bronze_category_tables(self) -> list[BTable]:
        for table in self.bronze_tables():
            if self.bronze_extra.name != table.name:
                yield table

    def init_bronze_schema(self, file_name:str = None, block = None):
        from gemmi import cif
        if file_name:
            block = cif.read_file(file_name).sole_block()  # mmCIF has exactly one block
        
        for table in self.bronze_category_tables():
            if not table.schema:
                schema = StructType([StructField(fld, StringType(), True) 
                            for fld in block.get_mmcif_category(table.category)])
                if len(schema) != 0:
                    schema.add('id_log_experiment',StringType(), True)
                    
                    table.schema = schema


@task
def remove(spark_context: SparkSession, dbutils = None):
    
    downloads_path = CONFIG['downloads_path']
    rmdir(downloads_path, dbutils = dbutils)
    
    table_path = CONFIG['table_path']
    rmdir(table_path, dbutils = dbutils)

    tables_config = Tables_config(spark_context = spark_context, dbutils=dbutils)

    for table in tables_config.tables():
        table.drop()

    spark_context.sql('SHOW TABLES;').show()   
    
    return tables_config


@task
def install(spark_context: SparkSession, tables_config: Tables_config = None):
    
    downloads_path = tables_config.config['downloads_path']
    
    if tables_config.dbutils:
        tables_config.dbutils.fs.mkdirs(dbr_prepare_path(downloads_path))
    else:
        Path(downloads_path).mkdir(parents=True)
        
    if not tables_config:
        tables_config = Tables_config(spark_context = spark_context)   

    for table in tables_config.service:
        table.create()

    spark_context.sql('SHOW TABLES;').show()
    
    

@task
def reinstall(spark_context: SparkSession, dbutils=None):
    tables_config = remove(spark_context = spark_context, dbutils=dbutils)
    install(spark_context=spark_context, tables_config=tables_config)


if __name__ == "__main__":
    reinstall()
