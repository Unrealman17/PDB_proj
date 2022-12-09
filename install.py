# from helper import spark
import json
from pyspark.sql import SparkSession
import os


class Table:

    def __init__(self, name:str, dml: str = None, exist: bool = None, spark: SparkSession = None) -> None:
        self.name = name
        self.dml = dml
        self.exist = exist
        self._spark = spark

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)

    def create(self):
        if self.dml:
            if self.spark:
                self.spark.sql(self.dml)
                self.exist = True
                return
            raise Exception('spark is not initialized')
        raise Exception('DML is empty')

    def drop(self):
        if self.spark:
            self.spark.sql(f'DROP TABLE IF EXISTS {self.name};')
            self.exist = False

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @spark.setter
    def spark(self, spark: SparkSession):
        self._spark = spark
        self.exist = None


class Tables_config:
    # reads "tables" folder and "config.json"
    def __init__(self, tables_path='tables', config_path='config.json') -> None:
        self.service = []
        for file_name in os.listdir(tables_path):
            file_path = os.path.join(tables_path, file_name)
            if os.path.isfile(file_path):
                if file_name.endswith('.sql'):
                    with open(file_path) as f:
                        dml = f.read()
                    self.service.append(Table(file_name[:-len('.sql')], dml))

        with open(config_path) as f:
            config = json.load(f)
        config["tables"].append('extra')
        self.data = [Table(f'{l}_{t}')
                     for l in ["bronze", "silver"]
                     for t in config["tables"]]

    def tables(self) -> list[Table]:
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

def install(spark: SparkSession):

    tables_config = Tables_config()

    for table in tables_config.tables():
        table.spark = spark
        table.drop()

    for table in tables_config.service:
        table.create()


if __name__ == "__main__":
    from helper import spark
    install(spark)
    spark.sql('SHOW TABLES;').show()
