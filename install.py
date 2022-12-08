# from helper import spark
import json
from pyspark.sql import SparkSession

class Table:
    # contains data from "tables.json" about one table 
    def __init__(self, name, columns=None) -> None:
        self.name = name
        self.columns = columns

    def __str__(self) -> str:
        return self.name
    
    def __repr__(self) -> str:
        return str(self)

    @property
    def create_cmd(self) -> str:
        if self.columns:
            return f'create table {self.name}(\n'     \
                + ",\n    ".join(self.columns) \
                + ");"
        raise Exception('Columns definition is empty')

    @property
    def drop_cmd(self) -> str:
        return f'DROP TABLE IF EXISTS {self.name};'


class Tables_config:
    # reads "tables.json"
    def __init__(self, path='tables.json') -> None:
        with open(path) as f:
            data = f.read()
        data = json.loads(data)
        k = 'pdb_actualizer'
        self.service = [Table(f'{name}_{k}', cols)
                        for name, cols in data[k].items()]

        data = data['data']
        self.data = [Table(f'{l}_{t}') 
                        for l in data["layers"]
                        for t in data["tables"]]

    def tables(self) -> list[Table]:
        for table in self.service:
            yield table
        for table in self.data:
            yield table

def install(spark:SparkSession):
    
    tables_config = Tables_config()

    for table in tables_config.tables():
        spark.sql(table.drop_cmd)
    
    for table in tables_config.service:
        spark.sql(table.create_cmd)

if __name__ == "__main__":
    from helper import spark
    install(spark)
    spark.sql('SHOW TABLES;').show()
