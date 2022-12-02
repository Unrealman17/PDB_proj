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
        self.service = [Table(f'{k}_{name}', cols)
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


# dbutils.fs.rm("dbfs:/user/hive/warehouse/config_pdb_actualizer/", True)
# dbutils.fs.rm("dbfs:/user/hive/warehouse/history_pdb_actualizer/", True)
# dbutils.fs.rm("dbfs:/user/hive/warehouse/register_pdb_actualizer/", True)


# create table config_pdb_actualizer(experiment string)

# create table history_pdb_actualizer(
#     id BIGINT GENERATED ALWAYS AS IDENTITY,
#     begin timestamp,
#     end timestamp
# )

# create table register_pdb_actualizer(
#     id_history BIGINT,
#     experiment string,
#     checksum string
# )
# -------------------------------------
# -- create table bronze_entity(
#     -- id_log_experiment BIGINT,
#     -- id string,
#     -- type string,
#     -- src_method string,
#     -- pdbx_description string,
#     -- formula_weight string,
#     -- pdbx_number_of_molecules string,
#     -- pdbx_ec string,
#     -- pdbx_mutation string,
#     -- pdbx_fragment string,
#     -- details string
#     - -)
# -- create table bronze_pdbx_database_PDB_obs_spr(
#     -- id_log_experiment BIGINT,
#     -- id string,
#     -- date string,
#     -- pdb_id string,
#     -- replace_pdb_id string,
#     -- details string
#     - -)
# --
# -- create table bronze_entity_poly_seq(
#     -- id_log_experiment BIGINT,
#     -- entity_id string,
#     -- num string,
#     -- mon_id string,
#     -- hetero string
#     - -)
# --
# -- create table bronze_chem_comp(
#     -- id_log_experiment BIGINT,
#     -- id string,
#     -- type string,
#     -- mon_nstd_flag string,
#     -- name string,
#     -- pdbx_synonyms string,
#     -- formula string,
#     -- formula_weight string
#     - -)
# -- create table bronze_extra(
#     -- id_log_experiment BIGINT,
#     -- `_exptl.entry_id` string,
#     -- `_exptl.method` string
#     - -)
