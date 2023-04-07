import pytest
import sys
import os

sys.path.append(os.getcwd())
from pdb_helper import spark
from installer import Tables_config, remove, path_exists, CONFIG, DTable
from fill_config_table import fill
from download_unzip_all import download_unzip_all_fn
from bronze import bronze
from python_pipeline.install_run import main as install_run
from python_pipeline.run_bronze import main as run_bronze
from silver import silver


def generate_new_path(path: str) -> str:
    return path
    return path[:-1] + '/test/'


@pytest.fixture
def config() -> dict:
    res = CONFIG.copy()
    res["downloads_path"] = generate_new_path(res["downloads_path"])
    res["external_table_path"] = generate_new_path(res["external_table_path"])
    return res


def generate_external_path(tables: list[str], config: dict) -> list[str]:
    return [os.path.join(config["external_table_path"], name)
            for name in tables]


def get_external_folders(folder_name, config: dict) -> list[str]:
    service_path = os.path.join(config["table_describe_path"], folder_name)
    sql = '.sql'
    service_table_names = [f[:-len(sql)] for f in os.listdir(service_path) if os.path.isfile(
        os.path.join(service_path, f)) and f.endswith(sql)]
    return generate_external_path(service_table_names, config)


@pytest.fixture
def path_list(config) -> list[str]:
    res = get_external_folders('service', config)
    res.extend(get_external_folders('silver_query', config))
    res.extend(generate_external_path(
        [f'bronze_{t}' for t in CONFIG["tables"]], config))
    return res


def create_fake_table(table_path):
    table_name = table_path.split(os.sep)[-1]
    external_location = table_path[:-len(table_name)]
    DTable(name=table_name,
           spark_session=spark,
           category=None,
           external_location=external_location).create_table_as('SELECT 1 as a, 2 as b')


def test_remove(path_list: list[str], config):

    downloads_path = config["downloads_path"]
    for path in path_list:
        if not os.path.exists(path):
            create_fake_table(path)

    tables_config = remove(spark_session=spark, config=config)
    assert path_exists(downloads_path) == False
    external_locations = set()
    for table in tables_config.tables():
        assert table.exist == False, f'Table {table} exists'
        external_locations.add(table.external_location)
    assert external_locations == set(
        path_list), 'something happened with external locations'

# TODO: add checking state


def test_pipeline(config):
    tables_config = Tables_config(
        spark_session=spark, config=config)
    for i in range(2):
        install_run()
        for table in tables_config.service_tables():
            assert os.path.exists(
                table.external_location), f'step {i+1}, table {table.name} does not exist'
        for table in tables_config.bronze_tables():
            assert os.path.exists(
                table.external_location), f'step {i+1}, table {table.name} does not exist'

    silver()
    entity_df = spark.sql(f"select * from silver_entity;").collect()
    for table in tables_config.silver_tables():
        assert os.path.exists(
            table.external_location), f'table {table.name} does not exist'

    run_bronze()

    df = spark.sql(
        f"SELECT checksum FROM register_pdb_actualizer ORDER BY checksum;").collect()
    spark.sql(f"UPDATE register_pdb_actualizer SET checksum = concat('a',checksum);")
    df2 = spark.sql(
        f"SELECT checksum FROM register_pdb_actualizer ORDER BY checksum;").collect()
    run_bronze()
    df3 = spark.sql(
        f"SELECT checksum FROM register_pdb_actualizer ORDER BY checksum;").collect()
    assert df3 == df
    silver()
    entity_df2 = spark.sql(f"select * from silver_entity;").collect()
    assert set(entity_df2) == set(entity_df)

# from pdb_helper import read_config

# test_pipeline(read_config())