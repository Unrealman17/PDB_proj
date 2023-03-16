import gzip
import os
import shutil
from pyspark.sql import SparkSession
import urllib.request


def download_unzip(experiment: str, config: dict) -> str:
    downloads_path = config['downloads_path']
    url = f'{config["download_url"]}{experiment.lower()}.cif.gz'
    gz_file_name = f"{downloads_path}{experiment}.cif.gz"
    file_name = gz_file_name[:-len('.gz')]
    urllib.request.urlretrieve(url, gz_file_name)
    with gzip.open(gz_file_name, 'rb') as f_in:
        with open(file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(gz_file_name)
    return file_name


def download_unzip_all_fn(spark_session: SparkSession, config: dict) -> str:
    '''
        download and unzip files
        Returns: list of file names
    '''
    res = spark_session.sql('select experiment from config_pdb_actualizer')
    res = res.rdd.map(lambda x: str(x[0]))
    res = res.map(lambda x: download_unzip(x, config=config))
    res = res.collect()
    # download_unzip('4HHB',config=config)
    return res
