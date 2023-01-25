import gzip
import os
import shutil
import sys
from pyspark.sql import SparkSession
import urllib.request

from pdb_helper import task, read_config



@task
def download_unzip(thread, spark_context: SparkSession,) -> str:
    downloads_path = read_config()['downloads_path']
    experiments = spark_context.sql(f"select experiment from config_pdb_actualizer where thread = {thread};").collect()
    res = []
    for e in experiments:
        experiment = e.experiment
        url = f'https://files.wwpdb.org/pub/pdb/data/structures/all/mmCIF/{experiment.lower()}.cif.gz'
        gz_file_name = f"{downloads_path}{experiment}.cif.gz"
        file_name = gz_file_name[:-len('.gz')]
        urllib.request.urlretrieve(url, gz_file_name)
        with gzip.open(gz_file_name, 'rb') as f_in:
            with open(file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_file_name)
        res.append(file_name)
    return res

if __name__ == "__main__":
    download_unzip(sys.argv[1].upper())