def main():
    import os
    import sys
    import urllib.request
    import gzip
    import shutil
    from pyspark.sql import SQLContext
    sys.path.append(os.getcwd())

    from pdb_helper import spark, read_config
    from installer import reinstall
    from fill_config_table import fill
    from configure import configure
    from download_file import download_unzip
    from bronze import bronze

    config = read_config()
    download_thread = config['download_thread']

    reinstall(spark_context = spark, config = config)
    fill(spark_context = spark, config = config)
    configure(spark_context = spark, config = config)
    def dnunzip(experiment):
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

    res = spark.sql('select experiment from config_pdb_actualizer')
    res = res.rdd.map(lambda x: str(x[0]))
    res = res.map(dnunzip)
    res = res.collect()
    #download_unzip(i)

    bronze(spark_context = spark, config = config)

if __name__ == "__main__":
    main()