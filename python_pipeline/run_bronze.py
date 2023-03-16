def main():
    import os
    import sys
    sys.path.append(os.getcwd())

    from pdb_helper import spark, read_config
    from installer import reinstall
    from fill_config_table import fill
    from configure import configure
    from download_unzip_all import download_unzip_all_fn
    from bronze import bronze

    config = read_config()

    fill(spark_session = spark)
    configure(spark_session = spark)

    download_unzip_all_fn(spark_session = spark, config = config)

    bronze(spark_session = spark)

if __name__ == "__main__":
    main()