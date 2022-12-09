from helper import spark

spark.sql('SHOW TABLES;').show()
spark.sql("DESCRIBE TABLE EXTENDED config_pdb_actualizer;").show()
# spark.sql('SHOW TABLE config_pdb_actualizer;').show()
spark.sql('SELECT * FROM config_pdb_actualizer;').show()

