from pdb_helper import read_config, task
from pyspark.sql import SparkSession
import hashlib
from gemmi import cif
import time
from datetime import datetime
from installer import Tables_config, CONFIG
start_time = time.time()
start_ts = datetime.now()


@task
def silver(spark_session: SparkSession, config: dict):
    tables_config = Tables_config(
        spark_session=spark_session, config=config)

    for table in tables_config.silver_tables():
        table.create_or_replace()
        table.head()


if __name__ == "__main__":
    silver()


# MAGIC %sql
# MAGIC -- _chem_comp
# MAGIC drop table if exists silver_chem_comp;
# MAGIC create EXTERNAL table silver_chem_comp
# MAGIC using delta LOCATION '/mnt/testblobstorage/data/silver_chem_comp'
# MAGIC as
# MAGIC SELECT  ex.experiment,
# MAGIC         m.id,
# MAGIC         m.type,
# MAGIC         case
# MAGIC           when m.mon_nstd_flag in ('y','yes')
# MAGIC             then true
# MAGIC           when m.mon_nstd_flag in ('n','no')
# MAGIC             then false
# MAGIC           else null
# MAGIC         end as mon_nstd_flag,
# MAGIC         m.name,
# MAGIC         m.pdbx_synonyms,
# MAGIC         m.formula,
# MAGIC         cast(m.formula_weight as float) as formula_weight
# MAGIC   FROM bronze_chem_comp m
# MAGIC   join register_pdb_actualizer ex
# MAGIC     on m.id_log_experiment = ex.experiment;
# MAGIC
# MAGIC ------------------
# MAGIC SELECT * FROM silver_chem_comp limit 10;

# COMMAND ----------

# MAGIC %py
# MAGIC rmdir("dbfs:/mnt/testblobstorage/data/silver_pdbx_database_PDB_obs_spr", dbutils)

# COMMAND ----------

# MAGIC %py
# MAGIC # replace using Tables_config
# MAGIC if spark._jsparkSession.catalog().tableExists('default', 'bronze_pdbx_database_PDB_obs_spr'):
# MAGIC     spark.sql('drop table if exists silver_pdbx_database_PDB_obs_spr;')
# MAGIC     spark.sql('''create table silver_pdbx_database_PDB_obs_spr as
# MAGIC                 SELECT  ex.experiment,
# MAGIC                         m.id,
# MAGIC                         cast(m.date as date) date,
# MAGIC                         m.pdb_id,
# MAGIC                         m.replace_pdb_id,
# MAGIC                         m.details
# MAGIC                   FROM bronze_pdbx_database_PDB_obs_spr m
# MAGIC                   join register_pdb_actualizer ex
# MAGIC                     on m.id_log_experiment = ex.experiment;''')
# MAGIC     display(spark.sql('SELECT * FROM silver_pdbx_database_PDB_obs_spr limit 10;'))
# MAGIC else:
# MAGIC     print("_pdbx_database_PDB_obs_spr skipped")

# COMMAND ----------

# MAGIC %py
# MAGIC rmdir("dbfs:/mnt/testblobstorage/data/silver_extra", dbutils)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC _extra
# MAGIC */
# MAGIC drop table if exists silver_extra;
# MAGIC create table silver_extra
# MAGIC using delta LOCATION '/mnt/testblobstorage/data/silver_extra'
# MAGIC as
# MAGIC SELECT  ex.experiment,
# MAGIC         `_exptl.entry_id` as entry_id,
# MAGIC         `_exptl.method` as method
# MAGIC         --trim(BOTH "'" FROM `_exptl.method` ) as method
# MAGIC   FROM bronze_extra m
# MAGIC   join register_pdb_actualizer ex
# MAGIC     on m.experiment = ex.experiment;
# MAGIC ------------------
# MAGIC SELECT * FROM silver_extra limit 10;

# COMMAND ----------

# MAGIC %py
# MAGIC rmdir("dbfs:/mnt/testblobstorage/data/silver_entity_poly_seq", dbutils)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC _entity_poly_seq
# MAGIC */
# MAGIC drop table if exists silver_entity_poly_seq;
# MAGIC create table silver_entity_poly_seq as
# MAGIC SELECT  ex.experiment,
# MAGIC         cast(m.entity_id as int) as entity_id,
# MAGIC         cast(m.num as int) as num,
# MAGIC         m.mon_id,
# MAGIC         m.hetero
# MAGIC   FROM bronze_entity_poly_seq m
# MAGIC   join register_pdb_actualizer ex
# MAGIC     on m.id_log_experiment = ex.experiment;
# MAGIC ------------------
# MAGIC SELECT * FROM silver_entity_poly_seq limit 10;

# COMMAND ----------

# MAGIC %py
# MAGIC rmdir("dbfs:/mnt/testblobstorage/data/silver_entity", dbutils)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- _entity
# MAGIC
# MAGIC drop table if exists silver_entity;
# MAGIC create table silver_entity as
# MAGIC SELECT  ex.experiment,
# MAGIC         cast(m.id as int) as id,
# MAGIC         m.type,
# MAGIC         m.src_method,
# MAGIC         m.pdbx_description,
# MAGIC         cast(m.formula_weight as float) as formula_weight,
# MAGIC         cast(m.pdbx_number_of_molecules as int) as pdbx_number_of_molecules,
# MAGIC         m.pdbx_ec,
# MAGIC         m.pdbx_mutation,
# MAGIC         m.pdbx_fragment,
# MAGIC         m.details
# MAGIC   FROM bronze_entity m
# MAGIC   join register_pdb_actualizer ex
# MAGIC     on m.id_log_experiment = ex.experiment;
# MAGIC ------------------
# MAGIC SELECT * FROM silver_entity limit 10;

# COMMAND ----------
