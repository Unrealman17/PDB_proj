-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_chem_comp/", True)

-- COMMAND ----------

-- _chem_comp
drop table if exists silver_chem_comp;
create table silver_chem_comp as
SELECT  ex.experiment, 
        m.id,
        m.type,
        case
          when m.mon_nstd_flag in ('y','yes')
            then true
          when m.mon_nstd_flag in ('n','no')
            then false
          else null
        end as mon_nstd_flag,
        m.name,
        m.pdbx_synonyms,
        m.formula,
        cast(m.formula_weight as float) as formula_weight
  FROM bronze_chem_comp m
  join register_pdb_actualizer ex
    on m.id_log_experiment = ex.experiment;
------------------
SELECT * FROM silver_chem_comp limit 10;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_pdbx_database_pdb_obs_spr/", True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if spark._jsparkSession.catalog().tableExists('default', 'bronze_pdbx_database_PDB_obs_spr'):
-- MAGIC     spark.sql('drop table if exists silver_pdbx_database_PDB_obs_spr;')
-- MAGIC     spark.sql('''create table silver_pdbx_database_PDB_obs_spr as
-- MAGIC                 SELECT  ex.experiment, 
-- MAGIC                         m.id,
-- MAGIC                         cast(m.date as date) date,
-- MAGIC                         m.pdb_id,
-- MAGIC                         m.replace_pdb_id,
-- MAGIC                         m.details
-- MAGIC                   FROM bronze_pdbx_database_PDB_obs_spr m
-- MAGIC                   join register_pdb_actualizer ex
-- MAGIC                     on m.id_log_experiment = ex.experiment;''')
-- MAGIC     display(spark.sql('SELECT * FROM silver_pdbx_database_PDB_obs_spr limit 10;'))
-- MAGIC else:
-- MAGIC   print("_pdbx_database_PDB_obs_spr skipped")

-- COMMAND ----------

-- _pdbx_database_PDB_obs_spr


------------------


-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_extra/", True)

-- COMMAND ----------

/*
_extra
*/
drop table if exists silver_extra;
create table silver_extra as
SELECT  ex.experiment, 
        `_exptl.entry_id` as entry_id,
        `_exptl.method` as method
        --trim(BOTH "'" FROM `_exptl.method` ) as method
  FROM bronze_extra m
  join register_pdb_actualizer ex
    on m.experiment = ex.experiment;
------------------
SELECT * FROM silver_extra limit 10;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_entity_poly_seq/", True)

-- COMMAND ----------

/*
_entity_poly_seq
*/
drop table if exists silver_entity_poly_seq;
create table silver_entity_poly_seq as
SELECT  ex.experiment, 
        cast(m.entity_id as int) as entity_id,
        cast(m.num as int) as num,
        m.mon_id,
        m.hetero
  FROM bronze_entity_poly_seq m
  join register_pdb_actualizer ex
    on m.id_log_experiment = ex.experiment;
------------------
SELECT * FROM silver_entity_poly_seq limit 10;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_entity/", True)

-- COMMAND ----------

-- _entity

drop table if exists silver_entity;
create table silver_entity as
SELECT  ex.experiment, 
        cast(m.id as int) as id,
        m.type,
        m.src_method,
        m.pdbx_description,
        cast(m.formula_weight as float) as formula_weight,
        cast(m.pdbx_number_of_molecules as int) as pdbx_number_of_molecules,
        m.pdbx_ec,
        m.pdbx_mutation,
        m.pdbx_fragment,
        m.details
  FROM bronze_entity m
  join register_pdb_actualizer ex
    on m.id_log_experiment = ex.experiment;
------------------
SELECT * FROM silver_entity limit 10;
