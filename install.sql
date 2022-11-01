-- Databricks notebook source
drop TABLE IF EXISTS config_pdb_actualizer;

drop TABLE IF EXISTS history_pdb_actualizer;
drop TABLE IF EXISTS register_pdb_actualizer;

drop TABLE IF EXISTS bronze_chem_comp;
drop TABLE IF EXISTS bronze_entity_poly_seq;
drop TABLE IF EXISTS bronze_pdbx_database_PDB_obs_spr;
drop TABLE IF EXISTS bronze_entity;
drop TABLE IF EXISTS bronze_extra;

drop TABLE IF EXISTS silver_chem_comp;
drop TABLE IF EXISTS silver_entity_poly_seq;
drop TABLE IF EXISTS silver_pdbx_database_PDB_obs_spr;
drop TABLE IF EXISTS silver_entity;
drop TABLE IF EXISTS silver_extra;


-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/config_pdb_actualizer/", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/history_pdb_actualizer/", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/register_pdb_actualizer/", True)

-- COMMAND ----------

create table config_pdb_actualizer(experiment string);

create table history_pdb_actualizer(
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  begin timestamp,
  end timestamp 
 );
 
create table register_pdb_actualizer(
  id_history BIGINT,
  experiment string,
  checksum string
);
-------------------------------------
-- create table bronze_entity(
--   id_log_experiment BIGINT, 
--   id string, 
--   type string, 
--   src_method string, 
--   pdbx_description string, 
--   formula_weight string, 
--   pdbx_number_of_molecules string, 
--   pdbx_ec string, 
--   pdbx_mutation string, 
--   pdbx_fragment string, 
--   details string
-- );
-- create table bronze_pdbx_database_PDB_obs_spr(
--   id_log_experiment BIGINT, 
--   id string, 
--   date string, 
--   pdb_id string, 
--   replace_pdb_id string, 
--   details string
-- );
-- 
-- create table bronze_entity_poly_seq(
--   id_log_experiment BIGINT, 
--   entity_id string, 
--   num string, 
--   mon_id string, 
--   hetero string
-- );
-- 
-- create table bronze_chem_comp(
--   id_log_experiment BIGINT, 
--   id string, 
--   type string, 
--   mon_nstd_flag string, 
--   name string, 
--   pdbx_synonyms string, 
--   formula string, 
--   formula_weight string
-- );
-- create table bronze_extra(
--   id_log_experiment BIGINT, 
--   `_exptl.entry_id` string, 
--   `_exptl.method` string
-- );

