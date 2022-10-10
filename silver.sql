-- Databricks notebook source
drop table if exists silver_chem_comp;
-- create table silver_chem_comp as
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
  join log_experiment_pdb_actualizer ex
    on m.id_log_experiment = ex.id;


-- COMMAND ----------

OPTIMIZE bronze_chem_comp;

-- COMMAND ----------

/*
_extra
_chem_comp
_entity_poly_seq
_entity
*/

SELECT  ex.experiment, 
        m.id,
        m.date,
        m.pdb_id,
        m.replace_pdb_id,
        m.details
  FROM bronze_pdbx_database_PDB_obs_spr m
  join log_experiment_pdb_actualizer ex
    on m.id_log_experiment = ex.id;
