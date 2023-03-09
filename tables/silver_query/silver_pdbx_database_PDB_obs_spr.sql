SELECT  ex.experiment, 
        m.id,
        cast(m.date as date) as date,
        m.pdb_id,
        m.replace_pdb_id,
        m.details
  FROM bronze_pdbx_database_PDB_obs_spr m
  join register_pdb_actualizer ex
    on m.id_log_experiment = ex.experiment;