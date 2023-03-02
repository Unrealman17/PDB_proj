SELECT  ex.experiment, 
        cast(m.entity_id as int) as entity_id,
        cast(m.num as int) as num,
        m.mon_id,
        m.hetero
  FROM bronze_entity_poly_seq m
  join register_pdb_actualizer ex
    on m.id_log_experiment = ex.experiment;