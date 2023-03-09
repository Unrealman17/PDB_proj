SELECT  ex.experiment, 
        `_exptl.entry_id` as entry_id,
        `_exptl.method` as method
        --trim(BOTH "'" FROM `_exptl.method` ) as method
  FROM bronze_extra m
  join register_pdb_actualizer ex
    on m.experiment = ex.experiment;