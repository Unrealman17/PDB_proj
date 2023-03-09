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