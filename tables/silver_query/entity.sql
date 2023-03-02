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