-- Get constraints (primary keys, foreign keys, unique, check)
SELECT
    constraint_name,
    constraint_type,
    table_name,
    status
FROM 
    all_constraints
WHERE 
    owner = '{ORACLE_SCHEMA}'
ORDER BY 
    table_name,
    constraint_type,
    constraint_name
