-- Get constraints (primary keys, foreign keys, unique, check)
SELECT
    tc.constraint_name,
    tc.constraint_type,
    tc.table_name,
    'ENABLED' as status
FROM 
    information_schema.table_constraints tc
WHERE 
    tc.table_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    tc.table_name,
    tc.constraint_type,
    tc.constraint_name
