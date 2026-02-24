-- Get constraints (primary keys, foreign keys, unique, check)
WITH partition_children AS (
    SELECT child.relname
    FROM pg_catalog.pg_partitioned_table part
    JOIN pg_catalog.pg_class parent ON parent.oid = part.partrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = parent.relnamespace
    JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parent.oid
    JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
    WHERE n.nspname = '{POSTGRES_SCHEMA}'
)
SELECT
    tc.constraint_name,
    tc.constraint_type,
    tc.table_name,
    'ENABLED' as status
FROM 
    information_schema.table_constraints tc
WHERE 
    tc.table_schema = '{POSTGRES_SCHEMA}'
    AND tc.table_name NOT IN (SELECT relname FROM partition_children)
ORDER BY 
    tc.table_name,
    tc.constraint_type,
    tc.constraint_name
