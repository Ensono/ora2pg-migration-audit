-- Count total number of tables in the schema
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
    COUNT(*) as table_count
FROM 
    information_schema.tables
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
    AND table_type = 'BASE TABLE'
    AND table_name NOT IN (SELECT relname FROM partition_children)
