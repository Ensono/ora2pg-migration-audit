-- List all tables and their columns in the schema
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
    table_name,
    column_name,
    data_type,
    character_maximum_length as data_length,
    numeric_precision as data_precision,
    numeric_scale as data_scale,
    is_nullable as nullable
FROM 
    information_schema.columns
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
    AND table_name NOT IN (SELECT relname FROM partition_children)
ORDER BY 
    table_name, 
    ordinal_position
