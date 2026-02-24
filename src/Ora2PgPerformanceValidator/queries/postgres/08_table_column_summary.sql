-- Complex join of table and column metadata
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
    t.table_name,
    COUNT(c.column_name) as column_count,
    SUM(c.character_maximum_length) as total_data_length,
    COUNT(CASE WHEN c.is_nullable = 'YES' THEN 1 END) as nullable_columns,
    COUNT(CASE WHEN c.is_nullable = 'NO' THEN 1 END) as not_null_columns
FROM 
    information_schema.tables t
    LEFT JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE 
    t.table_schema = '{POSTGRES_SCHEMA}'
    AND t.table_type = 'BASE TABLE'
    AND t.table_name NOT IN (SELECT relname FROM partition_children)
GROUP BY 
    t.table_name
ORDER BY 
    t.table_name
