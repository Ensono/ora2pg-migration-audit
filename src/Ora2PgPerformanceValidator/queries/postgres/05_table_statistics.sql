-- Get table metadata statistics
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
    COALESCE(ts.spcname, 'default') as tablespace_name,
    t.table_type,
    CASE WHEN c.relkind = 'p' THEN 'YES' ELSE 'NO' END as is_partitioned
FROM 
    information_schema.tables t
    LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = (
        SELECT oid FROM pg_namespace WHERE nspname = '{POSTGRES_SCHEMA}'
    )
    LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
WHERE 
    t.table_schema = '{POSTGRES_SCHEMA}'
    AND t.table_type = 'BASE TABLE'
    AND t.table_name NOT IN (SELECT relname FROM partition_children)
ORDER BY 
    t.table_name
