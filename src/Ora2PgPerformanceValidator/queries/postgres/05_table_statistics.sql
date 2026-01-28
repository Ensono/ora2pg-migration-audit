-- Get table metadata statistics
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
ORDER BY 
    t.table_name
