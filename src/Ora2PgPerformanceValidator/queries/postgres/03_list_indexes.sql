-- List all indexes in the schema with their table names
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
    i.indexname as index_name,
    i.tablename as table_name,
    CASE 
        WHEN ix.indisunique THEN 'UNIQUE'
        ELSE 'NONUNIQUE'
    END as uniqueness,
    CASE 
        WHEN ix.indisvalid THEN 'VALID'
        ELSE 'INVALID'
    END as status
FROM 
    pg_indexes i
    JOIN pg_class c ON c.relname = i.indexname
    JOIN pg_index ix ON ix.indexrelid = c.oid
    JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE 
    i.schemaname = '{POSTGRES_SCHEMA}'
    AND i.tablename NOT IN (SELECT relname FROM partition_children)
ORDER BY 
    i.tablename, 
    i.indexname
