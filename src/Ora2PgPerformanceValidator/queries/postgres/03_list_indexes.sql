-- List all indexes in the schema with their table names
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
ORDER BY 
    i.tablename, 
    i.indexname
