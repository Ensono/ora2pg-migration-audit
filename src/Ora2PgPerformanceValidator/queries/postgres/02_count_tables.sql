-- Count total number of tables in the schema
SELECT
    COUNT(*) as table_count
FROM 
    information_schema.tables
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
    AND table_type = 'BASE TABLE'
