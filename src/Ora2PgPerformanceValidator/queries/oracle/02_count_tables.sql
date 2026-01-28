-- Count total number of tables in the schema
SELECT
    COUNT(*) as table_count
FROM 
    all_tables
WHERE 
    owner = '{ORACLE_SCHEMA}'
