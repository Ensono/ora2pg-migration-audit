-- Get table metadata statistics
SELECT
    table_name,
    tablespace_name,
    CASE WHEN temporary = 'Y' THEN 'TEMPORARY' ELSE 'PERMANENT' END as table_type,
    CASE WHEN partitioned = 'YES' THEN 'YES' ELSE 'NO' END as is_partitioned
FROM 
    all_tables
WHERE 
    owner = '{ORACLE_SCHEMA}'
ORDER BY 
    table_name
