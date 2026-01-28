-- List all indexes in the schema with their table names
SELECT
    index_name,
    table_name,
    uniqueness,
    status
FROM 
    all_indexes
WHERE 
    owner = '{ORACLE_SCHEMA}'
ORDER BY 
    table_name, 
    index_name
