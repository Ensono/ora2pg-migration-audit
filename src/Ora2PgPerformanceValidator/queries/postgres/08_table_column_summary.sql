-- Complex join of table and column metadata
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
GROUP BY 
    t.table_name
ORDER BY 
    t.table_name
