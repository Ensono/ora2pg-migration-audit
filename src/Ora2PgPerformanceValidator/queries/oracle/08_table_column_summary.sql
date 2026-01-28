-- Complex join of table and column metadata
SELECT
    t.table_name,
    COUNT(c.column_name) as column_count,
    SUM(c.data_length) as total_data_length,
    COUNT(CASE WHEN c.nullable = 'Y' THEN 1 END) as nullable_columns,
    COUNT(CASE WHEN c.nullable = 'N' THEN 1 END) as not_null_columns
FROM 
    all_tables t
    LEFT JOIN all_tab_columns c ON t.owner = c.owner AND t.table_name = c.table_name
WHERE 
    t.owner = '{ORACLE_SCHEMA}'
GROUP BY 
    t.table_name
ORDER BY 
    t.table_name
