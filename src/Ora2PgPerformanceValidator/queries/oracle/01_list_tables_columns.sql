-- List all tables and their columns in the schema
SELECT
    table_name,
    column_name,
    data_type,
    data_length,
    data_precision,
    data_scale,
    nullable
FROM 
    all_tab_columns
WHERE 
    owner = '{ORACLE_SCHEMA}'
ORDER BY 
    table_name, 
    column_id
