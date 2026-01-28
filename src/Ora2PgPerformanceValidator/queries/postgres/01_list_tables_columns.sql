-- List all tables and their columns in the schema
SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length as data_length,
    numeric_precision as data_precision,
    numeric_scale as data_scale,
    is_nullable as nullable
FROM 
    information_schema.columns
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    table_name, 
    ordinal_position
