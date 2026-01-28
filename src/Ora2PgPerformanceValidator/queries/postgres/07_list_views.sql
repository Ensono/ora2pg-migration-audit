-- List all views in the schema
SELECT
    table_name as view_name,
    LENGTH(view_definition) as text_length,
    CASE 
        WHEN is_updatable = 'NO' THEN 'YES'
        ELSE 'NO'
    END as read_only
FROM 
    information_schema.views
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    table_name
