-- List all views in the schema
SELECT
    table_name as view_name,
    LENGTH(view_definition) as text_length,
    CASE 
        WHEN is_updatable = 'YES' THEN 'NO'
        ELSE 'YES'
    END as read_only
FROM 
    information_schema.views
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    table_name
