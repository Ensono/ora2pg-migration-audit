-- List all views in the schema
SELECT
    view_name,
    text_length,
    read_only
FROM 
    all_views
WHERE 
    owner = '{ORACLE_SCHEMA}'
ORDER BY 
    view_name
