-- List all sequences in the schema
SELECT
    sequencename as sequence_name,
    min_value,
    max_value,
    increment_by,
    last_value
FROM 
    pg_sequences
WHERE 
    schemaname = '{POSTGRES_SCHEMA}'
ORDER BY 
    sequencename
