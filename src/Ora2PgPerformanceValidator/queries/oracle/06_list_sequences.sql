-- List all sequences in the schema
SELECT
    sequence_name,
    min_value,
    max_value,
    increment_by,
    last_number as last_value
FROM 
    all_sequences
WHERE 
    sequence_owner = '{ORACLE_SCHEMA}'
ORDER BY 
    sequence_name
