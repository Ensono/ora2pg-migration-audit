-- List all sequences in the schema
SELECT
    sequence_name,
    CAST(start_value AS BIGINT) as start_value,
    CAST(increment AS BIGINT) as increment_by,
    CAST(minimum_value AS BIGINT) as min_value,
    CAST(maximum_value AS BIGINT) as max_value,
    cycle_option
FROM 
    information_schema.sequences
WHERE 
    sequence_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    sequence_name
