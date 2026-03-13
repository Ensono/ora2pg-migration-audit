-- List all sequences in the schema
SELECT
    sequence_name,
    last_number,
    increment_by,
    min_value,
    max_value,
    cycle_flag,
    cache_size
FROM
    dba_sequences
WHERE
    sequence_owner = '{ORACLE_SCHEMA}'
ORDER BY
    sequence_name
