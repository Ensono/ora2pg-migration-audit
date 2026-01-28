# Ora2PgPerformanceValidator

A tool to validate and compare query performance between Oracle and PostgreSQL databases after migration.

## Overview

This validator executes matching SQL queries against both Oracle and PostgreSQL databases and compares:
- Execution times
- Row counts returned
- Performance differences

## Features

- **Paired Query Execution**: Matches Oracle and PostgreSQL SQL files by name and executes them in parallel
- **Performance Metrics**: Measures execution time with warmup and multiple measurement runs for accuracy
- **Median Timing**: Uses median of multiple runs to reduce variance
- **Row Count Validation**: Ensures both databases return the same number of rows
- **Performance Thresholds**: Flags queries with >50% performance difference as warnings
- **Comprehensive Reports**: Generates Markdown and HTML reports with detailed results

## Configuration

Add these settings to your `.env` file:

```properties
# Required: Database schemas
ORACLE_SCHEMA=TESTUSER       # Oracle schema name (uppercase)
POSTGRES_SCHEMA=testschema   # PostgreSQL schema name (lowercase)

# Optional: Custom query paths (default: queries/oracle and queries/postgres)
ORACLE_QUERIES_PATH=/path/to/oracle/queries
POSTGRES_QUERIES_PATH=/path/to/postgres/queries

# Optional: Performance test settings
PERF_WARMUP_RUNS=1            # Warmup executions before measurement (default: 1)
PERF_MEASUREMENT_RUNS=3       # Number of measurement runs (default: 3)
PERF_THRESHOLD_PERCENT=50     # Performance difference threshold % (default: 50)
```

### Performance Threshold

The `PERF_THRESHOLD_PERCENT` setting determines when a query is flagged with a warning:

- **Values**: 1-100 (percentage)
- **Default**: 50 (%)
- **Meaning**: If the performance difference between Oracle and PostgreSQL exceeds this percentage, the query gets a ⚠️ Warning status
- **Examples**:
  - `PERF_THRESHOLD_PERCENT=30` - Stricter (30% difference triggers warning)
  - `PERF_THRESHOLD_PERCENT=75` - More lenient (75% difference triggers warning)

The `ORACLE_SCHEMA` and `POSTGRES_SCHEMA` values are automatically replaced in your SQL queries, making them portable across different environments.

## Query Files

### Directory Structure

```
queries/
├── oracle/
│   ├── 01_list_tables_columns.sql
│   ├── 02_count_tables.sql
│   ├── 03_list_indexes.sql
│   ├── 04_list_constraints.sql
│   ├── 05_table_statistics.sql
│   ├── 06_list_sequences.sql
│   ├── 07_list_views.sql
│   └── 08_table_column_summary.sql
└── postgres/
    ├── 01_list_tables_columns.sql
    ├── 02_count_tables.sql
    ├── 03_list_indexes.sql
    ├── 04_list_constraints.sql
    ├── 05_table_statistics.sql
    ├── 06_list_sequences.sql
    ├── 07_list_views.sql
    └── 08_table_column_summary.sql
```

### Naming Convention

- Query files must have the **same name** in both `oracle/` and `postgres/` directories
- Use `.sql` extension
- Suggested naming: `NN_description.sql` (e.g., `01_list_tables.sql`)

### Writing Queries

**Important**: Use parameter placeholders in your queries for portability across environments:

- `{ORACLE_SCHEMA}` - Replaced with the value from ORACLE_SCHEMA in .env
- `{POSTGRES_SCHEMA}` - Replaced with the value from POSTGRES_SCHEMA in .env

**Oracle queries** should use Oracle-specific syntax:
```sql
-- queries/oracle/01_list_tables_columns.sql
SELECT 
    table_name,
    column_name,
    data_type
FROM 
    information_schema.columns
WHERE 
    table_schema = '{ORACLE_SCHEMA}'
ORDER BY 
    table_name, column_name
```

**PostgreSQL queries** should use PostgreSQL-specific syntax:
```sql
-- queries/postgres/01_list_tables_columns.sql
SELECT 
    table_name,
    column_name,
    data_type
FROM 
    information_schema.columns
WHERE 
    table_schema = '{POSTGRES_SCHEMA}'
ORDER BY 
    table_name, column_name
```

**All included queries are generic** and work on any Oracle/PostgreSQL database without requiring specific tables or data. They query database metadata (tables, columns, indexes, constraints, views, sequences, etc.).

## Usage

### Run from solution root:
```bash
dotnet run --project src/Ora2PgPerformanceValidator/Ora2PgPerformanceValidator.csproj
```

### Run from project directory:
```bash
cd src/Ora2PgPerformanceValidator
dotnet run
```

## Reports

Reports are generated in `src/Ora2PgPerformanceValidator/reports/`:

- **Markdown**: `performance-validation-YYYYMMDD-HHMMSS.md`
- **HTML**: `performance-validation-YYYYMMDD-HHMMSS.html`
- **Text**: `performance-validation-YYYYMMDD-HHMMSS.txt`

### Report Sections

1. **Performance Threshold**: Shows the actual threshold % being used for warnings
2. **Summary**: Overall statistics and average execution times
3. **Query Results**: Detailed results for each query pair including:
   - Execution times for both databases
   - Row counts
   - Performance difference percentage
   - Status (Passed/Warning/Failed)
   - Error messages if any

## Performance Status

- **✓ Passed**: Both queries executed successfully with < threshold % performance difference
- **⚠ Warning**: Both executed but > threshold % performance difference (default: 50%)
- **❌ Failed**: One or both queries failed to execute (SQL errors, connection issues, etc.)
- **🔴 Row Count Mismatch**: Different number of rows returned (indicates data inconsistency)

## Performance Measurement

The validator uses multiple measurement techniques for accuracy:

1. **Warmup Runs**: Executes query once to warm up caches (default: 1 run)
2. **Measurement Runs**: Executes query multiple times (default: 3 runs)
3. **Median Calculation**: Uses median time to reduce variance from outliers

## Example Queries Included

All queries are **generic** and work on any Oracle/PostgreSQL database without requiring specific tables:

1. **01_list_tables_columns.sql**: Lists all tables and columns using information_schema
2. **02_count_tables.sql**: Counts total number of tables in the schema
3. **03_list_indexes.sql**: Lists all indexes and their columns
4. **04_list_constraints.sql**: Lists all constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
5. **05_table_statistics.sql**: Retrieves table statistics (row counts, sizes)
6. **06_list_sequences.sql**: Lists all sequences in the schema
7. **07_list_views.sql**: Lists all views in the schema
8. **08_table_column_summary.sql**: Complex query with JOINs and aggregates (tables with column counts)

## Adding Custom Queries

To add your own query pairs:

1. Create a `.sql` file in `queries/oracle/` (e.g., `09_my_query.sql`)
2. Create a matching `.sql` file in `queries/postgres/` with the **exact same name**
3. Use `{ORACLE_SCHEMA}` and `{POSTGRES_SCHEMA}` placeholders for schema references
4. Ensure both queries return equivalent data (same structure and row count expected)
5. Run the validator - your new query pair will be automatically detected and executed

**Tips for custom queries:**
- For generic metadata queries, use information_schema or system catalogs
- For database-specific queries (requiring specific tables), ensure those tables exist in both databases
- Test each query individually before pairing them
- Consider performance: complex queries may show natural variance between databases

## Troubleshooting

**No query pairs found:**
- Ensure `.sql` files exist in both `queries/oracle/` and `queries/postgres/`
- Check that file names match exactly

**Connection errors:**
- Verify database connection settings in `.env`
- Test connections with individual validators first

**Row count mismatch:**
- Some row count mismatches are expected (e.g., constraints and statistics may differ between databases)
- For metadata queries, check that both databases have similar schema structures
- For data queries, ensure test data is identical in both databases
- Verify schema names are correctly replaced from .env file

**Performance warnings:**
- Review query execution plans
- Check for missing indexes
- Consider database-specific optimizations
