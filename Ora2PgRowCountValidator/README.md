# Ora2PgRowCountValidator

Row count verification tool for Oracle to PostgreSQL database migrations. Compares row counts table-by-table between source Oracle and target PostgreSQL databases to validate data migration completeness.

## Features

- **Automated Row Count Extraction**: Queries all tables in specified schemas
- **Intelligent Comparison**: Matches tables by name (case-insensitive)
- **Severity-Based Reporting**: Critical/Error/Warning/Info levels based on percentage differences
- **Dual Report Formats**: Both Markdown (.md) and plain text (.txt) reports
- **Missing Table Detection**: Identifies tables present in Oracle but not in PostgreSQL (critical)
- **Extra Table Detection**: Identifies tables present only in PostgreSQL (warning)
- **Error Handling**: Gracefully handles tables that can't be queried
- **Comprehensive Statistics**: Total tables, row counts, match/mismatch counts

## Prerequisites

- .NET 8.0 SDK or later
- Oracle database (11g or later)
- PostgreSQL database (10 or later)
- Network connectivity to both databases

## Configuration

Configuration is loaded from `.env` file in the solution root directory (`ora2pg-migration-audit/.env`):

```bash
# Oracle Configuration
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=FREE
ORACLE_USER=system
ORACLE_PASSWORD=your_password
ORACLE_SCHEMA_NAME=CHINOOK

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA_NAME=chinook
```

## Usage

### Build and Run

```bash
cd ora2pg-migration-audit/Ora2PgRowCountValidator
dotnet build
dotnet run
```

### Output

The tool generates three types of output:

1. **Console Output**: Real-time progress and summary
2. **Markdown Report**: `reports/rowcount-validation-YYYYMMDD-HHmmss.md`
3. **Text Report**: `reports/rowcount-validation-YYYYMMDD-HHmmss.txt`

### Sample Console Output

```
🚀 Oracle to PostgreSQL Row Count Validator
═══════════════════════════════════════════════════════════
✓ Configuration loaded from .env file
🔌 Testing database connections...
✓ Oracle: Connected to localhost:1521/FREE
✓ PostgreSQL: Connected to localhost:5432/postgres
═══════════════════════════════════════════════════════════
📊 Extracting row counts from Oracle schema 'CHINOOK'...
✓ Found 11 tables in Oracle (Total: 15,607 rows)
📊 Extracting row counts from PostgreSQL schema 'chinook'...
✓ Found 11 tables in PostgreSQL (Total: 15,607 rows)
═══════════════════════════════════════════════════════════
🔍 Comparing row counts...
✓ Comparison completed in 2.34 seconds
  - Matches: 11
  - Mismatches: 0
  - Only in Oracle: 0
  - Only in PostgreSQL: 0
═══════════════════════════════════════════════════════════
📝 Generating reports...
📄 Markdown report: reports/rowcount-validation-20250101-143022.md
📄 Text report: reports/rowcount-validation-20250101-143022.txt
═══════════════════════════════════════════════════════════
Status: ✅ PASSED
Total Tables: 11
✅ Matching: 11
❌ Mismatched: 0
⚠️  Only in Oracle: 0
ℹ️  Only in PostgreSQL: 0
═══════════════════════════════════════════════════════════
✅ VALIDATION PASSED - All row counts match!
Exit code: 0
```

## Severity Levels

The validator assigns severity levels based on row count differences:

| Severity | Condition | Exit Code |
|----------|-----------|-----------|
| **Critical** | Missing tables in PostgreSQL, >10% row count difference | 2 |
| **Error** | 1-10% row count difference, query failures | 1 |
| **Warning** | <1% row count difference, extra tables in PostgreSQL | 0 |
| **Info** | Exact row count match | 0 |

### Severity Logic

```csharp
// Missing tables
if (table exists in Oracle but not in PostgreSQL)
    → Critical: "Missing Table - data not migrated"

// Row count differences
percentageDifference = abs(postgresCount - oracleCount) / oracleCount * 100
if (percentageDifference > 10%)
    → Critical: "Large row count mismatch"
else if (percentageDifference > 1%)
    → Error: "Moderate row count mismatch"
else if (percentageDifference > 0%)
    → Warning: "Small row count mismatch"
else
    → Info: "Row counts match"

// Extra tables
if (table exists in PostgreSQL but not in Oracle)
    → Warning: "Extra table in PostgreSQL"
```

## Report Structure

### Markdown Report (.md)

```markdown
# Oracle to PostgreSQL Row Count Validation Report

## Summary
- Overall Status: PASSED/WARNING/FAILED
- Total Tables Validated: 11
- Tables with Matching Counts: 11
- Tables with Mismatched Counts: 0
- Tables Only in Oracle: 0
- Tables Only in PostgreSQL: 0
- Total Oracle Rows: 15,607
- Total PostgreSQL Rows: 15,607

## ❌ Critical Issues (0)
(Tables missing from PostgreSQL, large mismatches >10%)

## 🔴 Errors (0)
(Moderate mismatches 1-10%, query failures)

## ⚠️ Warnings (0)
(Small mismatches <1%, extra tables)

## ℹ️ Tables with Matching Counts (11)
- ALBUM: 347 rows
- ARTIST: 275 rows
- CUSTOMER: 59 rows
...

## Issues by Type
| Issue Type | Count | Critical | Errors | Warnings | Info |
|------------|-------|----------|--------|----------|------|
| Match      | 11    | 0        | 0      | 0        | 11   |
```

### Text Report (.txt)

Plain text format suitable for log files and email reports:

```
===============================================================================
  ORACLE TO POSTGRESQL ROW COUNT VALIDATION REPORT
===============================================================================

Generated:          2025-01-01 14:30:22
Oracle Schema:      CHINOOK
PostgreSQL Schema:  chinook

-------------------------------------------------------------------------------
  SUMMARY
-------------------------------------------------------------------------------
Overall Status:                 PASSED
Total Tables Validated:         11
Tables with Matching Counts:    11
Tables with Mismatched Counts:  0
Tables Only in Oracle:          0
Tables Only in PostgreSQL:      0

Total Oracle Rows:              15,607
Total PostgreSQL Rows:          15,607
Row Difference:                 0
```

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | All row counts match or only minor warnings |
| 1 | Errors | Moderate mismatches (1-10%) or query failures |
| 2 | Critical | Missing tables or large mismatches (>10%) |
| 99 | Fatal | Application error (connection failure, etc.) |

## Use Cases

### Post-Migration Validation
Run immediately after data migration to verify row counts:
```bash
dotnet run
# Check exit code: 0 = success, 1 = investigate errors, 2 = migration incomplete
echo $?
```

### CI/CD Integration
```bash
#!/bin/bash
cd Ora2PgRowCountValidator
dotnet run
EXIT_CODE=$?

if [ $EXIT_CODE -eq 2 ]; then
    echo "CRITICAL: Row count validation failed - missing data!"
    exit 1
elif [ $EXIT_CODE -eq 1 ]; then
    echo "ERROR: Row count mismatches detected - review reports"
    exit 1
else
    echo "SUCCESS: All row counts validated"
fi
```

## Troubleshooting

### Connection Failures
```
❌ Oracle: ORA-12154: TNS:could not resolve the connect identifier specified
```
**Solution**: Verify `ORACLE_SERVICE_NAME` in .env file matches Oracle configuration.

### Schema Not Found
```
✓ Found 0 tables in Oracle
```
**Solution**: Verify `ORACLE_SCHEMA_NAME` is correct (Oracle uses uppercase by default).

### Permission Denied
```
ORA-00942: table or view does not exist
```
**Solution**: Grant SELECT privileges on all_tables and target schema tables to Oracle user.

### Query Timeout
```
⚠️ Failed to query table LARGE_TABLE: Timeout expired
```
**Solution**: Table will be marked as -1 rows (error state) and reported as critical issue.

## Architecture

```
Program.cs
  ├─ Load configuration (.env)
  ├─ Test database connections
  ├─ Extract row counts
  │   ├─ OracleRowCountExtractor
  │   │   └─ SELECT COUNT(*) FROM {schema}.{table}
  │   └─ PostgresRowCountExtractor
  │       └─ SELECT COUNT(*) FROM {schema}.{table}
  ├─ Compare row counts
  │   └─ RowCountComparer
  │       ├─ Match tables (case-insensitive)
  │       ├─ Identify missing/extra tables
  │       ├─ Calculate percentage differences
  │       └─ Assign severity levels
  └─ Generate reports
      └─ ValidationReportWriter
          ├─ Markdown report (.md)
          ├─ Text report (.txt)
          └─ Console summary
```

## Error Handling

- **Query Failures**: Tables that can't be queried are marked with -1 row count and reported as errors
- **Missing Tables**: Tables in Oracle but not PostgreSQL are flagged as critical issues
- **Connection Failures**: Application exits with code 99 and detailed error message
- **Schema Issues**: Empty schemas (0 tables) are reported but don't cause failures

## Performance

- **Small databases (<100 tables)**: ~1-2 seconds
- **Medium databases (100-1000 tables)**: ~10-30 seconds
- **Large databases (>1000 tables)**: ~1-5 minutes

Performance depends on:
- Network latency to databases
- Table sizes (COUNT(*) performance)
- Database load and query optimization

## Best Practices

1. **Run After Migration**: Execute immediately after data migration completes
2. **Review Critical Issues**: Always investigate missing tables and large mismatches (>10%)
3. **Monitor Warnings**: Small differences (<1%) may indicate ongoing transactions or timing differences
4. **Archive Reports**: Save timestamped reports for audit trail
5. **Automate**: Integrate into CI/CD pipeline for continuous validation
