# Ora2PgDataTypeValidator - Universal Data Type Validation Tool

> ✅ **COMPLETE** - Database-agnostic data type validation for Oracle to PostgreSQL migrations.

**Validates Oracle to PostgreSQL data type mappings** to prevent data loss, overflow, precision issues, and compatibility problems.

---

## Overview

This tool validates that Oracle data types have been correctly mapped to PostgreSQL equivalents during migration. It implements comprehensive validation checks based on Oracle to PostgreSQL migration best practices.

---

## Key Features

### Numeric Type Validations
- ✅ **NUMBER(p,0) → INTEGER/BIGINT**: Prevents overflow for values > 2 billion
- ✅ **NUMBER(p,s) → NUMERIC(p,s)**: Ensures precision and scale match exactly (critical for money fields)
- ✅ **FLOAT/BINARY_DOUBLE → DOUBLE PRECISION**: Checks rounding error risks
- ✅ **BINARY_FLOAT → REAL**: Validates 4-byte floating point mappings
- ✅ **IDENTITY/SEQUENCE**: Verifies auto-increment behavior

### String Type Validations
- ✅ **VARCHAR2(n) → VARCHAR(n)**: Validates character vs byte length semantics
- ✅ **NVARCHAR2(n) → VARCHAR(n)**: National character set (UTF-8) handling
- ✅ **CHAR(n) → CHAR(n)**: Checks padding behavior differences
- ✅ **NCHAR(n) → CHAR(n)**: National fixed-length character validation
- ✅ **CLOB → TEXT**: Prevents truncation for large text blocks
- ✅ **NCLOB → TEXT**: National large object validation
- ✅ **LONG → TEXT**: Legacy large text type (deprecated in Oracle)
- ✅ **Empty String Handling**: Critical validation for Oracle's `'' = NULL` vs PostgreSQL's `'' ≠ NULL`

### Date/Time Type Validations
- ✅ **DATE → TIMESTAMP**: Warns about time component loss if mapped to DATE
- ✅ **TIMESTAMP → TIMESTAMP**: Validates timestamp precision
- ✅ **TIMESTAMP WITH TIME ZONE → TIMESTAMPTZ**: Checks UTC conversion handling
- ✅ **INTERVAL YEAR TO MONTH → INTERVAL**: Date/time duration validation
- ✅ **INTERVAL DAY TO SECOND → INTERVAL**: Time span validation

### Binary Type Validations
- ✅ **BLOB → BYTEA**: Validates binary file integrity (PDFs, images)
- ✅ **RAW(n) → BYTEA**: Fixed-length binary data
- ✅ **LONG RAW → BYTEA**: Legacy binary type (deprecated in Oracle)

### Boolean Type Validations
- ✅ **NUMBER(1)/CHAR(1) → BOOLEAN**: Checks 0/1 or Y/N conversion logic

### Advanced Oracle Type Validations
- ✅ **XMLTYPE → XML/TEXT**: Native XML storage with XPath/XQuery support
- ✅ **JSON → JSONB/JSON**: Native JSON storage (Oracle 12c+)
- ✅ **ROWID/UROWID → VARCHAR**: Oracle internal row identifiers
- ✅ **BFILE → N/A**: External file pointer (requires migration strategy)
- ✅ **SDO_GEOMETRY → GEOMETRY/GEOGRAPHY**: Oracle Spatial (requires PostGIS)
- ✅ **User-Defined Types (UDTs)**: Custom object types requiring manual conversion

---

## Report Formats

The tool generates **two report formats** for maximum compatibility:

### 1. Markdown Report (.md)
- Rich formatting with tables and collapsible sections
- Ideal for GitHub/GitLab documentation
- Viewable in any markdown renderer
- Located in: `reports/datatype-validation-YYYYMMDD-HHmmss.md`

### 2. Plain Text Report (.txt)
- Simple ASCII formatting
- Compatible with all text editors and CI/CD logs
- Easy to parse with scripts
- Located in: `reports/datatype-validation-YYYYMMDD-HHmmss.txt`

---

## Prerequisites

- .NET 8.0 SDK
- Oracle database (source)
- PostgreSQL database (target)
- Schema names for your databases

---

## Quick Start

### 1. Configure Environment

Create a `.env` file in the solution root (`ora2pg-migration-audit/.env`):

```bash
# Oracle Connection
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=FREE
ORACLE_USER=YOUR_ORACLE_USER
ORACLE_PASSWORD=your_password
ORACLE_SCHEMA=YOUR_ORACLE_SCHEMA  # e.g., HR, SALES

# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=your_postgres_schema  # e.g., hr, sales
POSTGRES_SKIP_COLUMNS=
ORACLE_SKIP_COLUMNS=
```

> 💡 **Schema Names**:
> - **Oracle schemas** are typically UPPERCASE (e.g., `HR`, `SALES`)
> - **PostgreSQL schemas** are typically lowercase (e.g., `hr`, `sales`)

> 💡 **Column Filtering**:
> - Use this feature when PostgreSQL has audit columns (e.g., `created_at`, `updated_at`) that weren't in Oracle
> - Or when Oracle has deprecated columns that weren't migrated to PostgreSQL
> - Only the common columns will be validated for data type compatibility

### 2. Build and Run

```bash
cd ora2pg-migration-audit/Ora2PgDataTypeValidator
dotnet build
dotnet run
```

### 3. Review Results

The tool generates two outputs:

1. **Console Report**: Real-time validation summary
2. **Markdown Report**: Detailed report in `reports/datatype-validation-YYYYMMDD-HHmmss.md`

---

## Validation Checks

The tool validates **50+ Oracle to PostgreSQL data type mappings**:

### Standard Data Types

| Oracle Type | PostgreSQL Type | Validation Check |
|-------------|-----------------|------------------|
| NUMBER(p,0) | INTEGER / BIGINT | Values > 2 billion use BIGINT, not INTEGER |
| NUMBER(p,s) | NUMERIC(p,s) | Precision and scale match exactly (money fields) |
| FLOAT | DOUBLE PRECISION | Check for rounding errors |
| BINARY_FLOAT | REAL | 4-byte floating point precision |
| BINARY_DOUBLE | DOUBLE PRECISION | 8-byte floating point precision |
| VARCHAR2(n) | VARCHAR(n) or TEXT | Length: Bytes vs Characters (multi-byte chars) |
| NVARCHAR2(n) | VARCHAR(n) or TEXT | National character set (UTF-8) support |
| CHAR(n) | CHAR(n) | Padding behavior (Oracle pads, PostgreSQL doesn't) |
| NCHAR(n) | CHAR(n) | National fixed-length character |
| CLOB | TEXT | No truncation for large text (>4000 chars) |
| NCLOB | TEXT | National large object |
| **Empty Strings** | **NULL vs Empty** | **CRITICAL**: Oracle `'' = NULL`, PostgreSQL `'' ≠ NULL` |
| DATE | TIMESTAMP or TIMESTAMPTZ | Oracle DATE has time; warn if PostgreSQL DATE loses it |
| TIMESTAMP | TIMESTAMP WITHOUT TIME ZONE | Verify no unexpected timezone offsets |
| TIMESTAMP WITH TIME ZONE | TIMESTAMPTZ | Verify UTC conversions |
| INTERVAL YEAR TO MONTH | INTERVAL | Date/time duration validation |
| INTERVAL DAY TO SECOND | INTERVAL | Time span validation |
| BLOB | BYTEA | Binary files (PDFs, images) integrity |
| RAW(n) | BYTEA | Fixed-length binary data |
| NUMBER(1) or CHAR(1) | BOOLEAN | 0/1 or Y/N conversion |
| IDENTITY / SEQUENCE | SERIAL / GENERATED ALWAYS | Auto-increment behavior |

### Legacy/Deprecated Types (Still in Production DBs)

| Oracle Type | PostgreSQL Type | Validation Check |
|-------------|-----------------|------------------|
| LONG | TEXT | Deprecated in Oracle, limited to 2GB |
| LONG RAW | BYTEA | Deprecated binary type, limited to 2GB |

### Advanced Oracle Types

| Oracle Type | PostgreSQL Type | Validation Check |
|-------------|-----------------|------------------|
| XMLTYPE | XML or TEXT | Native XML support (XPath/XQuery) vs plain text |
| JSON | JSONB or JSON | Oracle 12c+ JSON, recommend JSONB for performance |
| ROWID | VARCHAR(18) | Oracle internal row ID (no equivalent) |
| UROWID | VARCHAR(4000) | Universal ROWID (no equivalent) |
| BFILE | **N/A** | **CRITICAL**: External file pointer, requires migration strategy |
| SDO_GEOMETRY | GEOMETRY / GEOGRAPHY | **Requires PostGIS extension** for spatial data |
| User-Defined Types | Composite Types / JSON | **CRITICAL**: Requires manual conversion |

### Key Severity Levels

- ❌ **CRITICAL**: Data loss or migration failure risk (BFILE, UDTs, empty strings)
- 🔴 **ERROR**: Incorrect mapping that will cause data corruption (wrong type, missing precision)
- ⚠️ **WARNING**: Potential issues that may affect behavior (padding, byte vs char, rounding)
- ℹ️ **INFO**: Successful validation or optimization suggestions

---

## Sample Output

### Console Report
```
═══════════════════════════════════════════════════════════
  DATA TYPE VALIDATION REPORT
═══════════════════════════════════════════════════════════
Oracle Schema: HR
PostgreSQL Schema: hr
Validation Time: 2025-12-09 14:30:00
───────────────────────────────────────────────────────────
Status: ⚠️  WARNING
Columns Validated: 45
Critical Issues: 0 ❌
Errors: 0 🔴
Warnings: 3 ⚠️
Info: 5 ℹ️
═══════════════════════════════════════════════════════════
⚠️  VALIDATION PASSED WITH WARNINGS
```

### Markdown Report Excerpt
```markdown
## ⚠️ Warnings (3)

### EMPLOYEES.SALARY

**Category:** Precision/Scale Mismatch
**Mapping:** Oracle `NUMBER(10,2)` → PostgreSQL `NUMERIC(10,2)`

**Issue:** Precision/scale match verified. Money fields preserved correctly.

**Recommendation:** None required.

---
```

---

## Exit Codes

- **0**: Success (all validations passed)
- **1**: Critical issues or errors found
- **2**: Warnings found (migration may proceed with caution)

---

## Advanced Usage

### Validate Specific Tables

Currently validates all tables in the schema. Future enhancement: table filtering via environment variable.

### Custom Validation Rules

Extend `DataTypeValidator.cs` to add project-specific validation rules.

---

## Architecture

```
Ora2PgDataTypeValidator/
├── src/
│   ├── Models/
│   │   ├── ColumnMetadata.cs         # Database column metadata
│   │   ├── ValidationIssue.cs        # Validation issue model
│   │   └── ValidationResult.cs       # Validation results
│   ├── Extractors/
│   │   ├── OracleColumnExtractor.cs  # Extract Oracle columns
│   │   └── PostgresColumnExtractor.cs# Extract PostgreSQL columns
│   ├── Validators/
│   │   └── DataTypeValidator.cs      # Core validation logic
│   └── Reports/
│       └── ValidationReportWriter.cs # Report generation
├── Program.cs                        # Entry point
└── reports/                          # Generated reports
```

---

## Common Validation Issues

### 1. Numeric Overflow
**Issue**: NUMBER(12,0) → INTEGER  
**Risk**: Values > 2,147,483,647 will overflow  
**Fix**: Change to BIGINT

### 2. Money Field Precision
**Issue**: NUMBER(10,2) → NUMERIC(10,4)  
**Risk**: Incorrect calculations  
**Fix**: Match precision and scale exactly

### 3. Empty String Handling
**Issue**: Oracle `''` = NULL, PostgreSQL `''` ≠ NULL  
**Risk**: WHERE clauses behave differently  
**Fix**: Review ETL logic for empty string conversions

### 4. Time Data Loss
**Issue**: Oracle DATE → PostgreSQL DATE  
**Risk**: Time component (HH:MM:SS) lost  
**Fix**: Use TIMESTAMP if time is needed

---

## Troubleshooting

### "Schema not found"
- Verify Oracle schema name is UPPERCASE
- Verify PostgreSQL schema name is lowercase
- Check connection permissions

### "No columns found"
- Ensure tables exist in both databases
- Check that migration has run (tables created in PostgreSQL)
- Verify schema names are correct
