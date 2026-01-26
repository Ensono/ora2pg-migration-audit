# Ora2PgSchemaComparer

## Overview

**Database-agnostic schema comparison tool** for validating Oracle to PostgreSQL database migrations. Compares schema objects between any Oracle and PostgreSQL schemas to ensure migration completeness and accuracy.

## Purpose

`Ora2PgSchemaComparer` validates that the **database schema objects** (tables, constraints, indexes, views, procedures, etc.) migrated correctly.

## Schema Comparison Coverage

### ✅ 1. Tables & Structure
- Table existence and count matching
- Column count, names, and order
- Data type mapping (Oracle → PostgreSQL)
- Table comments/metadata
- Partitioning strategy validation

### ✅ 2. Constraints (The Rules)
- **Primary Keys (PK)**: Existence per table
- **Foreign Keys (FK)**: Relationships + ON DELETE/UPDATE rules
- **Unique Constraints**: Column uniqueness enforcement
- **Check Constraints**: Logic rules (e.g., `age > 0`)
- **Not Null Constraints**: Mandatory field enforcement

### ✅ 3. Indexes & Performance
- **Standard B-Tree Indexes**: Column matching
- **Composite Indexes**: Multi-column order verification
- **Unique Indexes**: Uniqueness enforcement
- **Bitmap → GIN Conversion**: Oracle special case
- **Function-Based Indexes**: Expression indexes (e.g., `UPPER(lastname)`)

### ✅ 4. Database Code & Logic
- **Views**: Query logic comparison
- **Materialized Views**: Existence + refresh logic
- **Sequences**: Current value + increment settings
- **Triggers**: Event matching (BEFORE/AFTER INSERT/UPDATE)
- **Stored Procedures/Functions**: PL/SQL → PL/pgSQL conversion
- **Synonyms**: Oracle → PostgreSQL search_path/views

## Architecture

```
Ora2PgSchemaComparer/
├── src/
│   ├── Config/              # Configuration (reused from Ora2PgDataValidator)
│   ├── Connection/          # DB connections (reused from Ora2PgDataValidator)
│   ├── Extractor/           # Schema metadata extraction
│   │   ├── TableDefinition.cs
│   │   ├── ColumnDefinition.cs
│   │   ├── ConstraintDefinition.cs
│   │   ├── IndexDefinition.cs
│   │   ├── ViewDefinition.cs
│   │   ├── SequenceDefinition.cs
│   │   ├── TriggerDefinition.cs
│   │   ├── ProcedureDefinition.cs
│   │   ├── OracleSchemaExtractor.cs
│   │   └── PostgresSchemaExtractor.cs
│   ├── Comparison/          # Schema comparison logic
│   │   ├── TableComparator.cs
│   │   ├── ConstraintComparator.cs
│   │   ├── IndexComparator.cs
│   │   ├── CodeObjectComparator.cs
│   │   └── SchemaComparisonResult.cs
│   └── Report/              # Comparison reporting
│       └── SchemaComparisonReportWriter.cs
├── logs/                    # Application logs
└── reports/                 # Comparison reports
```

## Prerequisites

- .NET 8.0 SDK or higher
- Access to Oracle Database (source schema)
- Access to PostgreSQL Database (target schema)

## Setup

### 1. Configure Database Connections

The `.env` configuration file is located at the **solution root** (`ora2pg-migration-audit/.env`) and is shared by all validators.

```bash
# Navigate to solution root
cd ora2pg-migration-audit

# Copy the example configuration (if not already done)
cp .env.example .env

# Edit with your database credentials
nano .env  # or use your preferred editor
```

**Configuration Example:**
```bash
# Oracle Connection
ORACLE_HOST=your-oracle-host.example.com
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=system
ORACLE_PASSWORD=your_oracle_password
ORACLE_SCHEMA=CHINOOK

# PostgreSQL Connection
POSTGRES_HOST=your-postgres-host.example.com
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_SCHEMA=chinook

# Column Filtering (Optional - skip specific columns during comparison)
# Useful when PostgreSQL has additional audit columns not in Oracle
POSTGRES_SKIP_COLUMNS=created_at,updated_at,migration_id,synced_at
ORACLE_SKIP_COLUMNS=old_status,legacy_field
```

### Column Filtering Feature

When migrating from Oracle to PostgreSQL, you may have **additional columns** in PostgreSQL (or Oracle) that don't exist in the source database. Common scenarios:

**Scenario 1: PostgreSQL Has Audit Columns**
```bash
# Oracle: EMPLOYEES table (10 columns)
# PostgreSQL: employees table (12 columns - includes created_at, updated_by)

POSTGRES_SKIP_COLUMNS=created_at,updated_by,synced_at
```

**Scenario 2: Oracle Has Deprecated Columns**
```bash
# Oracle: CUSTOMERS (15 columns - includes old_status, legacy_field)
# PostgreSQL: customers (13 columns - deprecated columns not migrated)

ORACLE_SKIP_COLUMNS=old_status,legacy_field
```

**Scenario 3: Both Have Unique Columns**
```bash
ORACLE_SKIP_COLUMNS=oracle_specific_id
POSTGRES_SKIP_COLUMNS=created_at,updated_at,pg_migration_flag
```

**How It Works:**
- Columns are filtered **during schema extraction** from each database
- Column names are **case-insensitive** (works with Oracle UPPERCASE and PostgreSQL lowercase)
- Only the remaining columns are compared for structure, order, and data types
- Filtered columns appear in logs: `"Skipped N column(s) in [schema].[table]"`

**Benefits:**
- ✅ Accurate schema comparison focused on business data
- ✅ Handle audit columns gracefully (created_at, updated_at, etc.)
- ✅ Support for different schema evolution strategies
- ✅ Flexible configuration per database
- ✅ No code changes needed - pure configuration

```

**Note**: The solution automatically detects and loads the `.env` file from the solution root, regardless of which directory you run the tool from.

## Running the Tool

### Option 1: From Solution Root (Recommended)
```bash
cd ora2pg-migration-audit
dotnet run --project Ora2PgSchemaComparer/Ora2PgSchemaComparer.csproj
```

### Option 2: From Project Directory
```bash
cd ora2pg-migration-audit/Ora2PgSchemaComparer
dotnet run
```

### Option 3: Using Specific .NET Version
```bash
dotnet run --framework net8.0
```

Both options will automatically load the shared `.env` from the solution root.

## Output

### Console Output
The tool displays a comprehensive comparison report in the console:
- Schema summary (table/column/constraint counts)
- Detailed comparison by category (Tables, Constraints, Indexes, Code Objects)
- Issues found with severity indicators (✓ ⚠️ ❌)
- Final grade and recommendations

### Log Files
Detailed logs are saved to:
```
Ora2PgSchemaComparer/logs/application-YYYY-MM-DD.log
```

### Comparison Reports
Full comparison reports are saved to:
```
Ora2PgSchemaComparer/reports/schema-comparison-YYYYMMDD-HHMMSS.txt
```

### Exit Codes
- `0` - Success (schema migration complete and accurate)
- `1` - Critical issues found (migration needs attention)

## Actual Output (Tested with Chinook)

```
================================================================================
ORACLE TO POSTGRESQL SCHEMA COMPARISON
================================================================================
Source (Oracle):      CHINOOK schema
Target (PostgreSQL):  chinook schema
Comparison Date:      2025-12-08 12:03:17

SCHEMA SUMMARY
--------------------------------------------------------------------------------
  Tables:       Oracle=11   PostgreSQL=11 
  Columns:      Oracle=64   PostgreSQL=64 
  Primary Keys: Oracle=11   PostgreSQL=11 
  Foreign Keys: Oracle=11   PostgreSQL=11 

1. TABLES & STRUCTURE
--------------------------------------------------------------------------------
   ✓ Table Count:
     Oracle:     11 tables
     PostgreSQL: 11 tables

   ✓ Column Count:
     Oracle:     64 columns
     PostgreSQL: 64 columns

2. CONSTRAINTS
--------------------------------------------------------------------------------
   ✓ Primary Keys:
     Oracle:     11 primary keys
     PostgreSQL: 11 primary keys

   ✓ Foreign Keys:
     Oracle:     11 foreign keys
     PostgreSQL: 11 foreign keys

   ✓ Unique Constraints:
     Oracle:     0 unique constraints
     PostgreSQL: 0 unique constraints

   ⚠️ Check Constraints:
     Oracle:     0 check constraints
     PostgreSQL: 30 check constraints

3. INDEXES
--------------------------------------------------------------------------------
   ✓ Index Count:
     Oracle:     0 indexes
     PostgreSQL: 0 indexes

4. CODE OBJECTS (Views, Sequences, Triggers, Procedures)
--------------------------------------------------------------------------------
   ✓ Sequences:
     Oracle:     0 sequences
     PostgreSQL: 0 sequences

   ✓ Views:
     Oracle:     0 views
     PostgreSQL: 0 views

   ✓ Materialized Views:
     Oracle:     0 materialized views
     PostgreSQL: 0 materialized views

   ✓ Triggers:
     Oracle:     0 triggers
     PostgreSQL: 0 triggers

   ✓ Procedures/Functions:
     Oracle:     0 procedures/functions
     PostgreSQL: 0 procedures/functions

================================================================================
COMPARISON SUMMARY
================================================================================
Total Issues Found:    0
Critical Issues:       NO ✓
Migration Quality:     A+

✓ Schema migration is complete and accurate.
================================================================================
```

## Requirements

- .NET 8.0 SDK or higher
- Oracle Database access (11g+)
- PostgreSQL Database access (9.6+)
- Shared library: Ora2Pg.Common

## Troubleshooting

### .env File Not Found
If you see warnings about `.env` file not found:
```
⚠ .env file not found - checked solution root, project directory, and parent directories
```

**Solution:**
1. Ensure `.env` exists at solution root: `ora2pg-migration-audit/.env`
2. Copy from template if needed: `cp .env.example .env`
3. Check file permissions (must be readable)

### Database Connection Errors

**Oracle Connection Issues:**
```
ORA-12154: TNS:could not resolve the connect identifier specified
```
- Verify `ORACLE_SERVICE` name (e.g., `ORCL`, `XE`, `FREEPDB1`)
- Check Oracle listener is running: `lsnrctl status`
- Verify host and port are accessible

**PostgreSQL Connection Issues:**
```
FATAL:  password authentication failed for user "postgres"
```
- Verify PostgreSQL credentials in `.env`
- Check PostgreSQL is running: `pg_ctl status`
- Verify host and port are accessible

### Schema Not Found

If the tool reports 0 tables:
- Verify `ORACLE_SCHEMA` and `POSTGRES_SCHEMA` match actual schema names
- Oracle schema names are typically UPPERCASE
- PostgreSQL schema names are typically lowercase
- Check user has SELECT privileges on both schemas

## Related Tools

- **[Ora2PgDataValidator](../Ora2PgDataValidator/README.md)** - Validates data migration accuracy
- **[Ora2Pg.Common](../Ora2Pg.Common/README.md)** - Shared library for configuration and connections
