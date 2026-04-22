# Oracle to PostgreSQL Migration Audit

**Generic, database-agnostic tools for validating any Oracle to PostgreSQL migration.**

This solution provides production-ready validation tools that work with **any Oracle and PostgreSQL database** - not limited to specific schemas or sample databases.

## What's Included

This solution provides five complementary validation tools:

1. **Ora2PgSchemaComparer** - Validates database structure (tables, constraints, indexes, code objects)
    - Works with any Oracle and PostgreSQL schemas
    - Configurable via environment variables
    - No hardcoded database assumptions

2. **Ora2PgDataTypeValidator** - Validates Oracle to PostgreSQL data type mappings
    - Comprehensive coverage of 50+ Oracle data types (standard, legacy, advanced, spatial)
    - Prevents data loss, overflow, and precision issues
    - Validates all numeric, string, date/time, binary, and boolean type conversions
    - Advanced type support: XMLTYPE, JSON, ROWID, BFILE, SDO_GEOMETRY, User-Defined Types
    - Critical validation for empty string handling (Oracle '' = NULL vs PostgreSQL)
    - Dual report formats: Markdown (.md) and Plain Text (.txt)

3. **Ora2PgRowCountValidator** - Validates row counts table-by-table
    - Compares row counts between Oracle and PostgreSQL for all tables
    - Severity-based reporting (Critical/Error/Warning/Info)
    - Detects missing tables, extra tables, and row count mismatches
    - Percentage-based thresholds for intelligent issue classification
    - Dual report formats: Markdown (.md) and Plain Text (.txt)

4. **Ora2PgPerformanceValidator** - Validates and compares query performance
    - Executes paired SQL queries against both databases
    - Measures execution time with warmup and median timing
    - Configurable performance threshold for warnings
    - Generic schema parameters for portability
    - Includes 8 generic metadata queries (tables, indexes, constraints, views, sequences, etc.)
    - Triple report formats: Markdown (.md), HTML (.html), and Plain Text (.txt)

5. **Ora2PgDataValidator** - Validates actual data using cryptographic hash fingerprinting
    - Auto-discovers tables or uses explicit mappings
    - Supports any schema structure
    - Handles tables of any size

All tools share a common library (`Ora2Pg.Common`) and configuration (`.env` file at solution root).

> 💡 **Note**: While examples in documentation use the Chinook sample database for demonstration purposes, the tools themselves are fully generic and work with any Oracle and PostgreSQL databases.


## Solution Structure

```
ora2pg-migration-audit/
├── Ora2PgDataValidator.sln          # Visual Studio solution file
├── .env                             # 🔐 Shared configuration (gitignored)
├── .env.example                     # Configuration template
├── Ora2Pg.Common/                   # 🆕 Shared library for all validators
│   ├── Ora2Pg.Common.csproj        # Shared library project
│   ├── Config/                      # Configuration management
│   │   ├── ApplicationProperties.cs # .env loading with FindSolutionRoot()
│   │   └── DatabaseConfig.cs        # Oracle/PostgreSQL config
│   ├── Connection/                  # Database connections
│   │   ├── DatabaseType.cs          # Oracle/PostgreSQL enum
│   │   └── DatabaseConnectionManager.cs # Connection pooling
│   └── Util/                        # Shared utilities
│       └── CaseConverter.cs         # Schema/table name normalization
├── Ora2PgDataValidator/             # Data validation application
│   ├── Ora2PgDataValidator.csproj  # Project file (references Ora2Pg.Common)
│   ├── .env.example                 # Configuration reference
│   ├── README.md                    # Detailed documentation
│   └── src/                         # Source code
│       ├── Extractor/               # Data extraction
│       ├── Hasher/                  # Hash fingerprinting
│       ├── Comparison/              # Data comparison
│       ├── Report/                  # Report generation
│       └── Processor/               # Orchestration
├── Ora2PgSchemaComparer/            # Schema comparison tool
│   ├── Ora2PgSchemaComparer.csproj # Project file (references Ora2Pg.Common)
│   ├── README.md                    # Project documentation
│   ├── IMPLEMENTATION-SUMMARY.md    # Implementation details
│   └── src/                         # Source code
│       ├── Model/                   # Schema definition models
│       ├── Extractor/               # Oracle/PostgreSQL extractors
│       ├── Comparison/              # Schema comparison logic
│       └── Report/                  # Schema comparison reports
├── Ora2PgDataTypeValidator/         # 🆕 Data type validation tool
│   ├── Ora2PgDataTypeValidator.csproj # Project file (references Ora2Pg.Common)
│   ├── README.md                    # Comprehensive validation guide
│   └── src/                         # Source code
│       ├── Models/                  # Column metadata & validation models
│       ├── Extractors/              # Oracle/PostgreSQL column extractors
│       ├── Validators/              # Data type validation logic
│       └── Reports/                 # Validation report generation
├── Ora2PgRowCountValidator/         # 🆕 Row count validation tool
│   ├── Ora2PgRowCountValidator.csproj # Project file (references Ora2Pg.Common)
│   ├── README.md                    # Row count validation guide
│   └── src/                         # Source code
│       ├── Models/                  # Row count models
│       ├── Extractors/              # Oracle/PostgreSQL row count extractors
│       ├── Comparison/              # Row count comparison logic
│       └── Reports/                 # Validation report generation
├── Ora2PgPerformanceValidator/      # 🆕 Performance validation tool
│   ├── Ora2PgPerformanceValidator.csproj # Project file (references Ora2Pg.Common)
│   ├── README.md                    # Performance validation guide
│   ├── queries/                     # SQL query pairs
│   │   ├── oracle/                  # Oracle query files
│   │   └── postgres/                # PostgreSQL query files
│   └── src/                         # Source code
│       ├── Models.cs                # Performance result models
│       ├── QueryExecutor.cs         # Query execution with timing
│       ├── QueryLoader.cs           # Query file loader with parameter substitution
│       └── PerformanceReportWriter.cs # Report generation (MD/HTML/TXT)
└── tests/                           # Test projects
```

## Quick Start

### 1. Configure Environment

```bash
# Navigate to the solution root directory
cd ora2pg-migration-audit

# Copy the shared configuration template
cp .env.example .env

# Edit .env with your database credentials
# (Use your preferred editor: nano, vim, code, etc.)
nano .env
```

**Note**: The `.env` file is now located at the **solution root** and is shared by all validators. This provides a single source of truth for database connection configuration.

### Table/Object Filtering (All Validators)

You can exclude tables and other schema objects from validation runs using two optional settings:

```dotenv
# Exclude tables if their name contains any of these substrings
TABLE_EXCLUSION_PATTERNS=_bk,_bkup,_tmp

# Exclude specific objects by type (comma-separated key=value pairs)
IGNORED_OBJECTS=table=ignored_table,procedure=ignored_sproc,foreignkey=FK_LEGACY
```

**Supported ignored object keys:**
`table`, `view`, `materialized_view`, `sequence`, `trigger`, `index`, `procedure`, `function`, `package`, `foreignkey` (or `fk`)

**Foreign Key Exclusions (Schema Comparer):**
- Use `foreignkey=` or `fk=` to exclude specific foreign key constraints
- Foreign key issues are reported as **⚠️ WARNINGS** (not critical), allowing migrations to proceed
- Useful for:
  - Known FK differences between environments
  - FKs that will be added post-migration
  - Legacy FKs that are intentionally different

**Examples:**
```dotenv
# Exclude specific foreign keys
IGNORED_OBJECTS=foreignkey=FK_USER_ROLE,fk=FK_DEPT_MGR

# Combined exclusions
IGNORED_OBJECTS=table=AUDIT_LOG;procedure=SP_OLD;foreignkey=FK_LEGACY,FK_DEPRECATED
```

### Multi-Schema Validation (All Validators)

All validators support validating **multiple Oracle schemas** in a single run. This is useful when your migration includes several schemas that need to be validated together.

**Configuration:**

Specify multiple schemas as comma-separated values:

```dotenv
# Single schema mode
ORACLE_SCHEMA=MYSCHEMA
POSTGRES_SCHEMA=myschema

# Multi-schema mode (comma-separated, matching order)
ORACLE_SCHEMA=OSBINVS,OSBBATCH,OSBUSG,OSBMETER
POSTGRES_SCHEMA=osbinvs,osbbatch,osbusg,osbmeter

# Auto-discover all tables in each schema
TABLES_TO_COMPARE=ALL
```

**Requirements:**
- Oracle and PostgreSQL schema lists must have the **same number of entries**
- Schemas are paired by position (first Oracle schema → first PostgreSQL schema, etc.)
- Schema names are case-insensitive

**Report Output:**

Each schema generates its own set of reports with the schema name as a prefix:

```
reports/
├── osbinvs-schema-comparison-20260324-120000.md
├── osbinvs-datatype-validation-20260324-120000.md
├── osbinvs-rowcount-validation-20260324-120000.md
├── osbinvs-performance-validation-20260324-120000.md
├── osbinvs-data-fingerprint-validation-20260324-120000.md
├── osbbatch-schema-comparison-20260324-120000.md
├── osbbatch-datatype-validation-20260324-120000.md
...
```

**Summary Reports:**

When running in multi-schema mode, validators also generate **summary reports** that aggregate results across all schemas:

```
reports/
├── mydb-summary-schema-comparison-20260324-120000.md
├── mydb-summary-datatype-validation-20260324-120000.md
├── mydb-summary-rowcount-validation-20260324-120000.md
├── mydb-summary-performance-validation-20260324-120000.md
├── mydb-summary-data-fingerprint-validation-20260324-120000.md
```

The database name prefix (e.g., `mydb`) comes from the `POSTGRES_DB` environment variable.

### Performance Optimization Settings

The validators support extensive performance tuning options for faster execution:

#### Parallel Processing

```dotenv
# Number of tables processed in parallel within each schema (default: 4)
# Affects: DataValidator, RowCountValidator, SchemaComparer
PARALLEL_TABLES=4

# Number of schemas processed in parallel in multi-schema mode (default: 1)
# Affects: DataValidator, RowCountValidator
# Set to 2+ for parallel schema processing (logs may interleave)
PARALLEL_SCHEMAS=1

# Total parallelism = PARALLEL_SCHEMAS × PARALLEL_TABLES
# Example: PARALLEL_SCHEMAS=2, PARALLEL_TABLES=4 = up to 8 concurrent operations
```

**Parallel processing matrix:**

| Validator | Parallel Tables | Parallel Schemas | Notes |
|-----------|-----------------|------------------|-------|
| DataValidator | ✅ | ✅ | Full parallel support |
| RowCountValidator | ✅ | ✅ | Full parallel support |
| SchemaComparer | ✅ (objects) | ❌ | Parallel object extraction |
| DataTypeValidator | N/A | ❌ | Single-query extraction |
| PerformanceValidator | ❌ | ❌ | Sequential for timing accuracy |

#### LOB (BLOB/CLOB) Data Optimization (Data Validator)

Processing tables with LOB columns (BLOB, CLOB) can be slow. Use these options to speed up validation:

```dotenv
# Skip LOB columns entirely (fastest option)
# Set to true if you only care about non-LOB data integrity
# Applies to: BLOB, CLOB, NCLOB (Oracle) and bytea, text (PostgreSQL)
SKIP_LOB_COLUMNS=false

# Limit bytes fetched from LOB columns at DATABASE LEVEL
# LOB columns are ALWAYS fetched via DBMS_LOB.SUBSTR (never SELECT *) to
# avoid ORA-22835 (buffer overflow when CLOB > 4000 bytes).
#
# Oracle type limits (enforced automatically):
#   BLOB -> RAW:      max 2000 bytes in SQL context
#   CLOB -> VARCHAR2: max 4000 bytes in SQL context
#
# Examples:
#   0     = Use default safe limits (BLOB: 2000, CLOB: 4000) - recommended
#   1024  = Fetch first 1KB only (faster, less accurate)
#   4000  = Maximum allowed (Oracle VARCHAR2 limit for CLOB)
#           Note: BLOB is still capped at 2000 even if you set 4000
LOB_SIZE_LIMIT=0
```

**Performance comparison:**

| Configuration | Speed | How it works |
|---------------|-------|--------------|
| `SKIP_LOB_COLUMNS=true` | ⚡ Fastest | LOBs excluded from SELECT query |
| `LOB_SIZE_LIMIT=1024` | 🚀 Very Fast | Only 1KB transferred per LOB |
| `LOB_SIZE_LIMIT=2000` | 🏃 Fast | 2KB (max for BLOB columns) |
| Default (0) | 🐢 Safe default | BLOB: 2000 bytes, CLOB: 4000 bytes |

> **⚠️ Important:** LOB columns always use `DBMS_LOB.SUBSTR` — never `SELECT *` — to avoid `ORA-22835` on CLOBs larger than 4000 bytes. Setting `LOB_SIZE_LIMIT > 4000` will cause the validator to fail immediately.

#### Query Timeout Settings

```dotenv
# Database command timeout in seconds (default: 600 = 10 minutes)
# Increase for very large tables or slow networks
COMMAND_TIMEOUT_SECONDS=600

# For row count validation with tables >100M rows, consider 1800 seconds (30 min)
```

#### Detailed Row Comparison (Row Count Validator)

When row counts differ between Oracle and PostgreSQL, the Row Count Validator can perform a detailed comparison to identify **which specific rows** are missing or extra, using bulk primary-key queries (2 queries per mismatched table).

```dotenv
# true  = Identify missing/extra rows by primary key (default, ~2 bulk queries per mismatch)
# false = Only report count differences — fastest option for large tables
DETAILED_ROW_COMPARISON=true
```

> **Tip:** Set `DETAILED_ROW_COMPARISON=false` when you only need to know *that* counts differ (not *which* rows), or when mismatched tables are very large and even bulk queries are slow.

#### Row Limiting

```dotenv
# Limit rows per table for faster testing (0 = unlimited)
MAX_ROWS_PER_TABLE=0

# Useful for initial validation runs on large databases
# Example: MAX_ROWS_PER_TABLE=10000 for quick sanity checks
```

### 2. Build and Run

**From Solution Root:**

```bash
# Build the entire solution
dotnet build

# Run the data validator
dotnet run --project Ora2PgDataValidator/Ora2PgDataValidator.csproj

# Or run the schema comparer
dotnet run --project Ora2PgSchemaComparer/Ora2PgSchemaComparer.csproj

# Or run the data type validator
dotnet run --project Ora2PgDataTypeValidator/Ora2PgDataTypeValidator.csproj

# Or run the row count validator
dotnet run --project Ora2PgRowCountValidator/Ora2PgRowCountValidator.csproj

# Or run the performance validator
dotnet run --project Ora2PgPerformanceValidator/Ora2PgPerformanceValidator.csproj
```

**From Project Directory:**

```bash
# Navigate to the project
cd Ora2PgDataValidator  # or Ora2PgSchemaComparer or Ora2PgDataTypeValidator or Ora2PgRowCountValidator or Ora2PgPerformanceValidator

# Build
dotnet build

# Run
dotnet run
```

### Running All Five Validators

For comprehensive migration validation, run all five tools in sequence:

```bash
cd ora2pg-migration-audit

# 1. Validate schema structure first
cd Ora2PgSchemaComparer
dotnet run
cd ..

# 2. Validate data type mappings
cd Ora2PgDataTypeValidator
dotnet run
cd ..

# 3. Validate row counts
cd Ora2PgRowCountValidator
dotnet run
cd ..

# 4. Validate query performance
cd Ora2PgPerformanceValidator
dotnet run
cd ..

# 5. Validate actual data integrity
cd Ora2PgDataValidator
dotnet run
cd ..
```

This gives you a complete validation covering:
- ✅ Schema structure (tables, PKs, FKs, indexes)
- ✅ Data type mappings (proper type conversions)
- ✅ Row counts (no missing or extra data)
- ✅ Query performance (execution time comparisons)
- ✅ Data integrity (hash fingerprinting)

## Automated Validation (PowerShell Script)

For convenience, a PowerShell script is provided to automate building and running all five validators in the recommended sequence.

### Script: `run-all-validators.ps1`

**Location:** Solution root directory

**Features:**
- ✅ Automatically builds the entire solution
- ✅ Runs all five validators in the optimal order
- ✅ Checks prerequisites (.env file, .NET SDK)
- ✅ Color-coded output with status indicators
- ✅ Execution timing for each validator
- ✅ Summary report with pass/fail counts
- ✅ Proper error handling and exit codes
- ✅ Optional roll-forward support for .NET version compatibility

### Usage

**Basic Usage (Standard Execution):**

```powershell
# From solution root
./run-all-validators.ps1
```

**With .NET Roll-Forward Policy:**

Use the `-RollForward` parameter when your system has a higher version of .NET installed than the project targets (e.g., .NET 9.x when project targets .NET 8.0).

```powershell
# Roll forward to next major version (e.g., .NET 8.x → .NET 9.x)
./run-all-validators.ps1 -RollForward Major

# Roll forward to next minor version (e.g., .NET 8.0 → .NET 8.1)
./run-all-validators.ps1 -RollForward Minor
```

**Platform-Specific Instructions:**

```bash
# Windows (PowerShell)
.\run-all-validators.ps1

# Windows (cmd.exe)
pwsh run-all-validators.ps1

# macOS/Linux (with PowerShell Core installed)
# Make executable (first time only)
chmod +x run-all-validators.ps1

# Run
./run-all-validators.ps1

# Or explicitly with pwsh
pwsh run-all-validators.ps1
```

### Execution Order

The script runs validators in this optimized sequence:

1. **SchemaComparer** → Validates schema structure (tables, columns, constraints, indexes)
2. **RowCountValidator** → Validates row counts match between databases
3. **DataTypeValidator** → Validates data type mappings are correct
4. **PerformanceValidator** → Validates query performance benchmarks
5. **DataValidator** → Validates data integrity using hash fingerprinting

### Output Example

```
╔═══════════════════════════════════════════════════════════════╗
║  Oracle to PostgreSQL Migration Validation Suite             ║
╚═══════════════════════════════════════════════════════════════╝

✓ Found .env configuration file
✓ .NET SDK version: 8.0.100

═══════════════════════════════════════════════════════════════
  Building Solution
═══════════════════════════════════════════════════════════════

✓ Solution built successfully

═══════════════════════════════════════════════════════════════
  Running SchemaComparer
═══════════════════════════════════════════════════════════════

✓ SchemaComparer completed successfully (12.34s)

═══════════════════════════════════════════════════════════════
  Validation Summary
═══════════════════════════════════════════════════════════════

Total execution time: 67.89s

✓ SchemaComparer              12.34s Success
✓ RowCountValidator           8.12s Success
✓ DataTypeValidator           5.67s Success
✓ PerformanceValidator        23.45s Success
✓ DataValidator               18.31s Success

Results: 5 passed, 0 failed

✓ All validators completed successfully!
```

### Prerequisites

- **PowerShell:** Windows PowerShell 5.1+ or PowerShell Core 7.0+
- **.NET SDK:** 8.0 or higher
- **Configuration:** `.env` file must be configured at solution root

### Exit Codes

- `0` - All validators completed successfully
- `1` - One or more validators failed or prerequisites not met

### Notes

- The script uses `--no-build` for individual project execution (only builds once at start)
- All output from validators is displayed in real-time
- Each validator runs sequentially (not in parallel) to ensure clear output and proper resource usage
- Reports are generated in each project's directory as configured

## Projects
cd ..
### Ora2Pg.Common (Shared Library)

**Purpose:** Centralized code library shared across all validators to eliminate duplication.

**Provides:**
- **Config:** Application properties, database configuration
- **Connection:** Database connection management, connection pooling
- **Util:** Common utilities (case conversion, name normalization)

**Benefits:**
- ✅ Single source of truth for database connectivity
- ✅ Consistent configuration across all validators
- ✅ Easier maintenance (fix once, benefits all)
- ✅ Reduced code duplication (~200 lines saved per validator)

**Referenced By:**
- Ora2PgDataValidator
- Ora2PgSchemaComparer
- Ora2PgDataTypeValidator
- Ora2PgRowCountValidator
- Ora2PgPerformanceValidator

### Ora2PgDataValidator

Main application for validating Oracle to PostgreSQL data migrations using cryptographic hash fingerprinting.

**Key Features:**
- Extracts data from both Oracle and PostgreSQL databases
- Generates SHA256/MD5 hash fingerprints for each row
- Compares hashes to identify mismatches, missing rows, and extra rows
- **Column filtering** - Skip additional columns via environment variables (e.g., audit columns in PostgreSQL)
- **BLOB optimization** - Skip or limit BLOB columns for faster processing
- **Parallel processing** - Process multiple tables and schemas concurrently
- Generates detailed validation reports (Markdown, HTML, Text)

**Dependencies:**
- References: Ora2Pg.Common
- NuGet: CsvHelper, Serilog sinks

📖 **Full Documentation:** See [Ora2PgDataValidator/README.md](Ora2PgDataValidator/README.md) for:
- Detailed architecture and design
- Complete configuration options
- Advanced usage examples
- Troubleshooting guide
- Project structure details

### Ora2PgSchemaComparer

Schema object comparison tool for Oracle to PostgreSQL migrations following the P2.1 checklist.

**Key Features:**
- Compares tables, columns, and structure (including partitioning)
- Validates constraints (PK, FK, Unique, Check, Not Null)
  - **Foreign key mismatches reported as ⚠️ WARNINGS** (not critical) to allow migrations to proceed
  - FKs can be excluded via `IGNORED_OBJECTS=foreignkey=FK_NAME` or `fk=FK_NAME`
- Compares indexes (B-Tree, Unique, Function-based)
- Validates database code (Views, Materialized Views, Sequences, Triggers, Procedures)
- **Parallel extraction**: Extracts schema objects (constraints, indexes, etc.) concurrently
- Generates P2.1 compliance checklist reports

**Dependencies:**
- References: Ora2Pg.Common
- NuGet: Oracle.ManagedDataAccess.Core, Npgsql, Serilog sinks

### Ora2PgDataTypeValidator

Data type mapping validation tool that prevents data loss, overflow, and precision issues during Oracle to PostgreSQL migrations. **Now with comprehensive coverage of 50+ Oracle data types** including legacy, advanced, and spatial types.

**Key Features:**
- ✅ **Numeric Type Validations**: NUMBER → INTEGER/BIGINT/NUMERIC, FLOAT, BINARY_FLOAT, BINARY_DOUBLE
- ✅ **String Type Validations**: VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG (legacy)
- ✅ **Date/Time Type Validations**: DATE, TIMESTAMP, TIMESTAMPTZ, INTERVAL types
- ✅ **Binary Type Validations**: BLOB, RAW, LONG RAW (legacy), BYTEA mappings
- ✅ **Boolean Type Validations**: NUMBER(1)/CHAR(1) → BOOLEAN conversions
- ✅ **Column filtering** - Skip additional columns via environment variables (e.g., audit columns in PostgreSQL)
- ✅ **Advanced Oracle Types**:
    - XMLTYPE → XML/TEXT (XPath/XQuery support)
    - JSON → JSONB/JSON (Oracle 12c+)
    - ROWID/UROWID → VARCHAR (internal row identifiers)
    - BFILE → External file pointer (requires migration strategy)
    - SDO_GEOMETRY → PostGIS GEOMETRY/GEOGRAPHY (spatial data)
    - User-Defined Types (UDTs) → Composite types or JSON
- ✅ **Critical Validation**: Empty string handling (Oracle `''` = NULL vs PostgreSQL `''` ≠ NULL)
- ✅ **Dual Report Formats**: Generates both Markdown (.md) and Plain Text (.txt) reports

**Report Outputs:**
- **Markdown Report**: Rich formatting with tables, ideal for GitHub/GitLab documentation
- **Plain Text Report**: ASCII formatting, compatible with all text editors and CI/CD logs
- **Console Report**: Real-time validation summary with emoji indicators

**Dependencies:**
- References: Ora2Pg.Common
- NuGet: Oracle.ManagedDataAccess.Core, Npgsql, DotNetEnv, Serilog.Sinks.Console

📖 **Full Documentation:** See [Ora2PgDataTypeValidator/README.md](Ora2PgDataTypeValidator/README.md) for comprehensive validation rules, data type mapping tables, and examples.

**Validation Severity Levels:**
- ❌ **Critical**: Data loss or migration failure risk (e.g., BFILE, UDTs, empty strings)
- 🔴 **Error**: Incorrect mappings causing data corruption (e.g., wrong type, missing precision)
- ⚠️ **Warning**: Potential behavior issues (e.g., padding, byte vs char, rounding)
- ℹ️ **Info**: Successful validation or optimization suggestions

**Supports:**
- Standard Oracle types (VARCHAR2, NUMBER, DATE, etc.)
- Legacy/deprecated types (LONG, LONG RAW)
- Advanced types (XMLTYPE, JSON, spatial)
- Production database edge cases

### Ora2PgRowCountValidator

Row count validation tool that compares table row counts between Oracle and PostgreSQL databases with intelligent severity classification.

**Key Features:**
- ✅ **Automated Discovery**: Finds all tables in both databases
- ✅ **Intelligent Comparison**: Configurable thresholds for warning vs error classification
- ✅ **Missing/Extra Detection**: Identifies tables that exist in only one database
- ✅ **Percentage-Based Analysis**: Calculates row count difference percentages
- ✅ **Parallel Processing**: Process multiple tables and schemas concurrently
- ✅ **Dual Report Formats**: Generates both Markdown (.md) and Plain Text (.txt) reports
- ✅ **Severity Levels**: Critical/Error/Warning/Info classification

**Dependencies:**
- References: Ora2Pg.Common
- NuGet: Oracle.ManagedDataAccess.Core, Npgsql, Serilog.Sinks.Console

📖 **Full Documentation:** See [Ora2PgRowCountValidator/README.md](Ora2PgRowCountValidator/README.md)

### Ora2PgPerformanceValidator

Query performance comparison tool that validates execution time between Oracle and PostgreSQL databases.

**Key Features:**
- ✅ **Paired Query Execution**: Matches SQL files by name from oracle/ and postgres/ directories
- ✅ **Accurate Timing**: Warmup runs + multiple measurements + median calculation
- ✅ **Configurable Threshold**: Set acceptable performance difference percentage (default: 50%)
- ✅ **Generic Parameters**: `{ORACLE_SCHEMA}` and `{POSTGRES_SCHEMA}` placeholders for portability
- ✅ **8 Generic Queries Included**: Metadata queries (tables, indexes, constraints, views, sequences, etc.)
- ✅ **Triple Report Formats**: Generates Markdown (.md), HTML (.html), and Plain Text (.txt) reports
- ✅ **Performance Status**: Passed/Warning/Failed/Row Count Mismatch classification

**Included Generic Queries:**
1. List all tables and columns
2. Count total tables
3. List all indexes
4. List all constraints
5. Table statistics (row counts, sizes)
6. List all sequences
7. List all views
8. Complex JOIN with aggregates

**Configuration:**
```properties
PERF_WARMUP_RUNS=1              # Warmup executions (default: 1)
PERF_MEASUREMENT_RUNS=3         # Measurement runs (default: 3)
PERF_THRESHOLD_PERCENT=50       # Performance difference threshold % (default: 50)
```

**Dependencies:**
- References: Ora2Pg.Common
- NuGet: Oracle.ManagedDataAccess.Core, Npgsql, Serilog.Sinks.Console

📖 **Full Documentation:** See [Ora2PgPerformanceValidator/README.md](Ora2PgPerformanceValidator/README.md) for comprehensive configuration, query examples, and adding custom queries.

## Requirements

- .NET 8.0 SDK or higher
- Oracle Database access
- PostgreSQL Database access
- PowerShell 7.0+ (PowerShell Core) - Required for automated validation script (`run-all-validators.ps1`)
  - macOS/Linux: Install [PowerShell Core](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell)


## License

**This software is source-available but NOT open source.**

- ❌ No license is granted to use, copy, modify, merge, publish, distribute, or sublicense
- ℹ️ The source code is available for review and reference purposes only
- 📧 For a commercial license to use this software, contact: Ensono

See [LICENSE](LICENSE) for full terms.

### Why Source-Available?

This project is made available for transparency and review, but **running it requires a separate license** from Ensono. This approach:

- ✅ Allows code review and security audits
- ✅ Provides reference for migration patterns
- ✅ Maintains clear intellectual property ownership
- ❌ Does not permit use without explicit permission

### Need a License?

Contact Ensono for commercial licensing:
- Website: https://www.ensono.com/

## Contributing

**This project is NOT accepting external contributions.**

We do not accept pull requests, issues, or other contributions from external parties to avoid ownership ambiguity. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

For Ensono employees: Please follow internal development standards and submit PRs for code review.

---

**Copyright © 2026 Ensono. All rights reserved.**