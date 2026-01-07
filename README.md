# Oracle to PostgreSQL Migration Audit

**Generic, database-agnostic tools for validating any Oracle to PostgreSQL migration.**

This solution provides production-ready validation tools that work with **any Oracle and PostgreSQL database** - not limited to specific schemas or sample databases.

## What's Included

This solution provides four complementary validation tools:

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

4. **Ora2PgDataValidator** - Validates actual data using cryptographic hash fingerprinting
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
```

**From Project Directory:**

```bash
# Navigate to the project
cd Ora2PgDataValidator  # or Ora2PgSchemaComparer or Ora2PgDataTypeValidator or Ora2PgRowCountValidator

# Build
dotnet build

# Run
dotnet run
```

### Running All Four Validators

For comprehensive migration validation, run all four tools in sequence:

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

# 4. Validate actual data integrity
cd Ora2PgDataValidator
dotnet run
cd ..
```

This gives you a complete validation covering:
- ✅ Schema structure (tables, PKs, FKs, indexes)
- ✅ Data type mappings (proper type conversions)
- ✅ Row counts (no missing or extra data)
- ✅ Data integrity (hash fingerprinting)
- ✅ Data type mappings (preventing overflow, precision loss)
- ✅ Data integrity (row counts, hash fingerprints)

## Projects

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

### Ora2PgDataValidator

Main application for validating Oracle to PostgreSQL data migrations using cryptographic hash fingerprinting.

**Key Features:**
- Extracts data from both Oracle and PostgreSQL databases
- Generates SHA256/MD5 hash fingerprints for each row
- Compares hashes to identify mismatches, missing rows, and extra rows
- Generates detailed validation reports

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
- Compares indexes (B-Tree, Unique, Function-based)
- Validates database code (Views, Materialized Views, Sequences, Triggers, Procedures)
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

## Requirements

- .NET 8.0 SDK or higher
- Oracle Database access
- PostgreSQL Database access


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