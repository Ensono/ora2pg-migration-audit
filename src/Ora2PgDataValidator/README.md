# Ora2Pg Data Validator

A C# .NET 8.0-based tool for validating data integrity during Oracle to PostgreSQL database migrations using
cryptographic hash fingerprinting.

[![.NET](https://img.shields.io/badge/.NET-8.0-blue.svg)](https://dotnet.microsoft.com/download/dotnet/8.0)
[![C#](https://img.shields.io/badge/C%23-12-purple.svg)](https://docs.microsoft.com/en-us/dotnet/csharp/)

---

## 🎯 Project Overview

This C# application validates that data has been correctly migrated from **any Oracle database** to **any PostgreSQL
database** by:

1. **Auto-discovering or explicitly configuring tables** to compare
2. **Extracting data** from both Oracle and PostgreSQL databases
3. **Generating hash fingerprints** (SHA256/MD5) for each row of data
4. **Comparing hashes** to identify mismatches, missing rows, and extra rows
5. **Generating detailed reports** with statistics and discrepancies

> 💡 **Generic Tool**: Works with any schema structure—just configure your database connections and schema names in the
`.env` file. No hardcoded assumptions about table names or structure.

---


## 🚀 Quick Start

### 1. Prerequisites

- **.NET 8.0 SDK** or higher ([Download](https://dotnet.microsoft.com/download/dotnet/8.0))
- **Oracle Database** access
- **PostgreSQL Database** access
- Network connectivity to both databases

### 2. Clone and Build

```bash
# Navigate to the solution root directory
cd ora2pg-migration-audit

# Restore dependencies for all projects
dotnet restore

# Build the entire solution
dotnet build

# Or build in Release mode
dotnet build -c Release
```

### 3. Configure

The `.env` configuration file is located at the **solution root** and is shared by all validators.

```bash
# Navigate to solution root (if not already there)
cd ora2pg-migration-audit

# Copy the example configuration (if not already done)
cp .env.example .env

# Edit with your database credentials
nano .env  # or use your preferred editor
```

**Configuration Example:**

```bash
# Oracle Database
ORACLE_HOST=your-oracle-host.example.com
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
ORACLE_SCHEMA=SCHEMA_NAME

# PostgreSQL Database
POSTGRES_HOST=your-postgres-host.example.com
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_SCHEMA=schema_name

# Validation Configuration
TABLES_TO_COMPARE=ALL
HASH_ALGORITHM=SHA256
```

**Note**: The `.env` file is located at the **solution root** (`ora2pg-migration-audit/.env`), not in the project
directory. The application automatically detects and loads it from there.

### 4. Run

**Option 1: From Solution Root (Recommended)**

```bash
cd ora2pg-migration-audit
dotnet run --project Ora2PgDataValidator/Ora2PgDataValidator.csproj
```

**Option 2: From Project Directory**

```bash
cd ora2pg-migration-audit/Ora2PgDataValidator
dotnet run
```

**Option 3: Run in Release Mode**

```bash
dotnet run --project Ora2PgDataValidator/Ora2PgDataValidator.csproj -c Release
```

All options will automatically load the shared `.env` from the solution root.

# Or run the compiled executable

```bash
dotnet run --project Ora2PgDataValidator.csproj
```

# Run in Release mode

```bash
dotnet run -c Release
```

### 5. Review Results

Check the `reports/` folder for:

- CSV hash files (`*_oracle_hashes.csv`, `*_postgresql_hashes.csv`)
- Comparison report (`migration_comparison_report_*.txt`)

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                          │
│  • ApplicationProperties (DotNetEnv)                            │
│  • DatabaseConfig (Oracle/PostgreSQL)                           │
│  • appsettings.json + .env file                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│  ORACLE          │            │  POSTGRESQL      │
│  Connection      │            │  Connection      │
│  (Oracle.ManagedDataAccess)   │  (Npgsql)        │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│  Data Extraction │            │  Data Extraction │
│  (DataExtractor) │            │  (DataExtractor) │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│  Hash Generation │            │  Hash Generation │
│  (SHA256/MD5)    │            │  (SHA256/MD5)    │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│  CSV Output      │            │  CSV Output      │
│  (CsvHelper)     │            │  (CsvHelper)     │
└────────┬─────────┘            └────────┬─────────┘
         │                               │
         └───────────────┬───────────────┘
                         ▼
                ┌─────────────────┐
                │  Hash Comparison│
                │  (HashComparator)
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │  Report Generator│
                │  (ComparisonReport)
                └─────────────────┘
```

---

## 📋 Requirements

### Software Requirements

- **.NET SDK:** Version 8.0 or higher
- **Operating System:** Windows 10+, macOS 10.15+, Ubuntu 20.04+

### Database Requirements

- **Oracle Database:** 11g or higher
- **PostgreSQL:** 9.6 or higher

### NuGet Packages (Auto-Installed)

- **Oracle.ManagedDataAccess.Core** 23.5.1 - Oracle database driver
- **Npgsql** 8.0.4 - PostgreSQL database driver
- **DotNetEnv** 3.1.1 - Environment variable loading from .env files
- **CsvHelper** 33.0.1 - CSV file generation
- **Serilog** 4.0.2 - Structured logging framework
- **System.Text.Json** 8.0.5 - JSON serialization

---

## ⚙️ Configuration

### Configuration File: `.env`

The application uses environment variables for configuration. The `.env` file is located at the **solution root** (
`ora2pg-migration-audit/.env`) and is shared by all validators in the solution.

**Configuration Location:**

```
ora2pg-migration-audit/
├── .env                    ← Shared configuration file (gitignored)
├── .env.example           ← Template for configuration
├── Ora2Pg.Common/         ← Shared library (loads .env)
├── Ora2PgDataValidator/   ← Data validation tool
└── Ora2PgSchemaComparer/  ← Schema comparison tool
```

The shared library (`Ora2Pg.Common`) automatically detects and loads the `.env` file from the solution root using
intelligent search strategies:

1. **Solution root** (primary) - for shared configuration
2. **Project directory** (fallback) - for backward compatibility
3. **Parent directory** (debug scenarios) - when running from bin/Debug/net8.0

### Example Configuration

```bash
# ============================================================================
# ORACLE DATABASE CONFIGURATION
# ============================================================================
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=XEPDB1
ORACLE_USER=system
ORACLE_PASSWORD=your_oracle_password

# ============================================================================
# POSTGRESQL DATABASE CONFIGURATION
# ============================================================================
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password

# ============================================================================
# COMPARISON CONFIGURATION
# ============================================================================

# Extraction Mode:
#   false = Compare two databases (Oracle vs PostgreSQL)
#   true  = Extract from one database only
EXTRACT_SINGLE_DB=false

# Target Database (only used when EXTRACT_SINGLE_DB=true):
#   ORACLE or POSTGRESQL
TARGET_DATABASE=ORACLE

# Tables to Compare:
#   ALL - Auto-discover all common tables
#   Comma-separated list - chinook.artist,chinook.album,chinook.track
TABLES_TO_COMPARE=ALL

# Schemas for Auto-Discovery (required when TABLES_TO_COMPARE=ALL):
ORACLE_SCHEMA=CHINOOK
POSTGRES_SCHEMA=chinook

# ============================================================================
# PERFORMANCE SETTINGS
# ============================================================================

# Row Limiting (0 = unlimited, >0 = limit rows per table):
MAX_ROWS_PER_TABLE=0

# Batch Processing:
BATCH_SIZE=5000

# Hash Algorithm (SHA256 or MD5):
HASH_ALGORITHM=SHA256

# Save hash values to CSV files:
SAVE_HASHES_TO_CSV=true
```

---

## 💻 Usage Examples

### Example 1: Build and Run

```bash
# Build the project
dotnet build

# Run the application
dotnet run

# Or build and run in one command
dotnet run --project Ora2PgDataValidator.csproj
```

### Example 2: Build Release Version

```bash
# Build optimized release version
dotnet build -c Release

# Run the release build
dotnet run -c Release
```

### Example 3: Create Standalone Executable

```bash
# Publish self-contained executable (macOS)
dotnet publish -c Release -r osx-arm64 --self-contained

# Publish self-contained executable (Linux)
dotnet publish -c Release -r linux-x64 --self-contained

# Publish self-contained executable (Windows)
dotnet publish -c Release -r win-x64 --self-contained

# Run the standalone executable
./bin/Release/net8.0/osx-arm64/publish/Ora2PgDataValidator
```

### Example 4: Run with Environment Variables

```bash
# Override .env settings with environment variables
ORACLE_HOST=prod-oracle.example.com \
POSTGRES_HOST=prod-postgres.example.com \
dotnet run
```

---

## 📁 Project Structure

```
Ora2PgDataValidator/
├── Ora2PgDataValidator.csproj    # Project file
├── Program.cs                      # Main application entry point
├── appsettings.json                # Application configuration
├── .env.example                    # Example environment variables
├── README.md                       # This file
├── .gitignore                      # Git ignore patterns
│
├── src/                            # Source code
│   ├── Config/
│   │   ├── ApplicationProperties.cs    # Configuration management
│   │   └── DatabaseConfig.cs           # Database configuration
│   │
│   ├── Connection/
│   │   ├── DatabaseType.cs             # Enum: Oracle/PostgreSQL
│   │   └── DatabaseConnectionManager.cs # Connection pooling
│   │
│   ├── Extractor/
│   │   ├── DataExtractor.cs            # Data extraction logic
│   │   └── TableMetadata.cs            # Table schema information
│   │
│   ├── Hasher/
│   │   └── HashGenerator.cs            # SHA256/MD5 hash generation
│   │
│   ├── Comparison/
│   │   ├── HashComparator.cs           # Hash comparison logic
│   │   └── ComparisonResult.cs         # Comparison results
│   │
│   ├── Report/
│   │   ├── CsvHashWriter.cs            # CSV file generation
│   │   └── ComparisonReportWriter.cs   # Text report generation
│   │
│   ├── Processor/
│   │   ├── SingleDatabaseProcessor.cs      # Single DB extraction
│   │   ├── ComparisonDatabaseProcessor.cs  # Dual DB comparison
│   │   └── PostgresMultiDatabaseProcessor.cs # Multi-DB PostgreSQL
│   │
│   └── Util/
│       ├── CaseConverter.cs            # Case normalization
│       └── FileHelper.cs               # File utilities
│
├── tests/                          # Unit and integration tests
│   └── Ora2PgDataValidator.Tests/
│
├── reports/                        # Generated reports (git-ignored)
└── logs/                           # Application logs (git-ignored)
```

---

## 🔍 Troubleshooting

### Issue: `.env` file not found

**Error Message:**

```
⚠ .env file not found - checked solution root, project directory, and parent directories
Using environment variables and default values only
```

**Solution:**

```bash
# Navigate to solution root
cd ora2pg-migration-audit

# Ensure .env file exists at solution root
cp .env.example .env

# Edit with your credentials
nano .env

# Verify file location (should show .env at solution root)
ls -la .env
```

**Note:** The `.env` file must be at the **solution root** (`ora2pg-migration-audit/.env`), not in project directories.
The application uses intelligent detection to find it automatically.

### Issue: Oracle connection failed

**Error Message:**

```
ORA-12154: TNS:could not resolve the connect identifier specified
```

**Solution:**

```bash
# Test Oracle connectivity
telnet your-oracle-host.com 1521

# Verify Oracle service name in .env matches actual service
# Common values: ORCL, XE, FREEPDB1, XEPDB1

# Check credentials
sqlplus username/password@//host:1521/ORCL
```

### Issue: PostgreSQL connection failed

**Solution:**

```bash
# Test PostgreSQL connectivity
psql -h postgres-host.com -U username -d database

# Check pg_hba.conf allows remote connections
# Check postgresql.conf: listen_addresses = '*'
```

### Issue: NuGet restore failed

**Solution:**

```bash
# Clear NuGet cache
dotnet nuget locals all --clear

# Restore packages
dotnet restore

# Update packages
dotnet restore --force
```
