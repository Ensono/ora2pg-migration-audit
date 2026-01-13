# Ora2Pg.Common

> Shared library for Oracle to PostgreSQL migration validation tools

## Overview

`Ora2Pg.Common` is a class library that provides shared functionality for all migration validators in the `ora2pg-migration-audit` solution. This eliminates code duplication and ensures consistent behavior across all validation tools.

## Purpose

Centralizes common functionality needed by multiple validators:
- Database configuration management
- Database connection pooling
- Oracle/PostgreSQL connection management
- Schema/table name normalization utilities

## Architecture

```
Ora2Pg.Common/
├── Config/
│   ├── ApplicationProperties.cs     # Singleton for .env and environment variables
│   └── DatabaseConfig.cs            # Oracle/PostgreSQL configuration
├── Connection/
│   ├── DatabaseType.cs              # Oracle/PostgreSQL enumeration
│   └── DatabaseConnectionManager.cs # Connection pooling and management
└── Util/
    └── CaseConverter.cs             # Name normalization (UPPERCASE ↔ lowercase)
```

## Key Components

### Config Namespace

#### ApplicationProperties
- **Pattern:** Thread-safe singleton
- **Purpose:** Load and manage configuration from `.env` files and environment variables
- **Features:**
  - Automatic `.env` file discovery (supports multiple project structures)
  - Type-safe getters: `Get()`, `GetInt()`, `GetBool()`, `GetArray()`
  - Fallback to default values

#### DatabaseConfig
- **Purpose:** Store database connection parameters
- **Features:**
  - Factory methods: `CreateOracleConfig()`, `CreatePostgresConfig()`
  - Connection string builders with proper escaping
  - Support for special characters in passwords (via Npgsql builder)

### Connection Namespace

#### DatabaseType
- **Purpose:** Enumeration for database types
- **Values:** `Oracle`, `PostgreSQL`

#### DatabaseConnectionManager
- **Pattern:** Disposable resource manager
- **Purpose:** Manage database connections and connection pooling
- **Features:**
  - Initialize connection pools: `InitializePool(dbType, config)`
  - Get connections: `GetConnection(dbType)`
  - Test connections: `TestConnection(dbType)`
  - Schema introspection: `GetTablesInSchema(dbType, schema)`
  - Automatic cleanup on disposal


### Util Namespace

#### CaseConverter
- **Purpose:** Normalize schema and table names based on database conventions
- **Rules:**
  - Oracle: `UPPERCASE`
  - PostgreSQL: `lowercase`

**Features:**
- `NormalizeSchemaName(name, dbType)` - Normalize schema names
- `NormalizeTableName(name, dbType)` - Normalize table names
- `NormalizeTableReference(ref, dbType)` - Normalize `schema.table` references
- `ParseAndNormalizeMapping(str)` - Parse mapping strings (e.g., `"CHINOOK.ALBUM=chinook.album"`)


## Dependencies

**NuGet Packages:**
- `Oracle.ManagedDataAccess.Core` (23.26.0) - Oracle database connectivity
- `Npgsql` (10.0.0) - PostgreSQL database connectivity
- `DotNetEnv` (3.1.1) - .env file loading
- `Serilog` (4.3.0) - Structured logging

## Referenced By

This library is referenced by:
- **Ora2PgDataValidator** - Data validation tool
- **Ora2PgSchemaComparer** - Schema comparison tool
- **(Future validators)** - Any new migration validation tools

## Build and Test

```bash
# Build the library
dotnet build

# Run from solution root (builds all projects including dependents)
cd ..
dotnet build
```
