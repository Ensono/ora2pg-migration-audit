# Row Count Validator — Guide

Validates that row counts match between Oracle and PostgreSQL after migration.  
No installation required — just configure and run.

---

## Prerequisites

| Platform | Requirement |
|---|---|
| Windows | Nothing — the `.exe` is self-contained |
| Linux / macOS | Nothing — the binary is self-contained |

---

## Quick Start

### Step 1 — Configure

Open the `.env` file in any text editor (Notepad, VS Code, nano, etc.) and fill in your database connection details:

```
ORACLE_HOST=your-oracle-server
ORACLE_PORT=1521
ORACLE_SERVICE=your-service-name
ORACLE_USER=your-oracle-user
ORACLE_PASSWORD=your-oracle-password
ORACLE_SCHEMA=YOUR_SCHEMA

POSTGRES_HOST=your-postgres-server
POSTGRES_PORT=5432
POSTGRES_DB=your-database-name
POSTGRES_USER=your-pg-user
POSTGRES_PASSWORD=your-pg-password
POSTGRES_SCHEMA=your_schema
```

> ⚠ **Password tip:** If your password contains `$`, `"`, or `` ` ``, wrap it in single quotes:  
> `ORACLE_PASSWORD='P@ssw0rd!with$pecial'`

---

### Step 2 — Run

**Windows** — double-click `row-count-validator.exe`, or run in PowerShell/Command Prompt:
```powershell
.\row-count-validator.exe
```

**Linux / macOS** — open a terminal in this folder and run:
```bash
./row-count-validator
```

---

### Step 3 — Review Results

The tool prints a summary to the console and saves a report file in the same folder:

```
✅ MATCH   SCHEMA.TABLE_NAME          Oracle: 12,345  PostgreSQL: 12,345
❌ MISMATCH SCHEMA.OTHER_TABLE        Oracle: 9,876   PostgreSQL: 9,800  (diff: 76)
⚠  MISSING  SCHEMA.MISSING_TABLE     Oracle: 500     PostgreSQL: not found
```

A timestamped report file is also saved (e.g. `row-count-report-20260422-103045.html`).

---

## Common Settings

| Setting | Default | What it does |
|---|---|---|
| `TABLES_TO_COMPARE` | `ALL` | Set to `ALL` or list specific tables |
| `TABLE_EXCLUSION_PATTERNS` | _(empty)_ | Skip tables matching these substrings |
| `PARALLEL_TABLES` | `4` | How many tables to count at once |
| `COMMAND_TIMEOUT_SECONDS` | `600` | Timeout per query (10 min) |
| `DETAILED_ROW_COMPARISON` | `true` | Show exactly which rows are missing |

Full list of settings is in the `.env` file with comments.

---

## Multiple Schemas

To validate multiple schemas at once, list them as comma-separated pairs:

```
ORACLE_SCHEMA=SCHEMA1,SCHEMA2,SCHEMA3
POSTGRES_SCHEMA=schema1,schema2,schema3
```

Schemas are matched in order: `SCHEMA1 ↔ schema1`, `SCHEMA2 ↔ schema2`, etc.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `ORA-01017: invalid username/password` | Check `ORACLE_USER` / `ORACLE_PASSWORD` in `.env` |
| `Connection refused` on PostgreSQL | Check `POSTGRES_HOST` and `POSTGRES_PORT` |
| `ORA-12154: TNS: could not resolve` | Check `ORACLE_HOST` and `ORACLE_SERVICE` |
| Timeout errors | Increase `COMMAND_TIMEOUT_SECONDS=1800` |
| Very slow on large tables | Reduce `PARALLEL_TABLES=2` or set `DETAILED_ROW_COMPARISON=false` |

---

## Support

Contact the migration team with:
1. The console output (copy/paste or screenshot)
2. Which tables are showing differences
3. Your `.env` file **with passwords removed**
