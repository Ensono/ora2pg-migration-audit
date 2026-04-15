using System.Data;
using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;

namespace Ora2PgDataValidator.Extractor;


public class DataExtractor
{
    private readonly IDbConnection _connection;
    private readonly DatabaseType _databaseType;
    private readonly int _maxRowsPerTable;
    private readonly int _commandTimeoutSeconds;
    private readonly HashSet<string> _columnsToSkip;
    private readonly bool _skipLobColumns;
    private readonly int _lobSizeLimit;
    private readonly int _extraOrderColumns;  // Additional columns to add for retry logic

    private readonly Dictionary<string, TableMetadata> _metadataCache = new(StringComparer.OrdinalIgnoreCase);

    private static readonly HashSet<string> LobColumnTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "BLOB", "CLOB", "NCLOB", "BFILE",                              // Oracle LOB types (exact)
        "LONG RAW", "RAW", "LONG",                                     // Oracle LONG types  
        "XMLTYPE",                                                      // Oracle XML type (also LOB)
        "BYTEA", "OID", "TEXT",                                        // PostgreSQL binary/text types
        "SYSTEM.BYTE[]", "BYTE[]",                                     // .NET binary representations
        "Oracle.ManagedDataAccess.Types.OracleClob",                   // Oracle ADO.NET CLOB type
        "Oracle.ManagedDataAccess.Types.OracleBlob",                   // Oracle ADO.NET BLOB type
        "Npgsql.NpgsqlTypes.NpgsqlDbType"                              // PostgreSQL type namespace
    };

    private static readonly string[] LobTypePatterns =
    {
        "CLOB", "BLOB", "BFILE", "XMLTYPE", "BYTEA", "LONG RAW", "BYTE[]", "OracleClob", "OracleBlob"
    };

    public DataExtractor(IDbConnection connection, DatabaseType databaseType, int extraOrderColumns = 0)
    {
        _connection = connection;
        _databaseType = databaseType;
        _extraOrderColumns = extraOrderColumns;

        var props = ApplicationProperties.Instance;
        _maxRowsPerTable = props.GetInt("MAX_ROWS_PER_TABLE",
                                       props.GetInt("max.rows.per.table", 0));

        _commandTimeoutSeconds = props.GetInt("COMMAND_TIMEOUT_SECONDS",
                                             props.GetInt("command.timeout.seconds", 300));
        
        _skipLobColumns = props.Get("SKIP_LOB_COLUMNS",
                                    props.Get("SKIP_BLOB_COLUMNS", "false"))
                               .Equals("true", StringComparison.OrdinalIgnoreCase);
        if (_skipLobColumns)
        {
            Log.Information("LOB column skipping enabled - BLOB/CLOB/bytea/text columns will be excluded from data extraction");
        }
        
        _lobSizeLimit = props.GetInt("LOB_SIZE_LIMIT", props.GetInt("BLOB_SIZE_LIMIT", 0));
        
        // BLOB -> RAW: max 2000 bytes in Oracle SQL; CLOB -> VARCHAR2: max 4000 bytes
        // Validate against the higher CLOB limit; BLOB will be further capped to 2000 internally
        const int MaxLobLimit = 4000;
        if (_lobSizeLimit > MaxLobLimit)
        {
            Log.Error("LOB_SIZE_LIMIT={Limit} exceeds maximum of {Max} bytes. " +
                     "Oracle DBMS_LOB.SUBSTR returns VARCHAR2 (for CLOB, max {Max} bytes) or RAW (for BLOB, max 2000 bytes) in SQL context. " +
                     "Please set LOB_SIZE_LIMIT to {Max} or less, or use SKIP_LOB_COLUMNS=true.",
                     _lobSizeLimit, MaxLobLimit, MaxLobLimit);
            throw new InvalidOperationException(
                $"LOB_SIZE_LIMIT={_lobSizeLimit} exceeds maximum of {MaxLobLimit} bytes. " +
                $"Set LOB_SIZE_LIMIT to {MaxLobLimit} or less (BLOB columns are capped at 2000), or use SKIP_LOB_COLUMNS=true.");
        }
        
        if (_lobSizeLimit > 0 && !_skipLobColumns)
        {
            Log.Information("LOB size limit enabled: Only first {Limit} bytes will be fetched from BLOB/CLOB columns", _lobSizeLimit);
        }

        string skipColumnsConfig = databaseType == DatabaseType.PostgreSQL
            ? props.Get("POSTGRES_SKIP_COLUMNS", "")
            : props.Get("ORACLE_SKIP_COLUMNS", "");

        _columnsToSkip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        // Always skip DMS-added rowid columns in PostgreSQL
        if (databaseType == DatabaseType.PostgreSQL)
        {
            _columnsToSkip.Add("rowid");
            Log.Information("Auto-skipping DMS 'rowid' column in PostgreSQL (added by DMS for tables without PK)");
        }
        
        if (!string.IsNullOrWhiteSpace(skipColumnsConfig))
        {
            var columns = skipColumnsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var column in columns)
            {
                _columnsToSkip.Add(column);
            }
            Log.Information("Columns to skip in {DatabaseType}: {Columns}", databaseType, string.Join(", ", _columnsToSkip));
        }

        if (_maxRowsPerTable > 0)
        {
            Log.Information("Row limit enabled: Will process maximum {MaxRows} rows per table", _maxRowsPerTable);
        }
        else
        {
            Log.Information("No row limit: Will process all rows in each table");
        }

        Log.Information("Database command timeout set to {TimeoutSeconds} seconds", _commandTimeoutSeconds);
    }


    public TableMetadata GetTableMetadata(string tableReference)
    {
        if (_metadataCache.TryGetValue(tableReference, out var cached))
        {
            Log.Debug("Returning cached metadata for table: {TableReference}", tableReference);
            return cached;
        }

        Log.Information("Retrieving metadata for table: {TableReference}", tableReference);

        string? schema = null;
        string tableName = tableReference;

        if (tableReference.Contains("."))
        {
            var parts = tableReference.Split('.', 2);
            schema = parts[0];
            tableName = parts[1];
        }

        var columns = new List<TableMetadata.ColumnMetadata>();
        var primaryKeyColumns = new List<string>();

        var oracleNativeTypes = _databaseType == DatabaseType.Oracle && schema != null
            ? GetOracleNativeSqlTypes(schema, tableName)
            : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {tableReference} WHERE 1=0"; // Get schema only, no data

        using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
        {
            var schemaTable = reader.GetSchemaTable();

            if (schemaTable != null)
            {
                int position = 1;
                int skippedCount = 0;
                int blobSkippedCount = 0;
                foreach (DataRow row in schemaTable.Rows)
                {
                    string columnName = row["ColumnName"].ToString() ?? "";
                    
                    string nativeTypeName = schemaTable.Columns.Contains("DataTypeName") && row["DataTypeName"] != DBNull.Value
                        ? row["DataTypeName"].ToString() ?? ""
                        : "";
                    string dotNetTypeName = row["DataType"] != DBNull.Value ? row["DataType"].ToString() ?? "" : "";

                    string columnType;
                    if (oracleNativeTypes.TryGetValue(columnName, out var oracleSqlType) && !string.IsNullOrWhiteSpace(oracleSqlType))
                    {
                        columnType = oracleSqlType;
                    }
                    else
                    {
                        columnType = !string.IsNullOrWhiteSpace(nativeTypeName) ? nativeTypeName : dotNetTypeName;
                    }
                    
                    bool isKey = row["IsKey"] != DBNull.Value && (bool)row["IsKey"];
                    
                    Log.Debug("Column schema: {ColumnName} = native='{NativeType}' dotnet='{DotNetType}' -> using='{ColumnType}', IsLob={IsLob}", 
                        columnName, nativeTypeName, dotNetTypeName, columnType, IsLobColumnType(columnType));

                    if (_columnsToSkip.Contains(columnName))
                    {
                        skippedCount++;
                        Log.Debug("Skipping column: {ColumnName} in table {TableReference}", columnName, tableReference);
                        continue;
                    }
                    
                    // Skip LOB columns if SKIP_LOB_COLUMNS is enabled
                    if (_skipLobColumns && (IsLobColumnType(columnType) || CouldBeLobColumn(columnName, columnType)))
                    {
                        blobSkippedCount++;
                        Log.Information("Skipping LOB column: {ColumnName} ({ColumnType}) in table {TableReference} (SKIP_LOB_COLUMNS=true)", 
                            columnName, columnType, tableReference);
                        continue;
                    }

                    columns.Add(new TableMetadata.ColumnMetadata(columnName, columnType, position));

                    if (isKey)
                    {
                        primaryKeyColumns.Add(columnName);
                    }

                    position++;
                }

                if (skippedCount > 0)
                {
                    Log.Information("Skipped {Count} configured column(s) in table {TableReference}", skippedCount, tableReference);
                }
                
                if (blobSkippedCount > 0)
                {
                    Log.Information("Skipped {Count} BLOB/binary column(s) in table {TableReference} (SKIP_BLOB_COLUMNS=true)", 
                        blobSkippedCount, tableReference);
                }
            }
        }

        var primaryKeyFromDb = GetPrimaryKeyColumns(schema, tableName);
        if (primaryKeyFromDb.Count > 0)
        {
            var columnNameSet = columns.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            primaryKeyColumns = primaryKeyFromDb.Where(columnNameSet.Contains).ToList();
        }

        if (primaryKeyColumns.Count == 0 && columns.Count > 0)
        {
            var idColumns = columns
                .Where(c => c.Name.EndsWith("_ID", StringComparison.OrdinalIgnoreCase) || 
                           c.Name.EndsWith("ID", StringComparison.OrdinalIgnoreCase))
                .Select(c => c.Name)
                .ToList();

            if (idColumns.Count > 0)
            {
                primaryKeyColumns.AddRange(idColumns);
                Log.Warning("No primary key found for table {TableReference}, will order by ID columns for consistency: {OrderColumns}",
                           tableReference, string.Join(", ", idColumns));
            }
            else
            {
                // Use all orderable columns (exclude BLOB, CLOB, and other non-orderable types)
                var orderableColumns = columns
                    .Where(c => IsOrderableColumnType(c.Type))
                    .Select(c => c.Name)
                    .ToList();
                
                if (orderableColumns.Count > 0)
                {
                    primaryKeyColumns.AddRange(orderableColumns);
                    Log.Warning("No primary key or ID columns found for table {TableReference}, will order by ALL {ColumnCount} orderable column(s) for maximum consistency: {OrderColumns}",
                               tableReference, orderableColumns.Count, string.Join(", ", orderableColumns));
                }
                else
                {
                    // Fallback: use first column even if it might not be ideal
                    primaryKeyColumns.Add(columns[0].Name);
                    Log.Warning("No orderable columns found for table {TableReference}, using first column as fallback: {FirstColumn}",
                               tableReference, columns[0].Name);
                }
            }
        }

        Log.Information("Found {ColumnCount} columns in table {TableReference} (after filtering)", columns.Count, tableReference);
        if (primaryKeyColumns.Count > 0)
        {
            Log.Information("Order by columns: {OrderColumns}", string.Join(", ", primaryKeyColumns));
        }

        var metadata = new TableMetadata(tableReference, columns, primaryKeyColumns);
        _metadataCache[tableReference] = metadata;
        return metadata;
    }

    private bool IsLobColumnType(string columnType)
    {
        if (string.IsNullOrWhiteSpace(columnType))
        {
            return false;
        }

        if (LobColumnTypes.Contains(columnType))
        {
            Log.Debug("Column type '{ColumnType}' matched LOB exact type", columnType);
            return true;
        }

        foreach (var pattern in LobTypePatterns)
        {
            if (columnType.IndexOf(pattern, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                Log.Debug("Column type '{ColumnType}' matched LOB pattern '{Pattern}'", columnType, pattern);
                return true;
            }
        }
        
        Log.Debug("Column type '{ColumnType}' is NOT a LOB", columnType);
        return false;
    }

    private bool CouldBeLobColumn(string columnName, string columnType)
    {
        var nameUpper = columnName.ToUpperInvariant();
        var typeUpper = columnType.ToUpperInvariant();
        
        if ((nameUpper.Contains("XML") || nameUpper.Contains("CLOB") || nameUpper.Contains("BLOB"))
            && !typeUpper.Contains("VARCHAR") && !typeUpper.Contains("CHAR"))
        {
            Log.Debug("Column '{ColumnName}' ({ColumnType}) suspected as LOB based on name pattern", 
                columnName, columnType);
            return true;
        }
        
        return false;
    }

    private static bool IsTimestampColumnType(string columnTypeUpper)
    {
        return columnTypeUpper.Contains("TIMESTAMP") ||
               columnTypeUpper.Contains("DATE") ||
               columnTypeUpper == "DATE" ||
               columnTypeUpper.StartsWith("TIMESTAMP");
    }

    private static bool IsNumericOrTimestampColumnType(string columnTypeUpper)
    {
        // Timestamps (highest priority for ordering)
        if (IsTimestampColumnType(columnTypeUpper))
            return true;
        
        // Integer types (perfect cross-DB consistency)
        if (columnTypeUpper.Contains("INT") || 
            columnTypeUpper.StartsWith("NUMBER") || 
            columnTypeUpper == "BIGINT" || 
            columnTypeUpper == "SMALLINT" || 
            columnTypeUpper == "TINYINT" ||
            columnTypeUpper.Contains("SERIAL"))
            return true;
        
        // Decimal/numeric types (consistent if precision matches)
        if (columnTypeUpper == "NUMERIC" || 
            columnTypeUpper.StartsWith("NUMERIC(") ||
            columnTypeUpper == "DECIMAL" ||
            columnTypeUpper.StartsWith("DECIMAL(") ||
            columnTypeUpper.StartsWith("NUMBER("))
            return true;
        
        // Float types
        if (IsFloatingPointColumnType(columnTypeUpper))
            return true;
        
        return false;
    }

    private static bool IsFloatingPointColumnType(string columnTypeUpper)
    {
        if (columnTypeUpper == "FLOAT" ||
            columnTypeUpper == "REAL"  ||
            columnTypeUpper == "BINARY_FLOAT" ||
            columnTypeUpper == "BINARY_DOUBLE")
        {
            return true;
        }

        if (columnTypeUpper.StartsWith("FLOAT(") ||
            columnTypeUpper.StartsWith("BINARY_FLOAT(") ||
            columnTypeUpper.StartsWith("BINARY_DOUBLE("))
        {
            return true;
        }

        return false;
    }

    private bool IsOrderableColumnType(string columnType)
    {
        if (string.IsNullOrWhiteSpace(columnType))
        {
            return true; // Default to orderable if unknown
        }

        var typeUpper = columnType.ToUpperInvariant();

        // Non-orderable types (case-insensitive check)
        var nonOrderableTypes = new[]
        {
            "BLOB",           // Oracle binary large object
            "CLOB",           // Oracle character large object
            "NCLOB",          // Oracle national character large object
            "BFILE",          // Oracle binary file
            "BYTEA",          // PostgreSQL binary data
            "TEXT",           // PostgreSQL large text (can be huge like CLOB)
            "JSON",           // JSON data
            "JSONB",          // PostgreSQL binary JSON
            "XML",            // XML data
            "XMLTYPE",        // Oracle XML type
            "GEOGRAPHY",      // Spatial types
            "GEOMETRY",       // Spatial types
            "HSTORE",         // PostgreSQL key-value store
            "TSVECTOR",       // PostgreSQL text search
            "TSQUERY",        // PostgreSQL text search query
            "ARRAY",          // Array types (may not be orderable)
            "SYSTEM.BYTE[]",  // .NET byte array representation
            "BYTE[]"          // Byte array
        };

        // Check if column type matches any non-orderable type
        foreach (var nonOrderableType in nonOrderableTypes)
        {
            if (typeUpper.Contains(nonOrderableType))
            {
                Log.Debug("Column type {ColumnType} is not orderable, will be excluded from ORDER BY", columnType);
                return false;
            }
        }

        return true;
    }

    private class ColumnStats
    {
        public string ColumnName { get; set; } = "";
        public long DistinctCount { get; set; }
        public long NullCount { get; set; }
        public double Selectivity => DistinctCount > 0 ? (double)DistinctCount : 0;
    }

    private List<ColumnStats> GetColumnStatistics(string tableReference, List<TableMetadata.ColumnMetadata> columns)
    {
        var stats = new List<ColumnStats>();
        
        if (columns.Count == 0)
        {
            return stats;
        }

        try
        {
            // Only analyze orderable columns (excludes LOBs/CLOB/BLOB/TEXT which cause ORA-22835)
            var orderableColumns = columns.Where(c => IsOrderableColumnType(c.Type)).ToList();
            
            if (orderableColumns.Count == 0)
            {
                return stats;
            }

            // Limit to first 10 columns to avoid query timeout
            var columnsToAnalyze = orderableColumns.Take(10).ToList();
            
            Log.Debug("Gathering statistics for {Count} orderable columns (excluding LOBs)", columnsToAnalyze.Count);
            
            foreach (var col in columnsToAnalyze)
            {
                try
                {
                    var quotedCol = QuoteIdentifier(col.Name);
                    var sql = _databaseType == DatabaseType.Oracle
                        ? $"SELECT COUNT(DISTINCT {quotedCol}) as distinct_count, " +
                          $"SUM(CASE WHEN {quotedCol} IS NULL THEN 1 ELSE 0 END) as null_count " +
                          $"FROM {tableReference} WHERE ROWNUM <= 10000"
                        : $"SELECT COUNT(DISTINCT {quotedCol}) as distinct_count, " +
                          $"COUNT(*) FILTER (WHERE {quotedCol} IS NULL) as null_count " +
                          $"FROM {tableReference} LIMIT 10000";

                    using var cmd = _connection.CreateCommand();
                    cmd.CommandText = sql;
                    cmd.CommandTimeout = 30; // Short timeout for stats gathering
                    
                    using var reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        stats.Add(new ColumnStats
                        {
                            ColumnName = col.Name,
                            DistinctCount = Convert.ToInt64(reader["distinct_count"]),
                            NullCount = Convert.ToInt64(reader["null_count"])
                        });
                    }
                }
                catch (Exception ex)
                {
                    Log.Debug("Could not get stats for column {Column}: {Error}", col.Name, ex.Message);
                    // If stats fail for a column, give it neutral stats
                    stats.Add(new ColumnStats
                    {
                        ColumnName = col.Name,
                        DistinctCount = 100,
                        NullCount = 0
                    });
                }
            }

            Log.Debug("Column statistics for {Table}: {Stats}", 
                tableReference,
                string.Join(", ", stats.Select(s => $"{s.ColumnName}(distinct={s.DistinctCount}, nulls={s.NullCount})")));
        }
        catch (Exception ex)
        {
            Log.Warning("Could not gather column statistics for {Table}: {Error}", tableReference, ex.Message);
        }

        return stats;
    }


    private Dictionary<string, string> GetOracleNativeSqlTypes(string schema, string tableName)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            var cleanSchema    = schema.Trim('"').ToUpper();
            var cleanTableName = tableName.Trim('"').ToUpper();

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                SELECT column_name, data_type
                FROM all_columns
                WHERE owner      = :schema_name
                  AND table_name = :table_name";

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "schema_name";
            schemaParam.Value = cleanSchema;
            cmd.Parameters.Add(schemaParam);

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "table_name";
            tableParam.Value = cleanTableName;
            cmd.Parameters.Add(tableParam);

            cmd.CommandTimeout = _commandTimeoutSeconds;

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var colName  = reader.GetString(0);
                var dataType = reader.GetString(1);
                result[colName] = dataType;
            }

            if (result.Count == 0)
            {
                Log.Warning("GetOracleNativeSqlTypes returned 0 rows for {Schema}.{Table} — " +
                            "check that the connecting user has SELECT on ALL_COLUMNS and the " +
                            "schema/table names are correct. Float columns will not be normalised.",
                            cleanSchema, cleanTableName);
            }
            else
            {
                Log.Information("Loaded {Count} Oracle native SQL types for {Schema}.{Table} — " +
                                "float columns: {FloatCols}",
                                result.Count, cleanSchema, cleanTableName,
                                string.Join(", ", result.Where(kv =>
                                    kv.Value.Equals("FLOAT", StringComparison.OrdinalIgnoreCase) ||
                                    kv.Value.Equals("BINARY_FLOAT", StringComparison.OrdinalIgnoreCase) ||
                                    kv.Value.Equals("BINARY_DOUBLE", StringComparison.OrdinalIgnoreCase) ||
                                    kv.Value.Equals("REAL", StringComparison.OrdinalIgnoreCase))
                                .Select(kv => $"{kv.Key}={kv.Value}")));
            }
        }
        catch (Exception ex)
        {
            Log.Warning("Could not load Oracle native SQL types for {Schema}.{Table}: {Error}. " +
                        "Float columns may not be normalised correctly.",
                        schema, tableName, ex.Message);
        }

        return result;
    }


    private List<string> GetPrimaryKeyColumns(string? schema, string tableName)
    {
        if (string.IsNullOrWhiteSpace(schema))
        {
            return new List<string>();
        }

        using var cmd = _connection.CreateCommand();
        cmd.CommandTimeout = _commandTimeoutSeconds;

        if (_databaseType == DatabaseType.Oracle)
        {
            cmd.CommandText = @"
                SELECT cols.column_name
                FROM all_constraints cons
                JOIN all_cons_columns cols
                  ON cons.owner = cols.owner
                  AND cons.constraint_name = cols.constraint_name
                WHERE cons.owner = :schema_name
                  AND cons.table_name = :table_name
                  AND cons.constraint_type = 'P'
                ORDER BY cols.position";

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "schema_name";
            schemaParam.Value = schema.ToUpper();
            cmd.Parameters.Add(schemaParam);

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "table_name";
            tableParam.Value = tableName.ToUpper();
            cmd.Parameters.Add(tableParam);
        }
        else if (_databaseType == DatabaseType.PostgreSQL)
        {
            cmd.CommandText = @"
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary
                  AND n.nspname = @schema
                  AND c.relname = @table
                ORDER BY array_position(i.indkey, a.attnum)";

            var schemaParam = cmd.CreateParameter();
            schemaParam.ParameterName = "@schema";
            schemaParam.Value = schema.ToLower();
            cmd.Parameters.Add(schemaParam);

            var tableParam = cmd.CreateParameter();
            tableParam.ParameterName = "@table";
            tableParam.Value = tableName.ToLower();
            cmd.Parameters.Add(tableParam);
        }
        else
        {
            return new List<string>();
        }

        var primaryKeys = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            primaryKeys.Add(reader.GetString(0));
        }

        return primaryKeys;
    }


    public List<object?[]> ExtractTableData(string tableReference, int fetchSize)
    {
        Log.Information("Extracting data from table: {TableReference}", tableReference);

        var rows = new List<object?[]>();

        ProcessRows(tableReference, batch => rows.AddRange(batch), int.MaxValue);

        return rows;
    }


    public long GetRowCount(string tableReference)
    {
        string sql = $"SELECT COUNT(*) FROM {tableReference}";

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;

        var result = cmd.ExecuteScalar();
        return result != null ? Convert.ToInt64(result) : 0;
    }


    public void ExtractTableDataInBatches(string tableReference, int batchSize, Action<List<object?[]>> consumer)
    {
        Log.Information("Extracting data from table {TableReference} in batches of {BatchSize}",
                       tableReference, batchSize);

        ProcessRows(tableReference, consumer, batchSize);
    }


    private static readonly TimeZoneInfo _easternTz =
        TimeZoneInfo.FindSystemTimeZoneById(
            OperatingSystem.IsWindows()
                ? "Eastern Standard Time"
                : "America/New_York");

    private static bool IsTimestampWithTimeZoneType(string columnTypeUpper) =>
        columnTypeUpper.Contains("WITH TIME ZONE") ||
        columnTypeUpper.Contains("WITH LOCAL TIME ZONE");

    private void ProcessRows(string tableReference, Action<List<object?[]>> consumer, int batchSize)
    {
        var metadata = GetTableMetadata(tableReference);
        string sql = BuildSelectQuery(tableReference, metadata);

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _commandTimeoutSeconds;

        using var reader = cmd.ExecuteReader();
        int columnCount = reader.FieldCount;

        var batch = new List<object?[]>();
        int rowCount = 0;

        while (reader.Read())
        {
            if (_maxRowsPerTable > 0 && rowCount >= _maxRowsPerTable)
            {
                break;
            }

            var row = new object?[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                try
                {
                    row[i] = reader.GetValue(i);
                    if (row[i] == DBNull.Value)
                    {
                        row[i] = null;
                    }
                    else if (i < metadata.Columns.Count)
                    {
                        var colTypeUpper = (metadata.Columns[i].Type ?? "").ToUpperInvariant();

                        // Convert Oracle TIMESTAMP WITH TIME ZONE from EST to UTC
                        if (_databaseType == DatabaseType.Oracle && 
                            row[i] is DateTime oracleDt && 
                            IsTimestampWithTimeZoneType(colTypeUpper))
                        {
                            row[i] = TimeZoneInfo.ConvertTimeToUtc(
                                DateTime.SpecifyKind(oracleDt, DateTimeKind.Unspecified),
                                _easternTz);
                        }

                        // Convert Oracle FLOAT decimal to double for consistency with PostgreSQL
                        if (row[i] is decimal floatDecVal && IsFloatingPointColumnType(colTypeUpper))
                        {
                            row[i] = (double)floatDecVal;
                        }
                    }
                }
                catch (OverflowException)
                {
                    if (i < metadata.Columns.Count && IsFloatingPointColumnType(metadata.Columns[i].Type.ToUpperInvariant()))
                    {
                        try
                        {
                            row[i] = reader.GetDouble(i);
                        }
                        catch
                        {
                            row[i] = null;
                            Log.Warning("Column {Name} value overflow, setting to NULL", reader.GetName(i));
                        }
                    }
                    else
                    {
                        row[i] = null;
                        Log.Warning("Column {Name} value overflow, setting to NULL", reader.GetName(i));
                    }
                }
            }
            batch.Add(row);
            rowCount++;

            if (batch.Count >= batchSize)
            {
                consumer(batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            consumer(batch);
        }

        Log.Information("Extracted {RowCount} rows from table {TableReference}", rowCount, tableReference);
        if (_maxRowsPerTable > 0 && rowCount >= _maxRowsPerTable)
        {
            Log.Information("(Limited to first {MaxRows} rows due to MAX_ROWS_PER_TABLE setting)", _maxRowsPerTable);
        }
    }

    private string BuildOrderByExpression(string columnName, string columnType, bool useAggressiveNormalization = false)
    {
        var quotedName = QuoteIdentifier(columnName);
        var typeUpper = (columnType ?? "").ToUpperInvariant();
        
        Log.Debug("BuildOrderByExpression: column={Column}, type={Type}, dbType={DbType}, aggressive={Aggressive}", 
            columnName, columnType, _databaseType, useAggressiveNormalization);
        
        bool isLobType = typeUpper.Contains("CLOB") || typeUpper.Contains("NCLOB") ||
                         typeUpper.Contains("BLOB") || typeUpper.Contains("BYTEA") ||
                         typeUpper.Contains("TEXT") && _databaseType == DatabaseType.PostgreSQL;
        
        if (isLobType)
        {
            Log.Warning("LOB column {Column} ({Type}) should not be used in ORDER BY - using constant instead", 
                columnName, columnType);
            return "1"; // Fallback to constant
        }
        
        bool isStringType = typeUpper.Contains("CHAR") || typeUpper.Contains("STRING") || 
                           typeUpper.Contains("VARCHAR");
        
        if (isStringType)
        {
            if (_databaseType == DatabaseType.Oracle)
            {
                if (useAggressiveNormalization)
                {
                    // RETRY: Use UPPER/TRIM with binary collation (slower but more reliable)
                    return $"NLSSORT(UPPER(TRIM({quotedName})), 'NLS_SORT=BINARY') ASC NULLS FIRST";
                }
                else
                {
                    // FIRST ATTEMPT: Simple binary sort (faster)
                    return $"NLSSORT({quotedName}, 'NLS_SORT=BINARY') ASC NULLS FIRST";
                }
            }
            else
            {
                if (useAggressiveNormalization)
                {
                    // RETRY: Use UPPER/TRIM with C collation (slower but more reliable)
                    return $"UPPER(TRIM({quotedName})) COLLATE \"C\" ASC NULLS FIRST";
                }
                else
                {
                    // FIRST ATTEMPT: Simple C collation (faster)
                    return $"{quotedName} COLLATE \"C\" ASC NULLS FIRST";
                }
            }
        }
        else
        {
            return $"{quotedName} ASC NULLS FIRST";
        }
    }

    private string BuildSelectQuery(string tableReference, TableMetadata metadata)
    {
        int totalOrderableColumns = metadata.Columns.Count(c => IsOrderableColumnType(c.Type));
        
        int baseTargetOrderColumns = totalOrderableColumns switch
        {
            <= 3 => totalOrderableColumns,
            <= 6 => Math.Max(3, totalOrderableColumns - 1),
            <= 10 => Math.Max(4, totalOrderableColumns / 2),
            <= 15 => 5,
            _ => 6
        };
        
        int targetOrderColumns = Math.Max(1, Math.Min(baseTargetOrderColumns + _extraOrderColumns, totalOrderableColumns));
        
        if (_extraOrderColumns > 0)
        {
            Log.Information("Retry attempt: Using {Target} ORDER BY columns", targetOrderColumns);
        }
        
        var allOrderableColumns = metadata.Columns.Where(c => IsOrderableColumnType(c.Type)).ToList();
        var columnStats = GetColumnStatistics(tableReference, allOrderableColumns);
        var orderColumns = BuildOrderColumnList(allOrderableColumns, columnStats, metadata.PrimaryKeyColumns, targetOrderColumns);
        
        string orderByClause = BuildOrderByClause(orderColumns);
        string columnList = BuildColumnList(metadata);
        string sql = BuildFinalQuery(tableReference, columnList, orderByClause, metadata);
        
        return sql;
    }

    private List<TableMetadata.ColumnMetadata> BuildOrderColumnList(
        List<TableMetadata.ColumnMetadata> allOrderableColumns,
        List<ColumnStats> columnStats,
        List<string> primaryKeyColumns,
        int targetCount)
    {
        var orderColumns = new List<TableMetadata.ColumnMetadata>();
        
        // Priority 1: Numeric/timestamp columns FIRST (deterministic ordering across DBs)
        var numericColumns = allOrderableColumns
            .Where(c => IsNumericOrTimestampColumnType(c.Type.ToUpperInvariant()))
            .OrderByDescending(c => IsTimestampColumnType(c.Type.ToUpperInvariant()) ? 2 : 1) // Timestamps highest priority
            .ThenByDescending(c => primaryKeyColumns.Any(pk => pk.Equals(c.Name, StringComparison.OrdinalIgnoreCase)) ? 1 : 0) // Then PKs
            .ThenByDescending(c => GetDistinctCount(c.Name, columnStats))
            .Take(Math.Max(targetCount / 2, 3)) // At least half the order columns should be numeric
            .ToList();
        
        orderColumns.AddRange(numericColumns);
        
        if (numericColumns.Count > 0)
        {
            Log.Information("Using {Count} numeric/timestamp column(s) for deterministic ordering: {Columns}",
                numericColumns.Count,
                string.Join(", ", numericColumns.Select(c => $"{c.Name}({c.Type})")));
        }
        
        // Priority 2: High-cardinality string columns (after numeric stability)
        if (orderColumns.Count < targetCount)
        {
            var stringColumns = allOrderableColumns
                .Where(c => !orderColumns.Any(o => o.Name.Equals(c.Name, StringComparison.OrdinalIgnoreCase)))
                .OrderByDescending(c => GetDistinctCount(c.Name, columnStats))
                .ThenBy(c => GetNullCount(c.Name, columnStats))
                .Take(targetCount - orderColumns.Count);
            
            orderColumns.AddRange(stringColumns);
        }
        
        // Priority 3: Primary key columns (if not already included)
        if (primaryKeyColumns.Count > 0 && orderColumns.Count < targetCount)
        {
            foreach (var pk in primaryKeyColumns)
            {
                if (orderColumns.Count >= targetCount) break;
                
                var col = allOrderableColumns.FirstOrDefault(c => c.Name.Equals(pk, StringComparison.OrdinalIgnoreCase));
                if (col != null && !orderColumns.Any(o => o.Name.Equals(col.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    orderColumns.Add(col);
                }
            }
        }
        
        // Priority 4: Fill remaining slots
        if (orderColumns.Count < targetCount)
        {
            var remainingColumns = allOrderableColumns
                .Where(c => !orderColumns.Any(o => o.Name.Equals(c.Name, StringComparison.OrdinalIgnoreCase)))
                .Take(targetCount - orderColumns.Count);
            
            orderColumns.AddRange(remainingColumns);
        }
        return orderColumns.Where(c => !IsLobColumnType(c.Type) && !CouldBeLobColumn(c.Name, c.Type)).ToList();
    }

    private long GetDistinctCount(string columnName, List<ColumnStats> stats)
    {
        return stats.FirstOrDefault(s => s.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))?.DistinctCount ?? 0;
    }

    private long GetNullCount(string columnName, List<ColumnStats> stats)
    {
        return stats.FirstOrDefault(s => s.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))?.NullCount ?? long.MaxValue;
    }

    private string BuildOrderByClause(List<TableMetadata.ColumnMetadata> orderColumns)
    {
        if (orderColumns.Count == 0)
        {
            Log.Warning("No orderable columns available - using constant ORDER BY 1");
            return "1";
        }
        
        bool useAggressiveNormalization = _extraOrderColumns > 0;
        var orderByParts = orderColumns.Select(c => BuildOrderByExpression(c.Name, c.Type, useAggressiveNormalization));
        
        Log.Information("ORDER BY {Count} column(s): {Columns}",
            orderColumns.Count,
            string.Join(", ", orderColumns.Select(c => c.Name)));
        
        return string.Join(", ", orderByParts);
    }

    private string BuildColumnList(TableMetadata metadata)
    {
        if (_skipLobColumns && metadata.Columns.Any(c => IsLobColumnType(c.Type)))
        {
            var nonLobColumns = metadata.Columns
                .Where(c => !IsLobColumnType(c.Type))
                .Select(c => QuoteIdentifier(c.Name));
            
            return string.Join(", ", nonLobColumns);
        }
        
        if (metadata.Columns.Any(c => IsLobColumnType(c.Type)))
        {
            var columnExpressions = metadata.Columns.Select(col =>
                IsLobColumnType(col.Type) ? BuildLobLimitedColumn(col.Name, col.Type) : QuoteIdentifier(col.Name));
            
            return string.Join(", ", columnExpressions);
        }
        
        return "*";
    }

    private string BuildFinalQuery(string tableReference, string columnList, string orderByClause, TableMetadata metadata)
    {
        var sql = new System.Text.StringBuilder($"SELECT {columnList} FROM {tableReference} ORDER BY {orderByClause}");

        if (orderByClause != "1" && metadata.Columns.Count > 0)
        {
            // Get columns already in ORDER BY
            var columnsInOrderBy = new HashSet<string>(
                orderByClause.Split(',')
                    .Select(part => part.Trim())
                    .Select(part => part.Split(new[] { ' ', '(' }, StringSplitOptions.RemoveEmptyEntries)[0])
                    .Select(col => col.Trim('"', '[', ']'))
                    .Where(col => !string.IsNullOrEmpty(col)),
                StringComparer.OrdinalIgnoreCase);

            // Add remaining orderable columns (except LOBs) with proper normalization
            bool useAggressiveNormalization = _extraOrderColumns > 0;
            var remainingColumns = metadata.Columns
                .Where(c => IsOrderableColumnType(c.Type) && 
                           !IsLobColumnType(c.Type) &&
                           !columnsInOrderBy.Contains(c.Name))
                .Select(c => BuildOrderByExpression(c.Name, c.Type, useAggressiveNormalization))
                .ToList();
            
            if (remainingColumns.Count > 0)
            {
                sql.Append(", ");
                sql.Append(string.Join(", ", remainingColumns));
                Log.Information("Added {Count} additional columns to ORDER BY for deterministic sorting", remainingColumns.Count);
            }

            // Final tie-breaker: concatenation of all orderable columns as text,
            // sorted identically on both sides using binary/C collation.
            var hashColumns = metadata.Columns
                .Where(c => IsOrderableColumnType(c.Type) && !IsLobColumnType(c.Type))
                .Select(c => QuoteIdentifier(c.Name))
                .ToList();
            
            if (hashColumns.Count > 0)
            {
                string hashTieBreaker = _databaseType == DatabaseType.Oracle
                    ? $", NLSSORT({string.Join(" || '|' || ", hashColumns.Select(c => $"NVL(TO_CHAR({c}), 'NULL')"))}, 'NLS_SORT=BINARY') ASC"
                    : $", ({string.Join(" || '|' || ", hashColumns.Select(c => $"COALESCE(CAST({c} AS TEXT), 'NULL')"))}) COLLATE \"C\" ASC";
                
                sql.Append(hashTieBreaker);
            }
        }

        if (_maxRowsPerTable > 0)
        {
            sql.Append(_databaseType == DatabaseType.Oracle
                ? $" FETCH FIRST {_maxRowsPerTable} ROWS ONLY"
                : $" LIMIT {_maxRowsPerTable}");
        }

        Log.Information("Query: {Sql}", sql.ToString());
        return sql.ToString();
    }

    private string BuildLobLimitedColumn(string columnName, string columnType)
    {
        var quotedName = QuoteIdentifier(columnName);
        var typeUpper = columnType.ToUpperInvariant();

        if (_databaseType == DatabaseType.Oracle)
        {
            if (typeUpper.Contains("BYTE[]") || typeUpper.Contains("BYTE") ||
                typeUpper.Contains("BLOB"))
            {
                var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 2000) : 2000;
                return $"DBMS_LOB.SUBSTR({quotedName}, {effectiveLimit}, 1) AS {quotedName}";
            }
            else if (typeUpper.Contains("CLOB") || typeUpper.Contains("NCLOB"))
            {
                var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 4000) : 4000;
                return $"DBMS_LOB.SUBSTR({quotedName}, {effectiveLimit}, 1) AS {quotedName}";
            }
            else if (typeUpper.Contains("RAW"))
            {
                var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 2000) : 2000;
                return $"SUBSTR({quotedName}, 1, {effectiveLimit}) AS {quotedName}";
            }
        }
        else if (_databaseType == DatabaseType.PostgreSQL)
        {
            if (typeUpper.Contains("BYTEA") || typeUpper.Contains("BYTE[]") || typeUpper.Contains("BYTE"))
            {
                var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 2000) : 2000;
                return $"substring({quotedName} from 1 for {effectiveLimit}) AS {quotedName}";
            }
            else if (typeUpper.Contains("TEXT"))
            {
                var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 4000) : 4000;
                return $"substring({quotedName} from 1 for {effectiveLimit}) AS {quotedName}";
            }
        }
        
        return quotedName;
    }

    private string QuoteIdentifier(string identifier)
    {
        if (_databaseType == DatabaseType.Oracle)
        {
            return $"\"{identifier.ToUpperInvariant()}\"";
        }
        else if (_databaseType == DatabaseType.PostgreSQL)
        {
            return $"\"{identifier.ToLowerInvariant()}\"";
        }
        return identifier;
    }
}
