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

    private static readonly HashSet<string> LobColumnTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "BLOB", "CLOB", "NCLOB", "BFILE",                              // Oracle LOB types
        "LONG RAW", "RAW", "LONG",                                     // Oracle LONG types  
        "XMLTYPE",                                                      // Oracle XML type (also LOB)
        "BYTEA", "OID", "TEXT",                                        // PostgreSQL binary/text types
        "SYSTEM.BYTE[]", "BYTE[]",                                     // .NET binary representations
        "Oracle.ManagedDataAccess.Types.OracleClob",                   // Oracle ADO.NET CLOB type
        "Oracle.ManagedDataAccess.Types.OracleBlob",                   // Oracle ADO.NET BLOB type
        "Npgsql.NpgsqlTypes.NpgsqlDbType"                              // PostgreSQL type namespace
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
        
        const int MaxLobLimit = 2000;
        if (_lobSizeLimit > MaxLobLimit)
        {
            Log.Error("LOB_SIZE_LIMIT={Limit} exceeds maximum of {Max} bytes. " +
                     "Oracle DBMS_LOB.SUBSTR returns RAW (for BLOB) or VARCHAR2 (for CLOB), " +
                     "limited to {Max} bytes in SQL context for consistent hashing. " +
                     "Please set LOB_SIZE_LIMIT to {Max} or less, or use SKIP_LOB_COLUMNS=true.",
                     _lobSizeLimit, MaxLobLimit, MaxLobLimit);
            throw new InvalidOperationException(
                $"LOB_SIZE_LIMIT={_lobSizeLimit} exceeds maximum of {MaxLobLimit} bytes. " +
                $"Set LOB_SIZE_LIMIT to {MaxLobLimit} or less, or use SKIP_LOB_COLUMNS=true.");
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
                    string columnType = schemaTable.Columns.Contains("DataTypeName") && row["DataTypeName"] != DBNull.Value
                        ? row["DataTypeName"].ToString() ?? ""
                        : row["DataType"].ToString() ?? "";
                    bool isKey = row["IsKey"] != DBNull.Value && (bool)row["IsKey"];
                    
                    Log.Debug("Column schema: {ColumnName} = {ColumnType} (native), {NetType} (.NET), IsLob={IsLob}", 
                        columnName, columnType, row["DataType"], IsLobColumnType(columnType));

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

        return new TableMetadata(tableReference, columns, primaryKeyColumns);
    }

    private bool IsLobColumnType(string columnType)
    {
        if (string.IsNullOrWhiteSpace(columnType))
        {
            return false;
        }

        var typeUpper = columnType.ToUpperInvariant();
        
        foreach (var lobType in LobColumnTypes)
        {
            if (typeUpper.Contains(lobType, StringComparison.OrdinalIgnoreCase))
            {
                Log.Debug("Column type '{ColumnType}' matched LOB pattern '{LobPattern}'", columnType, lobType);
                return true;
            }
        }
        
        Log.Debug("Column type '{ColumnType}' is NOT a LOB (checked {Count} patterns)", columnType, LobColumnTypes.Count);
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
                Log.Warning("Reached row limit of {MaxRows} for table {TableReference}, stopping extraction",
                          _maxRowsPerTable, tableReference);
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
                }
                catch (OverflowException)
                {
                    try
                    {
                        row[i] = reader.GetString(i);
                        Log.Debug("Column {Index} ({Name}) value exceeds decimal range, retrieved as string", 
                            i, reader.GetName(i));
                    }
                    catch
                    {
                        row[i] = null;
                        Log.Warning("Column {Index} ({Name}) overflow and cannot be retrieved as string, setting to NULL", 
                            i, reader.GetName(i));
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
        string orderByClause;
        int totalColumns = metadata.Columns.Count;
        
        // Count orderable columns (exclude LOBs, etc.)
        int totalOrderableColumns = metadata.Columns.Count(c => IsOrderableColumnType(c.Type));
        
        // Calculate target order columns based on table size
        // Use more columns for better uniqueness - at least half the columns, up to 6
        int baseTargetOrderColumns = totalOrderableColumns switch
        {
            <= 3 => totalOrderableColumns,          // Use all columns for tiny tables
            <= 6 => Math.Max(3, totalOrderableColumns - 1),  // 4-6 columns -> use 3-5
            <= 10 => Math.Max(4, totalOrderableColumns / 2), // 7-10 columns -> use 4-5
            <= 15 => 5,
            _ => 6
        };
        
        // Add extra columns for retry attempts, but cap at available orderable columns
        int targetOrderColumns = baseTargetOrderColumns + _extraOrderColumns;
        targetOrderColumns = Math.Min(targetOrderColumns, totalOrderableColumns); // Can't exceed orderable columns
        targetOrderColumns = Math.Max(1, targetOrderColumns); // Minimum 1 column (for tables with only 1 orderable column)
        
        if (_extraOrderColumns > 0)
        {
            if (targetOrderColumns >= totalOrderableColumns)
            {
                Log.Information("Retry attempt: Using ALL {Total} orderable columns (max reached, requested +{Extra})", 
                    totalOrderableColumns, _extraOrderColumns);
            }
            else
            {
                Log.Information("Retry attempt: Adding {ExtraColumns} extra ORDER BY columns (total target: {Target})", 
                    _extraOrderColumns, targetOrderColumns);
            }
        }
        
        Log.Debug("Table has {TotalColumns} columns ({OrderableColumns} orderable), targeting {TargetOrderColumns} order columns", 
            totalColumns, totalOrderableColumns, targetOrderColumns);
        
        // Get column statistics to choose best ORDER BY columns (high cardinality, low nulls)
        var allOrderableColumns = metadata.Columns.Where(c => IsOrderableColumnType(c.Type)).ToList();
        var columnStats = GetColumnStatistics(tableReference, allOrderableColumns);
        
        // Sort ALL orderable columns by their statistics (best first)
        var columnsSortedByStats = allOrderableColumns;
        if (columnStats.Count > 0)
        {
            columnsSortedByStats = allOrderableColumns
                .OrderBy(c =>
                {
                    var stat = columnStats.FirstOrDefault(s => s.ColumnName.Equals(c.Name, StringComparison.OrdinalIgnoreCase));
                    return stat?.NullCount ?? long.MaxValue; // Fewest nulls first
                })
                .ThenByDescending(c =>
                {
                    var stat = columnStats.FirstOrDefault(s => s.ColumnName.Equals(c.Name, StringComparison.OrdinalIgnoreCase));
                    return stat?.DistinctCount ?? 0; // Most distinct values first
                })
                .ToList();
            
            Log.Debug("Sorted columns by statistics (best first): {Columns}", 
                string.Join(", ", columnsSortedByStats.Take(5).Select(c => c.Name)));
        }
        
        var orderColumns = new List<TableMetadata.ColumnMetadata>();
        
        // Prioritize columns: PK columns first, then by statistics (best columns), then ID columns as fallback
        if (metadata.PrimaryKeyColumns.Count > 0)
        {
            foreach (var pk in metadata.PrimaryKeyColumns)
            {
                var col = metadata.Columns.FirstOrDefault(c => c.Name.Equals(pk, StringComparison.OrdinalIgnoreCase));
                if (col != null)
                {
                    orderColumns.Add(col);
                }
            }
            Log.Debug("Added {Count} PK columns to order list", orderColumns.Count);
        }
        
        // Add remaining columns sorted by statistics (best columns first)
        if (orderColumns.Count < targetOrderColumns)
        {
            var remainingColumns = columnsSortedByStats
                .Where(c => !orderColumns.Any(o => o.Name.Equals(c.Name, StringComparison.OrdinalIgnoreCase)))
                .Take(targetOrderColumns - orderColumns.Count);
            orderColumns.AddRange(remainingColumns);
            Log.Debug("Added best columns by statistics, now have {Count} order columns", orderColumns.Count);
        }
        
        if (orderColumns.Count > 0)
        {
            var safeOrderColumns = orderColumns
                .Where(c => !IsLobColumnType(c.Type) && !CouldBeLobColumn(c.Name, c.Type))
                .ToList();
            
            if (safeOrderColumns.Count < orderColumns.Count)
            {
                var excluded = orderColumns.Except(safeOrderColumns).Select(c => $"{c.Name} ({c.Type})");
                Log.Warning("Excluded {Count} LOB column(s) from ORDER BY at final stage: {Columns}", 
                    orderColumns.Count - safeOrderColumns.Count, string.Join(", ", excluded));
            }
            
            if (safeOrderColumns.Count > 0)
            {
                // Use normalization (UPPER/TRIM) only on retries for better performance
                bool useAggressiveNormalization = _extraOrderColumns > 0;
                
                var orderByParts = safeOrderColumns.Select(c => BuildOrderByExpression(c.Name, c.Type, useAggressiveNormalization));
                orderByClause = string.Join(", ", orderByParts);
                
                var pkInfo = metadata.PrimaryKeyColumns.Count > 0 ? "with PK" : "no PK";
                var strategy = useAggressiveNormalization ? "aggressive (UPPER/TRIM)" : "simple (fast)";
                Log.Information("Ordering by {Count} column(s) ({PkInfo}, {Strategy}): {OrderBy}", 
                    safeOrderColumns.Count, pkInfo, strategy, orderByClause);
            }
            else
            {
                orderByClause = "1";
                Log.Warning("All order columns were LOBs - falling back to constant ORDER BY 1");
            }
        }
        else if (metadata.Columns.Count > 0)
        {
            var firstOrderableColumn = metadata.Columns.FirstOrDefault(c => IsOrderableColumnType(c.Type));
            
            if (firstOrderableColumn != null)
            {
                bool useAggressiveNormalization = _extraOrderColumns > 0;
                orderByClause = BuildOrderByExpression(firstOrderableColumn.Name, firstOrderableColumn.Type, useAggressiveNormalization);
                Log.Warning("No orderable columns found in normal flow - using first orderable column as fallback: {OrderBy}", 
                    orderByClause);
            }
            else
            {
                orderByClause = "1";
                Log.Warning("Table has no orderable columns (all are LOB/XML/etc) - using constant ORDER BY 1");
            }
        }
        else
        {
            orderByClause = "1";
            Log.Warning("No columns available for ordering table: {Table}", tableReference);
        }

        string columnList;
        
        if (_skipLobColumns && metadata.Columns.Any(c => IsLobColumnType(c.Type)))
        {
            var nonLobColumns = metadata.Columns
                .Where(c => !IsLobColumnType(c.Type))
                .Select(c => QuoteIdentifier(c.Name));
            
            columnList = string.Join(", ", nonLobColumns);
            
            var lobCount = metadata.Columns.Count(c => IsLobColumnType(c.Type));
            Log.Information("Skipping {Count} LOB column(s) in SELECT (SKIP_LOB_COLUMNS=true)", lobCount);
        }
        else if (_lobSizeLimit > 0 && metadata.Columns.Any(c => IsLobColumnType(c.Type)))
        {

            var columnExpressions = new List<string>();
            int lobLimitedCount = 0;
            
            foreach (var col in metadata.Columns)
            {
                if (IsLobColumnType(col.Type))
                {
                    var limitedCol = BuildLobLimitedColumn(col.Name, col.Type);
                    columnExpressions.Add(limitedCol);
                    lobLimitedCount++;
                }
                else
                {
                    columnExpressions.Add(QuoteIdentifier(col.Name));
                }
            }
            
            columnList = string.Join(", ", columnExpressions);
            Log.Information("Applied LOB size limit ({Limit} bytes) to {Count} column(s)", 
                _lobSizeLimit, lobLimitedCount);
        }
        else
        {
            columnList = "*";
            
            if (metadata.Columns.Any(c => IsLobColumnType(c.Type)))
            {
                var lobCount = metadata.Columns.Count(c => IsLobColumnType(c.Type));
                Log.Information("Using SELECT * with {Count} LOB column(s) - full LOB content will be fetched", lobCount);
            }
        }

        var sql = new System.Text.StringBuilder($"SELECT {columnList} FROM {tableReference} ORDER BY {orderByClause}");

        Log.Information("Generated SQL for {Database}: {Sql}", _databaseType, sql.ToString());

        if (_maxRowsPerTable > 0)
        {
            if (_databaseType == DatabaseType.Oracle)
            {
                sql.Append($" FETCH FIRST {_maxRowsPerTable} ROWS ONLY");
            }
            else if (_databaseType == DatabaseType.PostgreSQL)
            {
                sql.Append($" LIMIT {_maxRowsPerTable}");
            }
            Log.Debug("Query with row limit: {Sql}", sql.ToString());
        }

        return sql.ToString();
    }

    private string BuildLobLimitedColumn(string columnName, string columnType)
    {
        var quotedName = QuoteIdentifier(columnName);
        var typeUpper = columnType.ToUpperInvariant();
        
        var effectiveLimit = _lobSizeLimit > 0 ? Math.Min(_lobSizeLimit, 2000) : 2000;

        if (_databaseType == DatabaseType.Oracle)
        {
            if (typeUpper.Contains("BYTE[]") || typeUpper.Contains("BYTE") ||
                typeUpper.Contains("BLOB"))
            {
                return $"DBMS_LOB.SUBSTR({quotedName}, {effectiveLimit}, 1) AS {quotedName}";
            }
            else if (typeUpper.Contains("CLOB") || typeUpper.Contains("NCLOB"))
            {
                return $"DBMS_LOB.SUBSTR({quotedName}, {effectiveLimit}, 1) AS {quotedName}";
            }
            else if (typeUpper.Contains("RAW"))
            {
                return $"SUBSTR({quotedName}, 1, {effectiveLimit}) AS {quotedName}";
            }
        }
        else if (_databaseType == DatabaseType.PostgreSQL)
        {
            if (typeUpper.Contains("BYTEA") || typeUpper.Contains("BYTE[]") || typeUpper.Contains("BYTE"))
            {
                return $"substring({quotedName} from 1 for {effectiveLimit}) AS {quotedName}";
            }
            else if (typeUpper.Contains("TEXT"))
            {
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
