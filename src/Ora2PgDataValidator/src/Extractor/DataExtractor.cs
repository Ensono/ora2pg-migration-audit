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

    public DataExtractor(IDbConnection connection, DatabaseType databaseType)
    {
        _connection = connection;
        _databaseType = databaseType;

        var props = ApplicationProperties.Instance;
        _maxRowsPerTable = props.GetInt("MAX_ROWS_PER_TABLE",
                                       props.GetInt("max.rows.per.table", 0));

        _commandTimeoutSeconds = props.GetInt("COMMAND_TIMEOUT_SECONDS",
                                             props.GetInt("command.timeout.seconds", 300));

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
                foreach (DataRow row in schemaTable.Rows)
                {
                    string columnName = row["ColumnName"].ToString() ?? "";
                    string columnType = row["DataType"].ToString() ?? "";
                    bool isKey = row["IsKey"] != DBNull.Value && (bool)row["IsKey"];

                    if (_columnsToSkip.Contains(columnName))
                    {
                        skippedCount++;
                        Log.Debug("Skipping column: {ColumnName} in table {TableReference}", columnName, tableReference);
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
                    Log.Information("Skipped {Count} column(s) in table {TableReference}", skippedCount, tableReference);
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
                row[i] = reader.GetValue(i);
                if (row[i] == DBNull.Value)
                {
                    row[i] = null;
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


    private string BuildSelectQuery(string tableReference, TableMetadata metadata)
    {
        string orderByClause;
        if (metadata.PrimaryKeyColumns.Count > 0)
        {
            orderByClause = string.Join(", ", metadata.PrimaryKeyColumns.Select(pk => $"{QuoteIdentifier(pk)} DESC"));
            Log.Debug("Ordering by primary key columns (DESC): {OrderBy}", orderByClause);
        }
        else
        {
            var idColumn = metadata.Columns.FirstOrDefault(c =>
                c.Name.Equals("id", StringComparison.OrdinalIgnoreCase) ||
                c.Name.Equals("ID", StringComparison.OrdinalIgnoreCase) ||
                c.Name.EndsWith("_id", StringComparison.OrdinalIgnoreCase) ||
                c.Name.EndsWith("_ID", StringComparison.OrdinalIgnoreCase));
            
            if (idColumn != null)
            {
                orderByClause = $"{QuoteIdentifier(idColumn.Name)} DESC";
                Log.Debug("No primary key found - ordering by ID column (DESC): {OrderBy}", orderByClause);
            }
            else if (metadata.Columns.Count > 0)
            {
                orderByClause = $"{QuoteIdentifier(metadata.Columns[0].Name)} DESC";
                Log.Debug("No primary key or ID column - ordering by first column (DESC): {OrderBy}", orderByClause);
            }
            else
            {
                orderByClause = "1";
                Log.Warning("No columns available for ordering table: {Table}", tableReference);
            }
        }

        string columnList;
        if (metadata.Columns.Count > 0)
        {
            columnList = string.Join(", ", metadata.Columns.Select(c => QuoteIdentifier(c.Name)));
        }
        else
        {
            columnList = "*";
        }

        var sql = new System.Text.StringBuilder($"SELECT {columnList} FROM {tableReference} ORDER BY {orderByClause}");

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

    private string QuoteIdentifier(string identifier)
    {
        if (_databaseType == DatabaseType.Oracle)
        {
            return $"\"{identifier}\"";
        }
        else if (_databaseType == DatabaseType.PostgreSQL)
        {
            return $"\"{identifier}\"";
        }
        return identifier;
    }
}
