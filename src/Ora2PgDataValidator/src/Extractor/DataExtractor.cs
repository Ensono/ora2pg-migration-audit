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

    public DataExtractor(IDbConnection connection, DatabaseType databaseType)
    {
        _connection = connection;
        _databaseType = databaseType;

        var props = ApplicationProperties.Instance;
        _maxRowsPerTable = props.GetInt("MAX_ROWS_PER_TABLE", 
                                       props.GetInt("max.rows.per.table", 0));

        _commandTimeoutSeconds = props.GetInt("COMMAND_TIMEOUT_SECONDS",
                                             props.GetInt("command.timeout.seconds", 300));

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
        
        using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
        var schemaTable = reader.GetSchemaTable();

        if (schemaTable != null)
        {
            int position = 1;
            foreach (DataRow row in schemaTable.Rows)
            {
                string columnName = row["ColumnName"].ToString() ?? "";
                string columnType = row["DataType"].ToString() ?? "";
                bool isKey = row["IsKey"] != DBNull.Value && (bool)row["IsKey"];

                columns.Add(new TableMetadata.ColumnMetadata(columnName, columnType, position));

                if (isKey)
                {
                    primaryKeyColumns.Add(columnName);
                }

                position++;
            }
        }

        if (primaryKeyColumns.Count == 0 && columns.Count > 0)
        {
            primaryKeyColumns.AddRange(columns.Select(c => c.Name));
            Log.Warning("No primary key found for table {TableReference}, will order by all columns for consistency",
                       tableReference);
        }

        Log.Information("Found {ColumnCount} columns in table {TableReference}", columns.Count, tableReference);
        if (primaryKeyColumns.Count > 0)
        {
            Log.Information("Order by columns: {OrderColumns}", string.Join(", ", primaryKeyColumns));
        }
        
        return new TableMetadata(tableReference, columns, primaryKeyColumns);
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
            orderByClause = string.Join(", ", metadata.PrimaryKeyColumns);
        }
        else
        {
            orderByClause = "1";
        }
        
        var sql = new System.Text.StringBuilder($"SELECT * FROM {tableReference} ORDER BY {orderByClause}");

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
}
