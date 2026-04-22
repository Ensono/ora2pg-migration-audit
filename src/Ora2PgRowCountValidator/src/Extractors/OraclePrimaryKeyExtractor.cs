using Oracle.ManagedDataAccess.Client;
using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.Extractors;



public class OraclePrimaryKeyExtractor
{
    private readonly string _connectionString;

    public OraclePrimaryKeyExtractor(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<PrimaryKeyInfo> GetPrimaryKeyAsync(string schemaName, string tableName)
    {
        var pkInfo = new PrimaryKeyInfo { TableName = tableName };

        try
        {
            using var connection = new OracleConnection(_connectionString);
            await connection.OpenAsync();

            var query = @"
                SELECT acc.column_name
                FROM all_constraints ac
                JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name 
                    AND ac.owner = acc.owner
                WHERE ac.owner = :schemaName
                    AND ac.table_name = :tableName
                    AND ac.constraint_type = 'P'
                ORDER BY acc.position";

            using var command = new OracleCommand(query, connection);
            command.Parameters.Add(":schemaName", OracleDbType.Varchar2).Value = schemaName.ToUpper();
            command.Parameters.Add(":tableName", OracleDbType.Varchar2).Value = tableName.ToUpper();

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                pkInfo.PrimaryKeyColumns.Add(reader.GetString(0));
            }

            if (pkInfo.HasPrimaryKey)
            {
                Log.Debug($"Found PK for {tableName}: {pkInfo.PrimaryKeyColumnsString}");
            }
            else
            {
                Log.Debug($"No primary key found for {tableName}");
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed to get primary key for {tableName}");
        }

        return pkInfo;
    }


    public async Task<List<MissingRowInfo>> FindMissingRowsAsync(
        string schemaName, 
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        int limit = 100)
    {
        var allRows = new List<MissingRowInfo>();

        if (!pkInfo.HasPrimaryKey)
        {
            Log.Debug($"Cannot identify missing rows for {tableName} - no primary key");
            return allRows;
        }

        try
        {
            using var connection = new OracleConnection(_connectionString);
            await connection.OpenAsync();

            var pkColumns = string.Join(", ", pkInfo.PrimaryKeyColumns);

            var query = $@"
                SELECT {pkColumns}
                FROM {schemaName}.{tableName}
                WHERE ROWNUM <= :limit
                ORDER BY {pkColumns}";

            using var command = new OracleCommand(query, connection);
            command.Parameters.Add(":limit", OracleDbType.Int32).Value = limit;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var rowInfo = new MissingRowInfo { TableName = tableName };
                
                for (int i = 0; i < pkInfo.PrimaryKeyColumns.Count; i++)
                {
                    var columnName = pkInfo.PrimaryKeyColumns[i];
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    rowInfo.PrimaryKeyValues[columnName] = value;
                }
                allRows.Add(rowInfo);
            }

            Log.Debug($"Retrieved {allRows.Count} rows from Oracle {tableName} for comparison");
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed to retrieve sample rows from Oracle {tableName}");
        }

        return allRows;
    }


    public async Task<HashSet<string>> BulkRowExistsAsync(
        string schemaName,
        string tableName,
        PrimaryKeyInfo pkInfo,
        List<Dictionary<string, object?>> pkValuesList)
    {
        var found = new HashSet<string>();
        if (!pkInfo.HasPrimaryKey || pkValuesList.Count == 0) return found;

        try
        {
            using var connection = new OracleConnection(_connectionString);
            await connection.OpenAsync();

            if (pkInfo.PrimaryKeyColumns.Count == 1)
            {
                var col = pkInfo.PrimaryKeyColumns[0];
                var values = pkValuesList.Select(r => r.GetValueOrDefault(col)).ToList();
                
                foreach (var chunk in values.Chunk(999))
                {
                    var paramNames = chunk.Select((_, i) => $":p{i}").ToList();
                    var query = $"SELECT {col} FROM {schemaName}.{tableName} WHERE {col} IN ({string.Join(",", paramNames)})";
                    
                    using var command = new OracleCommand(query, connection);
                    for (int i = 0; i < chunk.Length; i++)
                        command.Parameters.Add($":p{i}", chunk[i] ?? DBNull.Value);
                    
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                        found.Add(reader.GetValue(0)?.ToString() ?? "NULL");
                }
            }
            else
            {
                var cols = pkInfo.PrimaryKeyColumns;

                foreach (var chunk in pkValuesList.Chunk(500))
                {
                    var unionRows = chunk.Select((row, rowIdx) =>
                    {
                        var selectCols = cols.Select((col, colIdx) => $":r{rowIdx}c{colIdx} AS \"{col}\"");
                        return $"SELECT {string.Join(", ", selectCols)} FROM DUAL";
                    });

                    var joinConditions = cols.Select(col => $"t.\"{col}\" = v.\"{col}\"");

                    var selectCols2 = cols.Select(col => $"t.\"{col}\"");
                    var query = $"SELECT {string.Join(", ", selectCols2)} " +
                                $"FROM {schemaName}.{tableName} t " +
                                $"JOIN ({string.Join(" UNION ALL ", unionRows)}) v " +
                                $"ON {string.Join(" AND ", joinConditions)}";

                    using var command = new OracleCommand(query, connection);
                    command.FetchSize = command.FetchSize * 2;

                    for (int rowIdx = 0; rowIdx < chunk.Length; rowIdx++)
                    {
                        var row = chunk[rowIdx];
                        for (int colIdx = 0; colIdx < cols.Count; colIdx++)
                        {
                            var colName = cols[colIdx];
                            command.Parameters.Add($":r{rowIdx}c{colIdx}", row.GetValueOrDefault(colName) ?? DBNull.Value);
                        }
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var key = string.Join("|", cols.Select((col, i) => reader.IsDBNull(i) ? "NULL" : reader.GetValue(i)?.ToString() ?? "NULL"));
                        found.Add(key);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed bulk existence check in Oracle {tableName}");
        }

        return found;
    }

    public async Task<bool> RowExistsAsync(
        string schemaName, 
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        Dictionary<string, object?> pkValues)
    {
        var result = await BulkRowExistsAsync(schemaName, tableName, pkInfo, new List<Dictionary<string, object?>> { pkValues });
        return result.Count > 0;
    }
}
