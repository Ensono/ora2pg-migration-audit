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


    public async Task<bool> RowExistsAsync(
        string schemaName, 
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        Dictionary<string, object?> pkValues)
    {
        if (!pkInfo.HasPrimaryKey) return false;

        try
        {
            using var connection = new OracleConnection(_connectionString);
            await connection.OpenAsync();

            var whereConditions = pkInfo.PrimaryKeyColumns
                .Select((col, idx) => $"{col} = :pk{idx}")
                .ToList();

            var query = $@"
                SELECT COUNT(*)
                FROM {schemaName}.{tableName}
                WHERE {string.Join(" AND ", whereConditions)}";

            using var command = new OracleCommand(query, connection);
            
            for (int i = 0; i < pkInfo.PrimaryKeyColumns.Count; i++)
            {
                var columnName = pkInfo.PrimaryKeyColumns[i];
                var value = pkValues.GetValueOrDefault(columnName);
                command.Parameters.Add($":pk{i}", value ?? DBNull.Value);
            }

            var count = Convert.ToInt64(await command.ExecuteScalarAsync());
            return count > 0;
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed to check row existence in Oracle {tableName}");
            return false;
        }
    }
}
