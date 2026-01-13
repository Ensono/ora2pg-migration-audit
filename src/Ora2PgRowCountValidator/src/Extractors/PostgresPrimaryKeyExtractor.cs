using Npgsql;
using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.Extractors;


public class PostgresPrimaryKeyExtractor
{
    private readonly string _connectionString;

    public PostgresPrimaryKeyExtractor(string connectionString)
    {
        _connectionString = connectionString;
    }


    public async Task<PrimaryKeyInfo> GetPrimaryKeyAsync(string schemaName, string tableName)
    {
        var pkInfo = new PrimaryKeyInfo { TableName = tableName };

        try
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var query = @"
                SELECT a.attname AS column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary
                    AND n.nspname = @schemaName
                    AND c.relname = @tableName
                ORDER BY array_position(i.indkey, a.attnum)";

            using var command = new NpgsqlCommand(query, connection);
            command.Parameters.AddWithValue("schemaName", schemaName.ToLower());
            command.Parameters.AddWithValue("tableName", tableName.ToLower());

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


    public async Task<List<MissingRowInfo>> FindExtraRowsAsync(
        string schemaName, 
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        int limit = 100)
    {
        var allRows = new List<MissingRowInfo>();

        if (!pkInfo.HasPrimaryKey)
        {
            Log.Debug($"Cannot identify extra rows for {tableName} - no primary key");
            return allRows;
        }

        try
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var pgTableName = tableName.ToLower();
            var pgSchemaName = schemaName.ToLower();

            var pkColumns = string.Join(", ", pkInfo.PrimaryKeyColumns.Select(c => $"\"{c.ToLower()}\""));

            var query = $@"
                SELECT {pkColumns}
                FROM ""{pgSchemaName}"".""{pgTableName}""
                ORDER BY {pkColumns}
                LIMIT {limit}";

            using var command = new NpgsqlCommand(query, connection);

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

            Log.Debug($"Retrieved {allRows.Count} rows from PostgreSQL {tableName} for comparison");
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed to retrieve sample rows from PostgreSQL {tableName}");
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
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var pgTableName = tableName.ToLower();
            var pgSchemaName = schemaName.ToLower();

            var whereConditions = pkInfo.PrimaryKeyColumns
                .Select((col, idx) => $"\"{col.ToLower()}\" = @pk{idx}")
                .ToList();

            var query = $@"
                SELECT COUNT(*)
                FROM ""{pgSchemaName}"".""{pgTableName}""
                WHERE {string.Join(" AND ", whereConditions)}";

            using var command = new NpgsqlCommand(query, connection);
            
            for (int i = 0; i < pkInfo.PrimaryKeyColumns.Count; i++)
            {
                var columnName = pkInfo.PrimaryKeyColumns[i];
                var value = pkValues.GetValueOrDefault(columnName);
                command.Parameters.AddWithValue($"@pk{i}", value ?? DBNull.Value);
            }

            var count = Convert.ToInt64(await command.ExecuteScalarAsync());
            return count > 0;
        }
        catch (Exception ex)
        {
            Log.Warning(ex, $"Failed to check row existence in PostgreSQL {tableName}");
            return false;
        }
    }
}
