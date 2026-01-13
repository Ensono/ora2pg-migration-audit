using Npgsql;
using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.Extractors;

public class PostgresRowCountExtractor
{
    private readonly string _connectionString;

    public PostgresRowCountExtractor(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<List<TableRowCount>> ExtractRowCountsAsync(string schemaName)
    {
        var rowCounts = new List<TableRowCount>();

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var tableQuery = @"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = @schemaName
            AND table_type = 'BASE TABLE'
            ORDER BY table_name";

        var tableNames = new List<string>();
        using (var cmd = new NpgsqlCommand(tableQuery, connection))
        {
            cmd.Parameters.AddWithValue("schemaName", schemaName.ToLower());
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tableNames.Add(reader.GetString(0));
            }
        }

        Log.Information($"Found {tableNames.Count} tables in PostgreSQL schema {schemaName}");

        foreach (var tableName in tableNames)
        {
            try
            {
                var countQuery = $"SELECT COUNT(*) FROM {schemaName.ToLower()}.{tableName}";
                using var cmd = new NpgsqlCommand(countQuery, connection);
                var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());

                rowCounts.Add(new TableRowCount
                {
                    SchemaName = schemaName.ToLower(),
                    TableName = tableName,
                    RowCount = count
                });

                Log.Debug($"PostgreSQL: {tableName} = {count:N0} rows");
            }
            catch (Exception ex)
            {
                Log.Warning($"Failed to get row count for {schemaName}.{tableName}: {ex.Message}");

                rowCounts.Add(new TableRowCount
                {
                    SchemaName = schemaName.ToLower(),
                    TableName = tableName,
                    RowCount = -1
                });
            }
        }
        return rowCounts;
    }
}
