using Oracle.ManagedDataAccess.Client;
using Ora2PgRowCountValidator.Models;
using Serilog;
using Ora2Pg.Common.Util;

namespace Ora2PgRowCountValidator.Extractors;


public class OracleRowCountExtractor
{
    private readonly string _connectionString;

    public OracleRowCountExtractor(string connectionString)
    {
        _connectionString = connectionString;
    }


    public async Task<List<TableRowCount>> ExtractRowCountsAsync(string schemaName)
    {
        var rowCounts = new List<TableRowCount>();

        using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        var tableQuery = @"
            SELECT table_name
            FROM all_tables
            WHERE owner = :schemaName
            AND table_name NOT LIKE 'BIN$%'
            ORDER BY table_name";

        var tableNames = new List<string>();
        using (var cmd = new OracleCommand(tableQuery, connection))
        {
            cmd.Parameters.Add("schemaName", OracleDbType.Varchar2).Value = schemaName.ToUpper();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tableNames.Add(reader.GetString(0));
            }
        }

        var objectFilter = ObjectFilter.FromProperties();
        var filteredTables = objectFilter.FilterTables(tableNames, schemaName);
        var excludedCount = tableNames.Count - filteredTables.Count;
        if (excludedCount > 0)
        {
            Log.Information("Excluded {Count} Oracle table(s) from row count extraction", excludedCount);
        }

        tableNames = filteredTables;

        Log.Information($"Found {tableNames.Count} tables in Oracle schema {schemaName}");


        foreach (var tableName in tableNames)
        {
            try
            {
                var countQuery = $"SELECT COUNT(*) FROM {schemaName.ToUpper()}.{tableName}";
                using var cmd = new OracleCommand(countQuery, connection);
                var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());

                rowCounts.Add(new TableRowCount
                {
                    SchemaName = schemaName.ToUpper(),
                    TableName = tableName,
                    RowCount = count
                });

                Log.Debug($"Oracle: {tableName} = {count:N0} rows");
            }
            catch (Exception ex)
            {
                Log.Warning($"Failed to get row count for {schemaName}.{tableName}: {ex.Message}");

                rowCounts.Add(new TableRowCount
                {
                    SchemaName = schemaName.ToUpper(),
                    TableName = tableName,
                    RowCount = -1
                });
            }
        }
        return rowCounts;
    }
}
