using Npgsql;
using Ora2PgRowCountValidator.Models;
using Serilog;
using Ora2Pg.Common.Util;

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

        var partitionMap = await GetPartitionMapAsync(connection, schemaName);
        var partitionChildren = partitionMap.Values.SelectMany(p => p).ToHashSet(StringComparer.OrdinalIgnoreCase);

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

        var objectFilter = ObjectFilter.FromProperties();
        var filteredTables = objectFilter.FilterTables(tableNames, schemaName);
        filteredTables = filteredTables
            .Where(t => !partitionChildren.Contains(t))
            .ToList();
        var excludedCount = tableNames.Count - filteredTables.Count;
        if (excludedCount > 0)
        {
            Log.Information("Excluded {Count} PostgreSQL table(s) from row count extraction", excludedCount);
        }

        tableNames = filteredTables;

        Log.Information($"Found {tableNames.Count} tables in PostgreSQL schema {schemaName}");

        foreach (var tableName in tableNames)
        {
            try
            {
                var countQuery = $"SELECT COUNT(*) FROM {schemaName.ToLower()}.{tableName}";
                using var cmd = new NpgsqlCommand(countQuery, connection);
                var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());

                var tableRowCount = new TableRowCount
                {
                    SchemaName = schemaName.ToLower(),
                    TableName = tableName,
                    RowCount = count,
                    IsPartitioned = partitionMap.ContainsKey(tableName)
                };

                if (tableRowCount.IsPartitioned)
                {
                    var partitionRows = await GetPartitionRowCountsAsync(connection, schemaName, partitionMap[tableName]);
                    tableRowCount.PartitionRowCounts = partitionRows;
                }

                rowCounts.Add(tableRowCount);

                Log.Debug($"PostgreSQL: {tableName} = {count:N0} rows");
            }
            catch (Exception ex)
            {
                Log.Warning($"Failed to get row count for {schemaName}.{tableName}: {ex.Message}");

                rowCounts.Add(new TableRowCount
                {
                    SchemaName = schemaName.ToLower(),
                    TableName = tableName,
                    RowCount = -1,
                    IsPartitioned = partitionMap.ContainsKey(tableName)
                });
            }
        }
        return rowCounts;
    }

    private async Task<Dictionary<string, List<string>>> GetPartitionMapAsync(NpgsqlConnection connection, string schemaName)
    {
        var partitions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        var query = @"
            SELECT parent.relname as parent_table,
                   child.relname as partition_table
            FROM pg_catalog.pg_partitioned_table part
            JOIN pg_catalog.pg_class parent ON parent.oid = part.partrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = parent.relnamespace
            LEFT JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parent.oid
            LEFT JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
            WHERE n.nspname = @schemaName
            ORDER BY parent.relname, child.relname";

        using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddWithValue("schemaName", schemaName.ToLower());

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var parent = reader.GetString(0);
            if (!partitions.TryGetValue(parent, out var children))
            {
                children = new List<string>();
                partitions[parent] = children;
            }

            if (!reader.IsDBNull(1))
            {
                children.Add(reader.GetString(1));
            }
        }

        return partitions;
    }

    private async Task<List<PartitionRowCount>> GetPartitionRowCountsAsync(
        NpgsqlConnection connection,
        string schemaName,
        List<string> partitionTables)
    {
        var partitionCounts = new List<PartitionRowCount>();

        foreach (var partition in partitionTables)
        {
            try
            {
                var countQuery = $"SELECT COUNT(*) FROM {schemaName.ToLower()}.{partition}";
                using var cmd = new NpgsqlCommand(countQuery, connection);
                var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());

                partitionCounts.Add(new PartitionRowCount
                {
                    PartitionName = partition,
                    RowCount = count
                });
            }
            catch (Exception ex)
            {
                Log.Warning($"Failed to get row count for {schemaName}.{partition}: {ex.Message}");
                partitionCounts.Add(new PartitionRowCount
                {
                    PartitionName = partition,
                    RowCount = -1
                });
            }
        }

        return partitionCounts;
    }
}
