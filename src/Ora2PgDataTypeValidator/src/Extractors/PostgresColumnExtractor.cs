using Npgsql;
using Ora2PgDataTypeValidator.Models;
using Ora2Pg.Common.Config;
using Serilog;

namespace Ora2PgDataTypeValidator.Extractors;


public class PostgresColumnExtractor
{
    private readonly string _connectionString;
    private readonly HashSet<string> _columnsToSkip;

    public PostgresColumnExtractor(string connectionString)
    {
        _connectionString = connectionString;

        var props = ApplicationProperties.Instance;
        string skipColumnsConfig = props.Get("POSTGRES_SKIP_COLUMNS", "");

        _columnsToSkip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(skipColumnsConfig))
        {
            var columns = skipColumnsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var column in columns)
            {
                _columnsToSkip.Add(column);
            }
            Log.Information("Columns to skip in PostgreSQL: {Columns}", string.Join(", ", _columnsToSkip));
        }
    }

    public async Task<List<ColumnMetadata>> ExtractColumnsAsync(string schemaName, List<string>? tableNames = null)
    {
        var columns = new List<ColumnMetadata>();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = @"
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length
            FROM information_schema.columns c
            INNER JOIN information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE c.table_schema = @schemaName
                AND t.table_type = 'BASE TABLE'";

        if (tableNames != null && tableNames.Any())
        {
            sql += " AND c.table_name = ANY(@tableNames)";
        }

        sql += " ORDER BY c.table_name, c.ordinal_position";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schemaName", schemaName.ToLower());

        if (tableNames != null && tableNames.Any())
        {
            command.Parameters.AddWithValue("tableNames", tableNames.Select(t => t.ToLower()).ToArray());
        }

        await using var reader = await command.ExecuteReaderAsync();
        int totalColumns = 0;
        int skippedColumns = 0;

        while (await reader.ReadAsync())
        {
            totalColumns++;
            string columnName = reader.GetString(2);

            if (_columnsToSkip.Contains(columnName))
            {
                skippedColumns++;
                Log.Debug("Skipping column: {ColumnName} in PostgreSQL", columnName);
                continue;
            }

            columns.Add(new ColumnMetadata
            {
                SchemaName = reader.GetString(0),
                TableName = reader.GetString(1),
                ColumnName = columnName,
                DataType = reader.GetString(3),
                DataLength = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                DataPrecision = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                DataScale = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                IsNullable = reader.GetString(7) == "YES",
                DefaultValue = reader.IsDBNull(8) ? null : reader.GetString(8),
                CharLength = reader.IsDBNull(9) ? null : reader.GetInt32(9)
            });
        }

        if (skippedColumns > 0)
        {
            Log.Information($"Skipped {skippedColumns} column(s) in PostgreSQL schema {schemaName}");
        }
        Log.Information($"✅ Extracted {columns.Count} columns from PostgreSQL schema {schemaName} (after filtering)");
        return columns;
    }
}
