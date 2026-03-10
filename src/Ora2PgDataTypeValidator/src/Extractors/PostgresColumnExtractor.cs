using Npgsql;
using Ora2PgDataTypeValidator.Models;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Util;
using Serilog;

namespace Ora2PgDataTypeValidator.Extractors;


public class PostgresColumnExtractor
{
    private readonly string _connectionString;
    private readonly HashSet<string> _columnsToSkip;
    private readonly ObjectFilter _objectFilter;

    public PostgresColumnExtractor(string connectionString)
    {
        _connectionString = connectionString;

        var props = ApplicationProperties.Instance;
        string skipColumnsConfig = props.Get("POSTGRES_SKIP_COLUMNS", "");

        _objectFilter = ObjectFilter.FromProperties(props);

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

        if (tableNames != null && tableNames.Any())
        {
            var filteredTables = _objectFilter.FilterTables(tableNames, schemaName);
            var excludedCount = tableNames.Count - filteredTables.Count;
            if (excludedCount > 0)
            {
                Log.Information("Excluded {Count} PostgreSQL table(s) from column extraction", excludedCount);
            }

            tableNames = filteredTables;
        }

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        var sql = @"
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                -- Get ACTUAL PostgreSQL type as it appears in the database
                CASE 
                    WHEN c.data_type = 'character varying' THEN 
                        'varchar(' || COALESCE(c.character_maximum_length::text, 'max') || ')'
                    WHEN c.data_type = 'character' THEN 
                        'char(' || COALESCE(c.character_maximum_length::text, '1') || ')'
                    WHEN c.data_type = 'numeric' AND c.numeric_precision IS NOT NULL THEN
                        CASE 
                            WHEN c.numeric_scale IS NOT NULL AND c.numeric_scale > 0 THEN
                                'numeric(' || c.numeric_precision || ',' || c.numeric_scale || ')'
                            ELSE
                                'numeric(' || c.numeric_precision || ')'
                        END
                    WHEN c.data_type = 'timestamp without time zone' THEN
                        'timestamp'
                    WHEN c.data_type = 'timestamp with time zone' THEN
                        'timestamptz'
                    ELSE 
                        c.data_type
                END AS formatted_data_type,
                c.data_type AS base_data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length AS char_length
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
        var skippedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        while (await reader.ReadAsync())
        {
            totalColumns++;
            string tableName = reader.GetString(1);

            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                skippedTables.Add(tableName);
                continue;
            }

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
                TableName = tableName,
                ColumnName = columnName,
                DataType = reader.GetString(3),  // Formatted type (e.g., "varchar(100)", "numeric(15,2)")
                DataLength = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                DataPrecision = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                DataScale = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                IsNullable = reader.GetString(8) == "YES",
                DefaultValue = reader.IsDBNull(9) ? null : reader.GetString(9),
                CharLength = reader.IsDBNull(10) ? null : reader.GetInt32(10)
            });
        }

        if (skippedTables.Count > 0)
        {
            Log.Information("Skipped {Count} table(s) in PostgreSQL schema {Schema} based on table exclusion settings",
                skippedTables.Count, schemaName);
        }

        if (skippedColumns > 0)
        {
            Log.Information($"Skipped {skippedColumns} column(s) in PostgreSQL schema {schemaName}");
        }
        Log.Information($"✅ Extracted {columns.Count} columns from PostgreSQL schema {schemaName} (after filtering)");
        return columns;
    }
}
