using Oracle.ManagedDataAccess.Client;
using Ora2PgDataTypeValidator.Models;
using Ora2Pg.Common.Config;
using Serilog;

namespace Ora2PgDataTypeValidator.Extractors;


public class OracleColumnExtractor
{
    private readonly string _connectionString;
    private readonly HashSet<string> _columnsToSkip;

    public OracleColumnExtractor(string connectionString)
    {
        _connectionString = connectionString;

        var props = ApplicationProperties.Instance;
        string skipColumnsConfig = props.Get("ORACLE_SKIP_COLUMNS", "");

        _columnsToSkip = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(skipColumnsConfig))
        {
            var columns = skipColumnsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var column in columns)
            {
                _columnsToSkip.Add(column);
            }
            Log.Information("Columns to skip in Oracle: {Columns}", string.Join(", ", _columnsToSkip));
        }
    }

    public async Task<List<ColumnMetadata>> ExtractColumnsAsync(string schemaName, List<string>? tableNames = null)
    {
        var columns = new List<ColumnMetadata>();

        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        var sql = @"
            SELECT
                c.OWNER as SCHEMA_NAME,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.DATA_LENGTH,
                c.DATA_PRECISION,
                c.DATA_SCALE,
                c.NULLABLE,
                c.DATA_DEFAULT,
                c.CHAR_LENGTH
            FROM ALL_TAB_COLUMNS c
            INNER JOIN ALL_TABLES t ON c.OWNER = t.OWNER AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.OWNER = :schemaName";

        if (tableNames != null && tableNames.Any())
        {
            var tableList = string.Join("','", tableNames);
            sql += $" AND c.TABLE_NAME IN ('{tableList}')";
        }

        sql += " ORDER BY c.TABLE_NAME, c.COLUMN_ID";

        await using var command = new OracleCommand(sql, connection);
        command.Parameters.Add("schemaName", schemaName.ToUpper());

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
                Log.Debug("Skipping column: {ColumnName} in Oracle", columnName);
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
                IsNullable = reader.GetString(7) == "Y",
                DefaultValue = reader.IsDBNull(8) ? null : reader.GetString(8),
                CharLength = reader.IsDBNull(9) ? null : reader.GetInt32(9)
            });
        }

        if (skippedColumns > 0)
        {
            Log.Information($"Skipped {skippedColumns} column(s) in Oracle schema {schemaName}");
        }
        Log.Information($"✅ Extracted {columns.Count} columns from Oracle schema {schemaName} (after filtering)");
        return columns;
    }
}
