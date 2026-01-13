using Oracle.ManagedDataAccess.Client;
using Ora2PgDataTypeValidator.Models;
using Serilog;

namespace Ora2PgDataTypeValidator.Extractors;


public class OracleColumnExtractor
{
    private readonly string _connectionString;

    public OracleColumnExtractor(string connectionString)
    {
        _connectionString = connectionString;
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
        while (await reader.ReadAsync())
        {
            columns.Add(new ColumnMetadata
            {
                SchemaName = reader.GetString(0),
                TableName = reader.GetString(1),
                ColumnName = reader.GetString(2),
                DataType = reader.GetString(3),
                DataLength = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                DataPrecision = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                DataScale = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                IsNullable = reader.GetString(7) == "Y",
                DefaultValue = reader.IsDBNull(8) ? null : reader.GetString(8),
                CharLength = reader.IsDBNull(9) ? null : reader.GetInt32(9)
            });
        }

        Log.Information($"✅ Extracted {columns.Count} columns from Oracle schema {schemaName}");
        return columns;
    }
}
