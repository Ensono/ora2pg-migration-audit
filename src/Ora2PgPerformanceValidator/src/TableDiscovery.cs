using Oracle.ManagedDataAccess.Client;
using Npgsql;
using Ora2PgPerformanceValidator.Models;
using Serilog;

namespace Ora2PgPerformanceValidator.Discovery;

public class TableDiscovery
{
    private readonly string _oracleConnectionString;
    private readonly string _postgresConnectionString;
    private readonly string _oracleSchema;
    private readonly string _postgresSchema;
    private readonly ILogger _logger = Log.ForContext<TableDiscovery>();

    public TableDiscovery(
        string oracleConnectionString,
        string postgresConnectionString,
        string oracleSchema,
        string postgresSchema)
    {
        _oracleConnectionString = oracleConnectionString;
        _postgresConnectionString = postgresConnectionString;
        _oracleSchema = oracleSchema;
        _postgresSchema = postgresSchema;
    }

    public async Task<List<TableInfo>> DiscoverOracleTablesAsync()
    {
        _logger.Information("Discovering tables from Oracle schema: {Schema}", _oracleSchema);
        
        var tables = new List<TableInfo>();

        var query = @"
            SELECT 
                t.table_name,
                cc.column_name as pk_column,
                cols.data_type as pk_data_type
            FROM all_tables t
            LEFT JOIN all_constraints tc 
                ON t.owner = tc.owner 
                AND t.table_name = tc.table_name 
                AND tc.constraint_type = 'P'
            LEFT JOIN all_cons_columns cc 
                ON tc.constraint_name = cc.constraint_name 
                AND tc.owner = cc.owner
                AND cc.position = 1
            LEFT JOIN all_tab_columns cols
                ON t.owner = cols.owner
                AND t.table_name = cols.table_name
                AND cc.column_name = cols.column_name
            WHERE t.owner = :schema
            ORDER BY t.table_name";

        await using var conn = new OracleConnection(_oracleConnectionString);
        await conn.OpenAsync();
        
        await using var cmd = new OracleCommand(query, conn);
        cmd.Parameters.Add("schema", _oracleSchema);
        
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var table = new TableInfo
            {
                Name = reader.GetString(0),
                PrimaryKey = reader.IsDBNull(1) ? null : new PrimaryKeyInfo
                {
                    Column = reader.GetString(1),
                    DataType = reader.GetString(2)
                }
            };
            
            tables.Add(table);
        }

        _logger.Information("Discovered {Count} tables from Oracle", tables.Count);
        return tables;
    }

    public async Task<List<TableInfo>> DiscoverPostgresTablesAsync()
    {
        _logger.Information("Discovering tables from PostgreSQL schema: {Schema}", _postgresSchema);
        
        var tables = new List<TableInfo>();

        var query = @"
            SELECT t.table_name,
                   kcu.column_name as pk_column,
                   c.data_type as pk_data_type
            FROM information_schema.tables t
            LEFT JOIN information_schema.table_constraints tc
                ON t.table_schema = tc.table_schema
                AND t.table_name = tc.table_name
                AND tc.constraint_type = 'PRIMARY KEY'
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND kcu.ordinal_position = 1
            LEFT JOIN information_schema.columns c
                ON kcu.table_schema = c.table_schema
                AND kcu.table_name = c.table_name
                AND kcu.column_name = c.column_name
            WHERE t.table_schema = $1
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name";

        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();
        
        await using var cmd = new NpgsqlCommand(query, conn);
        cmd.Parameters.AddWithValue(_postgresSchema);
        
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var table = new TableInfo
            {
                Name = reader.GetString(0),
                PrimaryKey = reader.IsDBNull(1) ? null : new PrimaryKeyInfo
                {
                    Column = reader.GetString(1),
                    DataType = reader.GetString(2)
                }
            };
            
            tables.Add(table);
        }

        _logger.Information("Discovered {Count} tables from PostgreSQL", tables.Count);
        return tables;
    }

    public async Task<string?> GetSamplePrimaryKeyValueAsync(string tableName, PrimaryKeyInfo pk)
    {
        try
        {
            var query = $"SELECT {pk.Column} FROM {_oracleSchema}.{tableName} WHERE ROWNUM = 1";
            
            await using var conn = new OracleConnection(_oracleConnectionString);
            await conn.OpenAsync();
            
            await using var cmd = new OracleCommand(query, conn);
            var value = await cmd.ExecuteScalarAsync();
            
            if (value != null && value != DBNull.Value)
            {
                // Format based on data type
                if (pk.DataType.Contains("VARCHAR") || 
                    pk.DataType.Contains("CHAR") ||
                    pk.DataType.Contains("DATE") ||
                    pk.DataType.Contains("TIMESTAMP"))
                {
                    return $"'{value}'";
                }
                return value.ToString();
            }
        }
        catch (Exception ex)
        {
            _logger.Warning("Could not get sample PK value for {Table}.{Column}: {Error}", 
                tableName, pk.Column, ex.Message);
        }
        
        return null;
    }
}
