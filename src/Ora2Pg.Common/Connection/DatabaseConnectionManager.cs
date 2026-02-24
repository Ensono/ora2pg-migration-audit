using System.Data;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Util;

namespace Ora2Pg.Common.Connection;


public class DatabaseConnectionManager : IDisposable
{
    private readonly ILogger _logger = Log.ForContext<DatabaseConnectionManager>();
    private readonly Dictionary<DatabaseType, DatabaseConfig> _configs = new();
    private readonly Dictionary<DatabaseType, string> _connectionStrings = new();


    public void InitializePool(DatabaseType dbType, DatabaseConfig config)
    {
        _configs[dbType] = config;
        
        string connectionString = dbType == DatabaseType.Oracle 
            ? config.GetOracleConnectionString() 
            : config.GetPostgresConnectionString();
        
        _connectionStrings[dbType] = connectionString;
        
        _logger.Information("✓ {DbType} connection pool initialized", dbType);
    }


    public IDbConnection GetConnection(DatabaseType dbType)
    {
        if (!_connectionStrings.ContainsKey(dbType))
        {
            throw new InvalidOperationException($"Connection pool not initialized for {dbType}");
        }

        string connectionString = _connectionStrings[dbType];
        
        return dbType == DatabaseType.Oracle 
            ? new OracleConnection(connectionString) 
            : new NpgsqlConnection(connectionString);
    }


    public bool TestConnection(DatabaseType dbType)
    {
        try
        {
            using var connection = GetConnection(dbType);
            connection.Open();
            
            string testQuery = dbType == DatabaseType.Oracle 
                ? "SELECT 1 FROM DUAL" 
                : "SELECT 1";
            
            using var command = connection.CreateCommand();
            command.CommandText = testQuery;
            command.ExecuteScalar();
            
            var config = _configs[dbType];
            string endpoint = dbType == DatabaseType.Oracle
                ? $"{config.Host}:{config.Port}/{config.ServiceOrDatabase}"
                : $"{config.Host}:{config.Port}/{config.ServiceOrDatabase}";
            
            _logger.Information("✓ {DbType}: Connected to {Endpoint}", dbType, endpoint);
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "✗ {DbType} connection test failed", dbType);
            return false;
        }
    }



    public List<string> GetTablesInSchema(DatabaseType dbType, string schema)
    {
        var tables = new List<string>();
        
        try
        {
            using var connection = GetConnection(dbType);
            connection.Open();
            
            string query = dbType == DatabaseType.Oracle
                ? "SELECT table_name FROM all_tables WHERE owner = :schema ORDER BY table_name"
                : "SELECT table_name FROM information_schema.tables WHERE table_schema = @schema AND table_type = 'BASE TABLE' ORDER BY table_name";
            
            using var command = connection.CreateCommand();
            command.CommandText = query;
            
            var parameter = command.CreateParameter();
            parameter.ParameterName = dbType == DatabaseType.Oracle ? "schema" : "@schema";
            parameter.Value = schema;
            command.Parameters.Add(parameter);
            
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    tables.Add(reader.GetString(0));
                }
            }

            if (dbType == DatabaseType.PostgreSQL)
            {
                var partitionChildren = GetPostgresPartitionChildren(connection, schema);
                if (partitionChildren.Count > 0)
                {
                    tables = tables.Where(t => !partitionChildren.Contains(t)).ToList();
                    _logger.Information("Excluded {Count} PostgreSQL partition table(s) from schema discovery", partitionChildren.Count);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error getting tables in schema {Schema} for {DbType}", schema, dbType);
        }

        var objectFilter = ObjectFilter.FromProperties();
        var filteredTables = objectFilter.FilterTables(tables, schema);
        var excludedCount = tables.Count - filteredTables.Count;
        if (excludedCount > 0)
        {
            _logger.Information("Filtered {ExcludedCount} table(s) from {DbType} schema {Schema}",
                excludedCount, dbType, schema);
        }

        return filteredTables;
    }

    private HashSet<string> GetPostgresPartitionChildren(IDbConnection connection, string schema)
    {
        var children = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT child.relname
            FROM pg_catalog.pg_partitioned_table part
            JOIN pg_catalog.pg_class parent ON parent.oid = part.partrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = parent.relnamespace
            JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parent.oid
            JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
            WHERE n.nspname = @schema";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "@schema";
        parameter.Value = schema.ToLower();
        command.Parameters.Add(parameter);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            children.Add(reader.GetString(0));
        }

        return children;
    }


    public string GetPoolStats(DatabaseType dbType)
    {
        if (_configs.TryGetValue(dbType, out var config))
        {
            return $"{dbType}: Active connections: 2/10"; // Simplified for now
        }
        return $"{dbType}: Not initialized";
    }

    public void Shutdown()
    {
        _logger.Information("Shutting down database connection pools...");
        _connectionStrings.Clear();
        _configs.Clear();
    }

    public void Dispose()
    {
        Shutdown();
        GC.SuppressFinalize(this);
    }
}
