using Npgsql;

namespace Ora2Pg.Common.Config;

public class DatabaseConfig
{
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }
    public string ServiceOrDatabase { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;

    public static DatabaseConfig CreateOracleConfig(ApplicationProperties props)
    {
        return new DatabaseConfig
        {
            Host = props.Get("ORACLE_HOST", "localhost"),
            Port = props.GetInt("ORACLE_PORT", 1521),
            ServiceOrDatabase = props.Get("ORACLE_SERVICE", "XEPDB1"),
            Username = props.Get("ORACLE_USER", "system"),
            Password = props.Get("ORACLE_PASSWORD", string.Empty)
        };
    }

    public static DatabaseConfig CreatePostgresConfig(ApplicationProperties props)
    {
        return new DatabaseConfig
        {
            Host = props.Get("POSTGRES_HOST", "localhost"),
            Port = props.GetInt("POSTGRES_PORT", 5432),
            ServiceOrDatabase = props.Get("POSTGRES_DB", "postgres"),
            Username = props.Get("POSTGRES_USER", "postgres"),
            Password = props.Get("POSTGRES_PASSWORD", string.Empty)
        };
    }

    public string GetOracleConnectionString()
    {
        var props = ApplicationProperties.Instance;
        var poolSize = props.GetInt("CONNECTION_POOL_SIZE", 10);
        var commandTimeout = props.GetInt("COMMAND_TIMEOUT_SECONDS", 300);
        
        // Oracle connection pooling settings
        // Min Pool Size: minimum connections to keep open
        // Max Pool Size: maximum connections allowed (should be >= PARALLEL_TABLES * PARALLEL_SCHEMAS * 2)
        // Connection Timeout: how long to wait for a connection from pool
        // Incr Pool Size: how many connections to add when pool is exhausted
        return $"Data Source={Host}:{Port}/{ServiceOrDatabase};" +
               $"User Id={Username};Password={Password};" +
               $"Min Pool Size=1;Max Pool Size={poolSize};" +
               $"Connection Timeout=60;Incr Pool Size=2;" +
               $"Pooling=true;";
    }

    public string GetPostgresConnectionString()
    {
        var props = ApplicationProperties.Instance;
        var poolSize = props.GetInt("CONNECTION_POOL_SIZE", 10);
        var commandTimeout = props.GetInt("COMMAND_TIMEOUT_SECONDS", 300);
        
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = Host,
            Port = Port,
            Database = ServiceOrDatabase,
            Username = Username,
            Password = Password,
            // PostgreSQL connection pooling settings
            MinPoolSize = 1,
            MaxPoolSize = poolSize,
            ConnectionIdleLifetime = 300,  // Close idle connections after 5 minutes
            Timeout = 60,                   // Connection timeout in seconds
            CommandTimeout = commandTimeout,
            Pooling = true
        };
        return builder.ConnectionString;
    }
}
