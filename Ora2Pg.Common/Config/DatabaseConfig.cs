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
        return $"Data Source={Host}:{Port}/{ServiceOrDatabase};User Id={Username};Password={Password};";
    }

    public string GetPostgresConnectionString()
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = Host,
            Port = Port,
            Database = ServiceOrDatabase,
            Username = Username,
            Password = Password
        };
        return builder.ConnectionString;
    }
}
