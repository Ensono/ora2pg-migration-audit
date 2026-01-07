using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataValidator.Processor;
using Ora2Pg.Common.Util;

namespace Ora2PgDataValidator;

class Program
{
    static void Main(string[] args)
    {
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Console(outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
            .WriteTo.File("logs/application-.log", 
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: 30,
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger();

        try
        {
            Log.Information(new string('=', 80));
            Log.Information("ORACLE TO POSTGRESQL MIGRATION AUDIT - Data Fingerprinting Validation");
            Log.Information(new string('=', 80));

            using var connectionManager = new DatabaseConnectionManager();

            Log.Information("");
            Log.Information("1. Loading configuration...");
            var props = ApplicationProperties.Instance;
            Log.Information("✓ Configuration loaded from .env");

            bool extractSingleDb = props.GetBool("EXTRACT_SINGLE_DB", props.GetBool("extract.single.db", false));

            if (extractSingleDb)
            {
                RunSingleDatabaseMode(connectionManager, props);
            }
            else
            {
                RunComparisonMode(connectionManager, props);
            }

            Log.Information("");
            Log.Information("✓ Migration validation completed successfully");
            Log.Information("  Check the reports/ folder for generated files");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "✗ Application failed");
            Environment.Exit(1);
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    
    static void RunSingleDatabaseMode(DatabaseConnectionManager connectionManager, ApplicationProperties props)
    {
        Log.Information("");
        Log.Information("2. MODE: Single Database Extraction");

        string targetDbStr = props.Get("TARGET_DATABASE", props.Get("target.database", "ORACLE"));
        
        if (!Enum.TryParse<DatabaseType>(targetDbStr, true, out var targetDatabase))
        {
            Log.Error("✗ Invalid TARGET_DATABASE: {TargetDb}. Must be ORACLE or POSTGRESQL", targetDbStr);
            Environment.Exit(1);
            return;
        }

        Log.Information("   Target Database: {TargetDb}", targetDatabase);

        Log.Information("");
        Log.Information("3. Initializing {DbType} connection...", targetDatabase);
        var config = targetDatabase == DatabaseType.Oracle
            ? DatabaseConfig.CreateOracleConfig(props)
            : DatabaseConfig.CreatePostgresConfig(props);

        connectionManager.InitializePool(targetDatabase, config);

        Log.Information("");
        Log.Information("4. Testing database connectivity...");
        bool connectionOk = connectionManager.TestConnection(targetDatabase);

        if (!connectionOk)
        {
            Log.Error("✗ Database connectivity test failed for {DbType}", targetDatabase);
            Environment.Exit(1);
        }

        Log.Information("✓ {DbType} connection validated", targetDatabase);

        string tablesStr = props.Get("TABLES_TO_PROCESS", props.Get("tables.to.process", ""));
        List<string> tables;

        if (!string.IsNullOrWhiteSpace(tablesStr))
        {
            tables = tablesStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
            Log.Information("Will process specified tables: {Tables}", string.Join(", ", tables));
        }
        else
        {
            Log.Warning("No tables specified - discovering tables from database schema");

            string schemaName = targetDatabase == DatabaseType.Oracle
                ? props.Get("ORACLE_SCHEMA", "")
                : props.Get("POSTGRES_SCHEMA", "");

            if (string.IsNullOrWhiteSpace(schemaName))
            {
                Log.Error("✗ No schema specified. Set ORACLE_SCHEMA or POSTGRES_SCHEMA in .env");
                Environment.Exit(1);
                return;
            }

            tables = connectionManager.GetTablesInSchema(targetDatabase, schemaName);
            Log.Information("Discovered {Count} tables in schema {Schema}", tables.Count, schemaName);
        }

        Log.Information("");
        Log.Information("5. Connection Pool Status:");
        Log.Information("   {Stats}", connectionManager.GetPoolStats(targetDatabase));

        var processor = new SingleDatabaseProcessor(connectionManager);
        processor.ProcessTables(targetDatabase, tables);
    }
    
    static void RunComparisonMode(DatabaseConnectionManager connectionManager, ApplicationProperties props)
    {
        Log.Information("");
        Log.Information("2. MODE: Dual Database Comparison (Migration Validation)");

        Log.Information("");
        Log.Information("3. Initializing database connections...");
        var oracleConfig = DatabaseConfig.CreateOracleConfig(props);
        var postgresConfig = DatabaseConfig.CreatePostgresConfig(props);

        connectionManager.InitializePool(DatabaseType.Oracle, oracleConfig);
        connectionManager.InitializePool(DatabaseType.PostgreSQL, postgresConfig);

        Log.Information("");
        Log.Information("4. Testing database connectivity...");
        bool oracleOk = connectionManager.TestConnection(DatabaseType.Oracle);
        bool postgresOk = connectionManager.TestConnection(DatabaseType.PostgreSQL);

        if (!oracleOk || !postgresOk)
        {
            Log.Error("✗ Database connectivity test failed");
            if (!oracleOk) Log.Error("  - Oracle connection failed");
            if (!postgresOk) Log.Error("  - PostgreSQL connection failed");
            Environment.Exit(1);
        }

        Log.Information("✓ All database connections validated");

        string tablesConfig = props.Get("TABLES_TO_COMPARE", props.Get("tables.to.compare", ""));

        Dictionary<string, string> tableMapping;

        if (string.IsNullOrWhiteSpace(tablesConfig))
        {
            Log.Error("");
            Log.Error("✗ No tables specified for comparison");
            Log.Error("  Set TABLES_TO_COMPARE in .env file");
            Log.Error("  Format: schema.table,schema.table2 or ALL for auto-discovery");
            Environment.Exit(1);
            return;
        }

        if (tablesConfig.Trim().Equals("ALL", StringComparison.OrdinalIgnoreCase))
        {
            Log.Information("");
            Log.Information("5. Discovering all common tables between Oracle and PostgreSQL...");
            
            string oracleSchema = props.Get("ORACLE_SCHEMA", "");
            string postgresSchema = props.Get("POSTGRES_SCHEMA", "");

            if (string.IsNullOrWhiteSpace(oracleSchema) || string.IsNullOrWhiteSpace(postgresSchema))
            {
                Log.Error("✗ Schema names required for auto-discovery");
                Log.Error("  Set ORACLE_SCHEMA and POSTGRES_SCHEMA in .env");
                Environment.Exit(1);
                return;
            }

            var oracleTables = connectionManager.GetTablesInSchema(DatabaseType.Oracle, oracleSchema);
            var postgresTables = connectionManager.GetTablesInSchema(DatabaseType.PostgreSQL, postgresSchema);

            tableMapping = new Dictionary<string, string>();
            foreach (var oracleTable in oracleTables)
            {
                var matchingPostgresTable = postgresTables
                    .FirstOrDefault(pt => string.Equals(pt, oracleTable, StringComparison.OrdinalIgnoreCase));
                
                if (matchingPostgresTable != null)
                {
                    string oracleRef = $"{oracleSchema}.{oracleTable}";
                    string postgresRef = $"{postgresSchema}.{matchingPostgresTable}";
                    tableMapping[oracleRef] = postgresRef;
                }
            }

            Log.Information("Found {Count} common tables", tableMapping.Count);
        }
        else
        {
            Log.Information("");
            Log.Information("5. Parsing table mapping configuration...");

            tableMapping = CaseConverter.ParseAndNormalizeMapping(tablesConfig);
            Log.Information("Parsed {Count} table mappings", tableMapping.Count);
        }

        Log.Information("");
        Log.Information("6. Configuration:");
        Log.Information("   Hash Algorithm: {Algorithm}", props.Get("HASH_ALGORITHM", props.Get("hash.algorithm", "SHA256")));
        Log.Information("   Batch Size: {BatchSize}", props.GetInt("BATCH_SIZE", props.GetInt("batch.size", 5000)));
        Log.Information("   Save Hashes to CSV: {SaveCsv}", props.GetBool("SAVE_HASHES_TO_CSV", props.GetBool("save.hashes.to.csv", true)));
        Log.Information("   Max Rows Per Table: {MaxRows}", props.GetInt("MAX_ROWS_PER_TABLE", props.GetInt("max.rows.per.table", 0)));

        Log.Information("");
        Log.Information("7. Connection Pool Status:");
        Log.Information("   {OracleStats}", connectionManager.GetPoolStats(DatabaseType.Oracle));
        Log.Information("   {PostgresStats}", connectionManager.GetPoolStats(DatabaseType.PostgreSQL));

        var processor = new ComparisonDatabaseProcessor(connectionManager);
        processor.ProcessAndCompareTables(tableMapping);
    }
}
