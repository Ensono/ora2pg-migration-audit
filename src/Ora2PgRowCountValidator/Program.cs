using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgRowCountValidator.Extractors;
using Ora2PgRowCountValidator.Comparison;
using Ora2PgRowCountValidator.Writers;
using Serilog;
using System.Diagnostics;
using Ora2PgRowCountValidator.src.Writers;
using System.Collections.Concurrent;

namespace Ora2PgRowCountValidator;

class Program
{
    static async Task<int> Main(string[] args)
    {

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Console()
            .CreateLogger();

        try
        {
            Log.Information("🚀 Oracle to PostgreSQL Row Count Validator");
            Log.Information("═══════════════════════════════════════════════════════════");

            var props = ApplicationProperties.Instance;
            Log.Information("✓ Configuration loaded from .env file");

            var oracleSchemasStr = props.Get("ORACLE_SCHEMA");
            var postgresSchemasStr = props.Get("POSTGRES_SCHEMA");

            if (string.IsNullOrWhiteSpace(oracleSchemasStr) || string.IsNullOrWhiteSpace(postgresSchemasStr))
            {
                Log.Fatal("❌ ORACLE_SCHEMA and POSTGRES_SCHEMA must be set in .env file");
                Log.Information("Example .env configuration:");
                Log.Information("  ORACLE_SCHEMA=YOUR_ORACLE_SCHEMA");
                Log.Information("  POSTGRES_SCHEMA=your_postgres_schema");
                Log.Information("  Or for multiple schemas:");
                Log.Information("  ORACLE_SCHEMA=SCHEMA1,SCHEMA2,SCHEMA3");
                Log.Information("  POSTGRES_SCHEMA=schema1,schema2,schema3");
                return 99;
            }

            var oracleSchemas = oracleSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var postgresSchemas = postgresSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            if (oracleSchemas.Length != postgresSchemas.Length)
            {
                Log.Fatal("❌ Number of Oracle schemas ({OracleCount}) must match number of PostgreSQL schemas ({PostgresCount})", 
                    oracleSchemas.Length, postgresSchemas.Length);
                Log.Information("  ORACLE_SCHEMA: {OracleSchemas}", oracleSchemasStr);
                Log.Information("  POSTGRES_SCHEMA: {PostgresSchemas}", postgresSchemasStr);
                return 99;
            }

            if (oracleSchemas.Length > 1)
            {
                Log.Information("Multi-schema mode: Validating {Count} schema pairs", oracleSchemas.Length);
            }

            var connectionManager = new DatabaseConnectionManager();
            
            var oracleConfig = new DatabaseConfig
            {
                Host = props.Get("ORACLE_HOST", "localhost"),
                Port = props.GetInt("ORACLE_PORT", 1521),
                ServiceOrDatabase = props.Get("ORACLE_SERVICE", "FREE"),
                Username = props.Get("ORACLE_USER"),
                Password = props.Get("ORACLE_PASSWORD")
            };
            connectionManager.InitializePool(DatabaseType.Oracle, oracleConfig);
            
            var postgresConfig = new DatabaseConfig
            {
                Host = props.Get("POSTGRES_HOST", "localhost"),
                Port = props.GetInt("POSTGRES_PORT", 5432),
                ServiceOrDatabase = props.Get("POSTGRES_DB"),
                Username = props.Get("POSTGRES_USER"),
                Password = props.Get("POSTGRES_PASSWORD")
            };
            connectionManager.InitializePool(DatabaseType.PostgreSQL, postgresConfig);
            
            Log.Information("🔌 Testing database connections...");
            if (!connectionManager.TestConnection(DatabaseType.Oracle))
            {
                Log.Fatal("❌ Oracle connection failed");
                return 99;
            }
            if (!connectionManager.TestConnection(DatabaseType.PostgreSQL))
            {
                Log.Fatal("❌ PostgreSQL connection failed");
                return 99;
            }
            
            Log.Information("═══════════════════════════════════════════════════════════");

            var oracleConnString = oracleConfig.GetOracleConnectionString();
            var postgresConnString = postgresConfig.GetPostgresConnectionString();

            int maxExitCode = 0;
            var allSchemaResults = new ConcurrentBag<(string OracleSchema, string PostgresSchema, Ora2PgRowCountValidator.Models.ValidationResult Result)>();
            var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
            var reportsDir = props.GetReportsDirectory("Ora2PgRowCountValidator");
            
            // Command timeout for COUNT queries on large tables (default: 10 minutes)
            var commandTimeoutSeconds = props.GetInt("COMMAND_TIMEOUT_SECONDS", 600);
            Log.Information("Using query timeout: {Timeout} seconds", commandTimeoutSeconds);

            // Check if parallel schema processing is enabled
            var parallelSchemas = props.GetInt("PARALLEL_SCHEMAS", 1);
            var processInParallel = parallelSchemas > 1 && oracleSchemas.Length > 1;

            if (processInParallel)
            {
                Log.Information("Processing {Count} schemas in parallel (max {Parallel} concurrent)", 
                    oracleSchemas.Length, parallelSchemas);

                var schemaList = oracleSchemas.Zip(postgresSchemas, (o, p) => (Oracle: o, Postgres: p)).ToList();
                var exitCodes = new ConcurrentBag<int>();

                await Parallel.ForEachAsync(schemaList, new ParallelOptions { MaxDegreeOfParallelism = parallelSchemas }, 
                    async (schemaPair, cancellationToken) =>
                {
                    var (oracleSchema, postgresSchema) = schemaPair;
                    var exitCode = await ProcessSchemaAsync(
                        oracleSchema, postgresSchema, 
                        oracleConnString, postgresConnString,
                        commandTimeoutSeconds, props, reportsDir, timestamp,
                        allSchemaResults);
                    exitCodes.Add(exitCode);
                });

                maxExitCode = exitCodes.DefaultIfEmpty(0).Max();
            }
            else
            {
                for (int i = 0; i < oracleSchemas.Length; i++)
                {
                    string oracleSchema = oracleSchemas[i];
                    string postgresSchema = postgresSchemas[i];

                    Log.Information("");
                    Log.Information("═══════════════════════════════════════════════════════════");
                    Log.Information("  Schema Pair {Index}/{Total}: {OracleSchema} → {PostgresSchema}", 
                        i + 1, oracleSchemas.Length, oracleSchema, postgresSchema);
                    Log.Information("═══════════════════════════════════════════════════════════");

                    var exitCode = await ProcessSchemaAsync(
                        oracleSchema, postgresSchema,
                        oracleConnString, postgresConnString,
                        commandTimeoutSeconds, props, reportsDir, timestamp,
                        allSchemaResults);
                    maxExitCode = Math.Max(maxExitCode, exitCode);
                }
            }

            var allSchemaResultsList = allSchemaResults.ToList();

            if (oracleSchemas.Length > 1)
            {
                Log.Information("");
                Log.Information("═══════════════════════════════════════════════════════════");
                Log.Information("  Multi-Schema Validation Summary");
                Log.Information("═══════════════════════════════════════════════════════════");
                Log.Information("Total schema pairs validated: {Count}", oracleSchemas.Length);
                
                Log.Information("📝 Generating multi-schema summary report...");
                var summaryWriter = new MultiSchemaSummaryWriter();
                summaryWriter.WriteSummaryReport(allSchemaResultsList, reportsDir, timestamp);
                
                if (maxExitCode == 99)
                {
                    Log.Error("⚠️ One or more schemas had fatal errors");
                }
                else if (maxExitCode == 2)
                {
                    Log.Error("⚠️ One or more schemas have critical issues");
                }
                else if (maxExitCode == 1)
                {
                    Log.Warning("⚠️ One or more schemas have errors");
                }
                else
                {
                    Log.Information("✓ All schemas validated successfully");
                }
            }

            Log.Information($"Exit code: {maxExitCode}");
            return maxExitCode;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "❌ Fatal error during row count validation");
            return 99; // Fatal error
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    private static async Task<int> ProcessSchemaAsync(
        string oracleSchema, 
        string postgresSchema,
        string oracleConnString,
        string postgresConnString,
        int commandTimeoutSeconds,
        ApplicationProperties props,
        string reportsDir,
        string timestamp,
        ConcurrentBag<(string OracleSchema, string PostgresSchema, Ora2PgRowCountValidator.Models.ValidationResult Result)> allSchemaResults)
    {
        var stopwatch = Stopwatch.StartNew();
        
        Log.Information($"[{oracleSchema}] 📊 Extracting row counts from Oracle schema...");
        var oracleExtractor = new OracleRowCountExtractor(oracleConnString, commandTimeoutSeconds);
        var oracleCounts = await oracleExtractor.ExtractRowCountsAsync(oracleSchema);
        Log.Information($"[{oracleSchema}] ✓ Found {oracleCounts.Count} tables in Oracle (Total: {oracleCounts.Sum(t => t.RowCount):N0} rows)");

        Log.Information($"[{oracleSchema}] 📊 Extracting row counts from PostgreSQL schema '{postgresSchema}'...");
        var postgresExtractor = new PostgresRowCountExtractor(postgresConnString, commandTimeoutSeconds);
        var postgresCounts = await postgresExtractor.ExtractRowCountsAsync(postgresSchema);
        Log.Information($"[{oracleSchema}] ✓ Found {postgresCounts.Count} tables in PostgreSQL (Total: {postgresCounts.Sum(t => t.RowCount):N0} rows)");

        Log.Information($"[{oracleSchema}] 🔍 Comparing row counts...");
        var comparer = new RowCountComparer(
            oracleConnString, 
            postgresConnString, 
            enableDetailedComparison: true);
        
        var result = await comparer.CompareAsync(
            oracleCounts,
            postgresCounts,
            oracleSchema,
            postgresSchema
        );

        result.OracleDatabase = props.Get("ORACLE_SERVICE", "");
        result.PostgresDatabase = props.Get("POSTGRES_DB", "");

        stopwatch.Stop();
        Log.Information($"[{oracleSchema}] ✓ Comparison completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        Log.Information($"[{oracleSchema}]   - Matches: {result.TablesWithMatchingCounts}");
        Log.Information($"[{oracleSchema}]   - Mismatches: {result.TablesWithMismatchedCounts}");
        Log.Information($"[{oracleSchema}]   - Only in Oracle: {result.TablesOnlyInOracle}");
        Log.Information($"[{oracleSchema}]   - Only in PostgreSQL: {result.TablesOnlyInPostgres}");

        allSchemaResults.Add((oracleSchema, postgresSchema, result));

        Log.Information($"[{oracleSchema}] 📝 Generating reports...");

        var dbName = props.Get("POSTGRES_DB", "").ToLower();
        var dbPrefix = string.IsNullOrEmpty(dbName) ? "" : $"{dbName}-";
        var schemaPrefix = $"{dbPrefix}{oracleSchema.ToLower()}-";
        var baseReportPath = Path.Combine(reportsDir, $"{schemaPrefix}rowcount-validation-{timestamp}");

        var reportWriter = new ValidationReportWriter();
        await reportWriter.WriteReportsAsync(result, baseReportPath);

        var htmlWriter = new ValidationReportHtmlWriter();
        htmlWriter.WriteHtmlReport(result, oracleSchema, postgresSchema, $"{baseReportPath}.html");

        reportWriter.WriteConsoleReport(result);

        var exitCode = GetExitCode(result);
        Log.Information($"[{oracleSchema}] ✓ Schema pair validation complete (exit code: {exitCode})");
        
        return exitCode;
    }

    private static int GetExitCode(Ora2PgRowCountValidator.Models.ValidationResult result)
    {
        if (result.HasCriticalIssues)
            return 2;
        
        if (result.HasErrors)
            return 1;
        
        if (result.Warnings > 0)
            return 0;
        
        return 0;
    }
}
