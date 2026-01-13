using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgRowCountValidator.Extractors;
using Ora2PgRowCountValidator.Comparison;
using Serilog;
using System.Diagnostics;
using Ora2PgRowCountValidator.src.Writers;

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


            var oracleSchema = props.Get("ORACLE_SCHEMA");
            var postgresSchema = props.Get("POSTGRES_SCHEMA");

            if (string.IsNullOrWhiteSpace(oracleSchema) || string.IsNullOrWhiteSpace(postgresSchema))
            {
                Log.Fatal("❌ ORACLE_SCHEMA and POSTGRES_SCHEMA must be set in .env file");
                return 99;
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
                ServiceOrDatabase = props.Get("POSTGRES_DATABASE"),
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


            var stopwatch = Stopwatch.StartNew();
            
            Log.Information($"📊 Extracting row counts from Oracle schema '{oracleSchema}'...");
            var oracleExtractor = new OracleRowCountExtractor(oracleConnString);
            var oracleCounts = await oracleExtractor.ExtractRowCountsAsync(oracleSchema);
            Log.Information($"✓ Found {oracleCounts.Count} tables in Oracle (Total: {oracleCounts.Sum(t => t.RowCount):N0} rows)");

            Log.Information($"📊 Extracting row counts from PostgreSQL schema '{postgresSchema}'...");
            var postgresExtractor = new PostgresRowCountExtractor(postgresConnString);
            var postgresCounts = await postgresExtractor.ExtractRowCountsAsync(postgresSchema);
            Log.Information($"✓ Found {postgresCounts.Count} tables in PostgreSQL (Total: {postgresCounts.Sum(t => t.RowCount):N0} rows)");

            Log.Information("═══════════════════════════════════════════════════════════");


            Log.Information("🔍 Comparing row counts...");
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

            stopwatch.Stop();
            Log.Information($"✓ Comparison completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Log.Information($"  - Matches: {result.TablesWithMatchingCounts}");
            Log.Information($"  - Mismatches: {result.TablesWithMismatchedCounts}");
            Log.Information($"  - Only in Oracle: {result.TablesOnlyInOracle}");
            Log.Information($"  - Only in PostgreSQL: {result.TablesOnlyInPostgres}");
            Log.Information("═══════════════════════════════════════════════════════════");


            Log.Information("📝 Generating reports...");
            var reportsDir = Path.Combine(Directory.GetCurrentDirectory(), "reports");
            Directory.CreateDirectory(reportsDir);

            var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
            var baseReportPath = Path.Combine(reportsDir, $"rowcount-validation-{timestamp}");

            var reportWriter = new ValidationReportWriter();
            await reportWriter.WriteReportsAsync(result, baseReportPath);


            var htmlWriter = new ValidationReportHtmlWriter();
            htmlWriter.WriteHtmlReport(result, oracleSchema, postgresSchema, $"{baseReportPath}.html");


            reportWriter.WriteConsoleReport(result);


            var exitCode = GetExitCode(result);
            Log.Information($"Exit code: {exitCode}");

            return exitCode;
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
