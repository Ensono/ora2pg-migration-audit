using Serilog;
using Serilog.Events;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgSchemaComparer.Extractor;
using Ora2PgSchemaComparer.Comparison;
using Ora2PgSchemaComparer.src.Writers;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File("logs/application.log", 
        rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    Log.Information("=== Ora2Pg Schema Comparer Started ===");

    var props = ApplicationProperties.Instance;

    var oracleSchema = props.Get("ORACLE_SCHEMA");
    var postgresSchema = props.Get("POSTGRES_SCHEMA");
    
    if (string.IsNullOrWhiteSpace(oracleSchema) || string.IsNullOrWhiteSpace(postgresSchema))
    {
        Log.Fatal("❌ ORACLE_SCHEMA and POSTGRES_SCHEMA must be set in .env file");
        Log.Information("Example .env configuration:");
        Log.Information("  ORACLE_SCHEMA=YOUR_ORACLE_SCHEMA");
        Log.Information("  POSTGRES_SCHEMA=your_postgres_schema");
        Environment.Exit(1);
    }
    
    Log.Information("Comparing schemas: Oracle={OracleSchema} vs PostgreSQL={PostgresSchema}", 
        oracleSchema, postgresSchema);

    Log.Information("Extracting Oracle schema metadata...");
    using var oracleManager = new DatabaseConnectionManager();
    oracleManager.InitializePool(DatabaseType.Oracle, DatabaseConfig.CreateOracleConfig(props));
    var oracleExtractor = new OracleSchemaExtractor(oracleManager);
    var oracleSchemaDefinition = await Task.Run(() => oracleExtractor.ExtractSchema(oracleSchema));

    Log.Information("Extracting PostgreSQL schema metadata...");
    using var postgresManager = new DatabaseConnectionManager();
    postgresManager.InitializePool(DatabaseType.PostgreSQL, DatabaseConfig.CreatePostgresConfig(props));
    var postgresExtractor = new PostgresSchemaExtractor(postgresManager);
    var postgresSchemaDefinition = await Task.Run(() => postgresExtractor.ExtractSchema(postgresSchema));

    Log.Information("Comparing schemas...");
    var comparator = new SchemaComparator();
    var comparisonResult = comparator.Compare(oracleSchemaDefinition, postgresSchemaDefinition);

    Log.Information("Generating schema comparison reports...");
    Directory.CreateDirectory("reports");
    var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");

    var markdownWriter = new SchemaComparisonMarkdownWriter();
    var markdownReportPath = $"reports/schema-comparison-{timestamp}.md";
    markdownWriter.WriteMarkdownReport(comparisonResult, markdownReportPath);
    Log.Information("📄 Markdown report saved to: {ReportPath}", markdownReportPath);

    var reportWriter = new SchemaComparisonReportWriter();
    var report = reportWriter.GenerateReport(comparisonResult);
    var textReportPath = $"reports/schema-comparison-{timestamp}.txt";
    await File.WriteAllTextAsync(textReportPath, report);
    Log.Information("📄 Text report saved to: {ReportPath}", textReportPath);

    var htmlWriter = new SchemaComparisonHtmlWriter();
    var htmlReportPath = $"reports/schema-comparison-{timestamp}.html";
    htmlWriter.WriteHtmlReport(comparisonResult, htmlReportPath);
    Log.Information("📄 HTML report saved to: {ReportPath}", htmlReportPath);

    Console.WriteLine();
    Console.WriteLine(report);

    if (comparisonResult.HasCriticalIssues)
    {
        Log.Warning("⚠️ Critical issues found in schema comparison");
        Environment.Exit(1);
    }
    
    Log.Information("✓ Schema comparison complete: Grade {Grade}", comparisonResult.OverallGrade);
    Environment.Exit(0);
}
catch (Exception ex)
{
    Log.Fatal(ex, "❌ Fatal error during schema comparison");
    Environment.Exit(1);
}
finally
{
    Log.CloseAndFlush();
}
