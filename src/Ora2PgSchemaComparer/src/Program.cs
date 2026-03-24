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
        Environment.Exit(1);
    }

    var oracleSchemas = oracleSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    var postgresSchemas = postgresSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    if (oracleSchemas.Length != postgresSchemas.Length)
    {
        Log.Fatal("❌ Number of Oracle schemas ({OracleCount}) must match number of PostgreSQL schemas ({PostgresCount})", 
            oracleSchemas.Length, postgresSchemas.Length);
        Log.Information("  ORACLE_SCHEMA: {OracleSchemas}", oracleSchemasStr);
        Log.Information("  POSTGRES_SCHEMA: {PostgresSchemas}", postgresSchemasStr);
        Log.Information("  Schemas must be provided in matching order, comma-separated");
        Environment.Exit(1);
    }

    if (oracleSchemas.Length > 1)
    {
        Log.Information("Multi-schema mode: Comparing {Count} schema pairs", oracleSchemas.Length);
    }

    using var oracleManager = new DatabaseConnectionManager();
    oracleManager.InitializePool(DatabaseType.Oracle, DatabaseConfig.CreateOracleConfig(props));
    var oracleExtractor = new OracleSchemaExtractor(oracleManager);

    using var postgresManager = new DatabaseConnectionManager();
    postgresManager.InitializePool(DatabaseType.PostgreSQL, DatabaseConfig.CreatePostgresConfig(props));
    var postgresExtractor = new PostgresSchemaExtractor(postgresManager);

    var allComparisonResults = new List<ComparisonResult>();
    var allSchemaResults = new List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)>();
    var comparator = new SchemaComparator();

    for (int i = 0; i < oracleSchemas.Length; i++)
    {
        string oracleSchema = oracleSchemas[i];
        string postgresSchema = postgresSchemas[i];

        Log.Information("");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Schema Pair {Index}/{Total}: {OracleSchema} → {PostgresSchema}", 
            i + 1, oracleSchemas.Length, oracleSchema, postgresSchema);
        Log.Information("═══════════════════════════════════════════════════════════");

        Log.Information("Extracting Oracle schema metadata...");
        var oracleSchemaDefinition = await Task.Run(() => oracleExtractor.ExtractSchema(oracleSchema));

        Log.Information("Extracting PostgreSQL schema metadata...");
        var postgresSchemaDefinition = await Task.Run(() => postgresExtractor.ExtractSchema(postgresSchema));

        Log.Information("Comparing schemas...");
        var comparisonResult = comparator.Compare(oracleSchemaDefinition, postgresSchemaDefinition);
        allComparisonResults.Add(comparisonResult);
        allSchemaResults.Add((oracleSchema, postgresSchema, comparisonResult));

        Log.Information("✓ Schema pair comparison complete: {TotalIssues} issues found", comparisonResult.TotalIssues);
    }

    Log.Information("");
    Log.Information("═══════════════════════════════════════════════════════════");
    Log.Information("  Generating Consolidated Reports");
    Log.Information("═══════════════════════════════════════════════════════════");

    var reportsDir = props.GetReportsDirectory("Ora2PgSchemaComparer");
    var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");

    var reportWriter = new SchemaComparisonReportWriter();
    var markdownWriter = new SchemaComparisonMarkdownWriter();
    var htmlWriter = new SchemaComparisonHtmlWriter();

    bool hasCriticalIssues = false;
    var allReportsText = new System.Text.StringBuilder();

    for (int i = 0; i < allComparisonResults.Count; i++)
    {
        var result = allComparisonResults[i];
        var schemaPrefix = $"{oracleSchemas[i].ToLower()}-";

        var markdownReportPath = Path.Combine(reportsDir, $"{schemaPrefix}schema-comparison-{timestamp}.md");
        markdownWriter.WriteMarkdownReport(result, markdownReportPath);
        Log.Information("📄 Markdown report saved to: {ReportPath}", markdownReportPath);

        var report = reportWriter.GenerateReport(result);
        var textReportPath = Path.Combine(reportsDir, $"{schemaPrefix}schema-comparison-{timestamp}.txt");
        await File.WriteAllTextAsync(textReportPath, report);
        Log.Information("📄 Text report saved to: {ReportPath}", textReportPath);

        var htmlReportPath = Path.Combine(reportsDir, $"{schemaPrefix}schema-comparison-{timestamp}.html");
        htmlWriter.WriteHtmlReport(result, htmlReportPath);
        Log.Information("📄 HTML report saved to: {ReportPath}", htmlReportPath);

        if (oracleSchemas.Length > 1)
        {
            allReportsText.AppendLine($"\n{'═',80}");
            allReportsText.AppendLine($"  Schema Pair {i + 1}/{oracleSchemas.Length}: {oracleSchemas[i]} → {postgresSchemas[i]}");
            allReportsText.AppendLine($"{'═',80}\n");
        }
        allReportsText.AppendLine(report);

        if (result.HasCriticalIssues)
        {
            hasCriticalIssues = true;
        }
    }

    Console.WriteLine();
    Console.WriteLine(allReportsText.ToString());

    if (oracleSchemas.Length > 1)
    {
        var summaryWriter = new MultiSchemaSummaryWriter();
        summaryWriter.WriteSummaryReport(allSchemaResults, reportsDir, timestamp);
    }

    if (oracleSchemas.Length > 1)
    {
        Log.Information("");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Multi-Schema Comparison Summary");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("Total schema pairs compared: {Count}", allComparisonResults.Count);
        
        var totalIssues = allComparisonResults.Sum(r => r.TotalIssues);
        var schemasWithIssues = allComparisonResults.Count(r => r.TotalIssues > 0);
        var schemasWithCritical = allComparisonResults.Count(r => r.HasCriticalIssues);

        Log.Information("Total issues across all schemas: {TotalIssues}", totalIssues);
        Log.Information("Schemas with issues: {Count}/{Total}", schemasWithIssues, allComparisonResults.Count);
        Log.Information("Schemas with critical issues: {Count}/{Total}", schemasWithCritical, allComparisonResults.Count);

        for (int i = 0; i < allComparisonResults.Count; i++)
        {
            var result = allComparisonResults[i];
            var status = result.HasCriticalIssues ? "❌ CRITICAL" : 
                        result.TotalIssues > 0 ? "⚠️ WARNINGS" : "✓ PASSED";
            Log.Information("  {OracleSchema} → {PostgresSchema}: {Status} (Grade: {Grade}, Issues: {Issues})",
                oracleSchemas[i], postgresSchemas[i], status, result.OverallGrade, result.TotalIssues);
        }
    }

    if (hasCriticalIssues)
    {
        Log.Warning("⚠️ Critical issues found in schema comparison");
        Environment.Exit(1);
    }
    
    var overallGrade = oracleSchemas.Length == 1
        ? allComparisonResults[0].OverallGrade
        : CalculateOverallGrade(allComparisonResults);
    
    Log.Information("✓ Schema comparison complete: Overall Grade {Grade}", overallGrade);
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

static string CalculateOverallGrade(List<ComparisonResult> results)
{
    var grades = results.Select(r => r.OverallGrade).ToList();
    
    if (grades.Any(g => g == "F")) return "F";
    if (grades.Any(g => g == "D")) return "D";
    if (grades.Any(g => g == "C")) return "C";
    if (grades.Any(g => g == "B")) return "B";
    if (grades.All(g => g == "A+")) return "A+";
    return "A";
}
