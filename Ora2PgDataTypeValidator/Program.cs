using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataTypeValidator.Extractors;
using Ora2PgDataTypeValidator.Validators;
using Ora2PgDataTypeValidator.Reports;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .CreateLogger();

try
{
    Log.Information("🚀 Oracle to PostgreSQL Data Type Validator");
    Log.Information("============================================");

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
        Environment.Exit(1);
    }
    if (!connectionManager.TestConnection(DatabaseType.PostgreSQL))
    {
        Log.Fatal("❌ PostgreSQL connection failed");
        Environment.Exit(1);
    }

    var oracleConnString = oracleConfig.GetOracleConnectionString();
    var postgresConnString = postgresConfig.GetPostgresConnectionString();

    Log.Information($"📊 Extracting column metadata from Oracle schema: {oracleSchema}");
    var oracleExtractor = new OracleColumnExtractor(oracleConnString);
    var oracleColumns = await oracleExtractor.ExtractColumnsAsync(oracleSchema);

    Log.Information($"📊 Extracting column metadata from PostgreSQL schema: {postgresSchema}");
    var postgresExtractor = new PostgresColumnExtractor(postgresConnString);
    var postgresColumns = await postgresExtractor.ExtractColumnsAsync(postgresSchema);

    var validator = new DataTypeValidator();
    var result = validator.Validate(oracleColumns, postgresColumns);

    Log.Information("📝 Generating validation reports...");
    Directory.CreateDirectory("reports");
    var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
    
    var reportWriter = new ValidationReportWriter();

    reportWriter.WriteConsoleReport(result);

    var markdownReportPath = Path.Combine("reports", $"datatype-validation-{timestamp}.md");
    await reportWriter.WriteReportsAsync(result, markdownReportPath);

    var htmlWriter = new DataTypeValidationHtmlWriter();
    var htmlReportPath = Path.Combine("reports", $"datatype-validation-{timestamp}.html");
    htmlWriter.WriteHtmlReport(result, htmlReportPath);
    Log.Information("📄 HTML report saved to: {ReportPath}", htmlReportPath);

    connectionManager.Dispose();

    if (result.HasCriticalIssues || result.HasErrors)
    {
        Environment.Exit(1);
    }
    else if (result.Warnings > 0)
    {
        Environment.Exit(2); // Warning exit code
    }
    else
    {
        Environment.Exit(0); // Success
    }
}
catch (Exception ex)
{
    Log.Fatal(ex, "💥 Fatal error during validation");
    Environment.Exit(1);
}
finally
{
    Log.CloseAndFlush();
}
