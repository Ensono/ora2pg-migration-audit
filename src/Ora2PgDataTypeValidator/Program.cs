using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataTypeValidator.Extractors;
using Ora2PgDataTypeValidator.Models;
using Ora2PgDataTypeValidator.src.Writers;
using Ora2PgDataTypeValidator.Validators;
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
        Environment.Exit(1);
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
        Environment.Exit(1);
    }
    if (!connectionManager.TestConnection(DatabaseType.PostgreSQL))
    {
        Log.Fatal("❌ PostgreSQL connection failed");
        Environment.Exit(1);
    }

    var oracleConnString = oracleConfig.GetOracleConnectionString();
    var postgresConnString = postgresConfig.GetPostgresConnectionString();

    var oracleExtractor = new OracleColumnExtractor(oracleConnString);
    var postgresExtractor = new PostgresColumnExtractor(postgresConnString);
    var validator = new DataTypeValidator();

    bool hasAnyErrors = false;
    bool hasAnyWarnings = false;
    var allSchemaResults = new List<(string OracleSchema, string PostgresSchema, ValidationResult Result)>();

    var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
    var reportsDir = props.GetReportsDirectory("Ora2PgDataTypeValidator");

    for (int i = 0; i < oracleSchemas.Length; i++)
    {
        string oracleSchema = oracleSchemas[i];
        string postgresSchema = postgresSchemas[i];

        Log.Information("");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Schema Pair {Index}/{Total}: {OracleSchema} → {PostgresSchema}", 
            i + 1, oracleSchemas.Length, oracleSchema, postgresSchema);
        Log.Information("═══════════════════════════════════════════════════════════");

        Log.Information($"📊 Extracting column metadata from Oracle schema: {oracleSchema}");
        var oracleColumns = await oracleExtractor.ExtractColumnsAsync(oracleSchema);

        Log.Information($"📊 Extracting column metadata from PostgreSQL schema: {postgresSchema}");
        var postgresColumns = await postgresExtractor.ExtractColumnsAsync(postgresSchema);

        var result = validator.Validate(oracleColumns, postgresColumns);
        allSchemaResults.Add((oracleSchema, postgresSchema, result));
        
        Log.Debug("Added result for schema pair: {OracleSchema} → {PostgresSchema}. Total results: {Count}", 
            oracleSchema, postgresSchema, allSchemaResults.Count);

        Log.Information("📝 Generating validation reports...");
        
        var schemaPrefix = $"{oracleSchema.ToLower()}-";
        
        var reportWriter = new ValidationReportWriter();

        reportWriter.WriteConsoleReport(result);

        var markdownReportPath = Path.Combine(reportsDir, $"{schemaPrefix}datatype-validation-{timestamp}.md");
        await reportWriter.WriteReportsAsync(result, markdownReportPath);

        var htmlWriter = new DataTypeValidationHtmlWriter();
        var htmlReportPath = Path.Combine(reportsDir, $"{schemaPrefix}datatype-validation-{timestamp}.html");
        htmlWriter.WriteHtmlReport(result, htmlReportPath);
        Log.Information("📄 HTML report saved to: {ReportPath}", htmlReportPath);

        if (result.HasCriticalIssues || result.HasErrors)
        {
            hasAnyErrors = true;
        }
        if (result.Warnings > 0)
        {
            hasAnyWarnings = true;
        }

        Log.Information("✓ Schema pair validation complete");
    }

    if (oracleSchemas.Length > 1)
    {
        Log.Information("");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Multi-Schema Validation Summary");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("Total schema pairs validated: {Count}", oracleSchemas.Length);
        Log.Information("Total results collected: {Count}", allSchemaResults.Count);
        
        Log.Information("📝 Generating multi-schema summary report...");
        var summaryWriter = new MultiSchemaSummaryWriter();
        summaryWriter.WriteSummaryReport(allSchemaResults, reportsDir, timestamp);
        
        if (hasAnyErrors)
        {
            Log.Error("⚠️ One or more schemas have critical issues or errors");
        }
        else if (hasAnyWarnings)
        {
            Log.Warning("⚠️ One or more schemas have warnings");
        }
        else
        {
            Log.Information("✓ All schemas validated successfully");
        }
    }

    connectionManager.Dispose();

    if (hasAnyErrors)
    {
        Environment.Exit(1);
    }
    else if (hasAnyWarnings)
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
