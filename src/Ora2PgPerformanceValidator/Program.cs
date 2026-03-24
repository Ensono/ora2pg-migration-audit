using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2Pg.Common.Util;
using Ora2PgPerformanceValidator.Loaders;
using Ora2PgPerformanceValidator.Executors;
using Ora2PgPerformanceValidator.Writers;
using Ora2PgPerformanceValidator.Models;
using Ora2PgPerformanceValidator.Discovery;
using Ora2PgPerformanceValidator.Generators;
using Serilog;
using System.Diagnostics;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .CreateLogger();

try
{
    Log.Information("🚀 Oracle to PostgreSQL Performance Validator");
    Log.Information("═══════════════════════════════════════════════════════════");

    var props = ApplicationProperties.Instance;

    var oracleSchemasStr = props.Get("ORACLE_SCHEMA", "TESTUSER");
    var postgresSchemasStr = props.Get("POSTGRES_SCHEMA", "testschema");

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

    // Support comma-separated schemas
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
    Log.Information("✓ Database connections validated");
    Log.Information("");

    var baseDir = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) ?? ".";
    var oracleQueriesPath = props.Get("ORACLE_QUERIES_PATH", Path.Combine(baseDir, "queries", "oracle"));
    var postgresQueriesPath = props.Get("POSTGRES_QUERIES_PATH", Path.Combine(baseDir, "queries", "postgres"));

    Log.Information("Oracle queries path: {Path}", oracleQueriesPath);
    Log.Information("PostgreSQL queries path: {Path}", postgresQueriesPath);
    Log.Information("");

    bool hasAnyFailures = false;
    bool hasAnyWarnings = false;
    var allSchemaResults = new List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)>();
    var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
    var reportsDir = props.GetReportsDirectory("Ora2PgPerformanceValidator");

    for (int i = 0; i < oracleSchemas.Length; i++)
    {
        string oracleSchema = oracleSchemas[i];
        string postgresSchema = postgresSchemas[i];

        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Schema Pair {Index}/{Total}: {OracleSchema} → {PostgresSchema}", 
            i + 1, oracleSchemas.Length, oracleSchema, postgresSchema);
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("");

        var queryParameters = new Dictionary<string, string>
        {
            { "ORACLE_SCHEMA", oracleSchema },
            { "POSTGRES_SCHEMA", postgresSchema }
        };

        Log.Information("Query parameters:");
        foreach (var (key, value) in queryParameters)
        {
            Log.Information("  {Key}: {Value}", key, value);
        }
        Log.Information("");

        var loader = new QueryLoader();
        var queryPairs = loader.LoadQueryPairs(oracleQueriesPath, postgresQueriesPath, queryParameters);

        if (queryPairs.Count == 0)
        {
            Log.Warning("No query pairs found. Please add .sql files to the queries/oracle and queries/postgres directories.");
            Log.Information("Query file pairs should have the same name (e.g., 01_list_tables.sql)");
            continue;
        }

        Log.Information("Found {Count} static query pairs to test", queryPairs.Count);
        Log.Information("");

        var objectFilter = ObjectFilter.FromProperties(props);

        var enableTableTests = props.GetBool("ENABLE_TABLE_PERFORMANCE_TESTS", true);
        var maxTablesToTest = props.GetInt("MAX_TABLES_TO_TEST", 100);
        var sampleRowLimit = props.GetInt("SAMPLE_ROW_LIMIT", 100);
        var enableCountTests = props.GetBool("ENABLE_COUNT_TESTS", true);
        var enableSampleTests = props.GetBool("ENABLE_SAMPLE_TESTS", true);
        var enablePkLookupTests = props.GetBool("ENABLE_PK_LOOKUP_TESTS", true);
        var enableOrderedScanTests = props.GetBool("ENABLE_ORDERED_SCAN_TESTS", true);

        if (enableTableTests)
        {
            Log.Information("═══════════════════════════════════════════════════════════");
            Log.Information("  DISCOVERING TABLES FOR PERFORMANCE TESTING");
            Log.Information("═══════════════════════════════════════════════════════════");
            Log.Information("");

            var tableDiscovery = new TableDiscovery(
                oracleConfig.GetOracleConnectionString(),
                postgresConfig.GetPostgresConnectionString(),
                oracleSchema,
                postgresSchema);

            var oracleTables = await tableDiscovery.DiscoverOracleTablesAsync();
            var postgresTables = await tableDiscovery.DiscoverPostgresTablesAsync();
            
            Log.Information("Found {OracleCount} tables in Oracle schema {Schema}", oracleTables.Count, oracleSchema);
            Log.Information("Found {PostgresCount} tables in PostgreSQL schema {Schema}", postgresTables.Count, postgresSchema);
            
            var commonTables = oracleTables.Where(ot =>
                postgresTables.Any(pt => pt.Name.Equals(ot.Name, StringComparison.OrdinalIgnoreCase))
            ).ToList();
            
            Log.Information("Found {CommonCount} common tables to test", commonTables.Count);
            
            var beforeExclusionCount = commonTables.Count;
            commonTables = commonTables.Where(t => !objectFilter.IsTableExcluded(t.Name, oracleSchema)).ToList();
            var excludedCount = beforeExclusionCount - commonTables.Count;
            
            if (excludedCount > 0)
            {
                Log.Information("Excluded {ExcludedCount} table(s) based on exclusion filters", excludedCount);
                Log.Information("{RemainingCount} table(s) remain after exclusions", commonTables.Count);
            }
            
            if (commonTables.Count == 0)
            {
                Log.Warning("No common tables found between Oracle and PostgreSQL schemas");
                Log.Warning("Skipping table performance tests");
                oracleTables = new List<TableInfo>();
            }
            else
            {
                oracleTables = commonTables;
                
                if (oracleTables.Count > maxTablesToTest)
                {
                    Log.Warning("Limiting to {Max} tables (out of {Total} common tables)", maxTablesToTest, oracleTables.Count);
                    oracleTables = oracleTables.Take(maxTablesToTest).ToList();
                }
            }

            Log.Information("");
            Log.Information("Table Performance Test Settings:");
            Log.Information("  Count Tests: {Enabled}", enableCountTests);
            Log.Information("  Sample Tests: {Enabled}", enableSampleTests);
            Log.Information("  PK Lookup Tests: {Enabled}", enablePkLookupTests);
            Log.Information("  Ordered Scan Tests: {Enabled}", enableOrderedScanTests);
            Log.Information("  Sample Row Limit: {Limit}", sampleRowLimit);
            Log.Information("");

            var queryGenerator = new TablePerformanceQueryGenerator(oracleSchema, postgresSchema);
            var tableQueries = queryGenerator.GenerateQueries(
                oracleTables,
                enableCountTests,
                enableSampleTests,
                enablePkLookupTests,
                enableOrderedScanTests,
                sampleRowLimit);

            Log.Information("Generated {Count} table performance queries", tableQueries.Count);
            Log.Information("");

            foreach (var (name, oracleQuery, postgresQuery, category) in tableQueries)
            {
                if (category == "pk_lookup")
                {
                    var tableName = name.Replace("table_pk_lookup_", "");
                    var table = oracleTables.FirstOrDefault(t => t.Name == tableName);
                    
                    if (table?.PrimaryKey != null)
                    {
                        var pkValue = await tableDiscovery.GetSamplePrimaryKeyValueAsync(tableName, table.PrimaryKey);
                        if (pkValue != null)
                        {
                            var (oracleQueryWithValue, postgresQueryWithValue) = 
                                queryGenerator.SubstitutePkValue(oracleQuery, postgresQuery, pkValue);
                            queryPairs[name] = (oracleQueryWithValue, postgresQueryWithValue);
                        }
                        else
                        {
                            Log.Warning("  Skipping PK lookup test for {Table} - no sample value found", tableName);
                        }
                    }
                }
                else
                {
                    queryPairs[name] = (oracleQuery, postgresQuery);
                }
            }

            Log.Information("Total queries to execute: {Count}", queryPairs.Count);
            Log.Information("");
        }
        else
        {
            Log.Information("Table performance testing disabled (ENABLE_TABLE_PERFORMANCE_TESTS=false)");
            Log.Information("");
        }

        var warmupRuns = props.GetInt("PERF_WARMUP_RUNS", 1);
        var measurementRuns = props.GetInt("PERF_MEASUREMENT_RUNS", 3);
        var thresholdPercent = props.GetInt("PERF_THRESHOLD_PERCENT", 50);
        
        Log.Information("Performance test settings:");
        Log.Information("  Warmup runs: {Warmup}", warmupRuns);
        Log.Information("  Measurement runs: {Measurement}", measurementRuns);
        Log.Information("  Performance threshold: {Threshold}%", thresholdPercent);
        Log.Information("");

        var executor = new QueryExecutor(
            oracleConfig.GetOracleConnectionString(),
            postgresConfig.GetPostgresConnectionString(),
            objectFilter,
            oracleSchema,
            postgresSchema,
            warmupRuns,
            measurementRuns,
            thresholdPercent);

        var summary = new PerformanceTestSummary
        {
            TestStartTime = DateTime.Now,
            TotalQueries = queryPairs.Count,
            ThresholdPercent = thresholdPercent
        };

        var testStopwatch = Stopwatch.StartNew();

        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  EXECUTING PERFORMANCE TESTS");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("");

        foreach (var (queryName, (oracleQuery, postgresQuery)) in queryPairs)
        {
            var result = await executor.ExecuteQueryPairAsync(queryName, oracleQuery, postgresQuery);
            summary.Results.Add(result);

            switch (result.Status)
            {
                case PerformanceStatus.Passed:
                    summary.PassedQueries++;
                    break;
                case PerformanceStatus.Warning:
                    summary.WarningQueries++;
                    break;
                case PerformanceStatus.Failed:
                    summary.FailedQueries++;
                    break;
                case PerformanceStatus.RowCountMismatch:
                    summary.RowCountMismatchQueries++;
                    break;
            }

            Log.Information("");
        }

        testStopwatch.Stop();
        summary.TestEndTime = DateTime.Now;
        summary.TotalTestDurationMs = testStopwatch.Elapsed.TotalMilliseconds;

        var successfulResults = summary.Results.Where(r => r.OracleExecuted && r.PostgresExecuted).ToList();
        if (successfulResults.Any())
        {
            summary.AverageOracleExecutionTimeMs = successfulResults.Average(r => r.OracleExecutionTimeMs);
            summary.AveragePostgresExecutionTimeMs = successfulResults.Average(r => r.PostgresExecutionTimeMs);
        }

        allSchemaResults.Add((oracleSchema, postgresSchema, summary));

        var reportWriter = new PerformanceReportWriter();
        reportWriter.WriteConsoleReport(summary);
        
        var schemaPrefix = $"{oracleSchema.ToLower()}-";

        var markdownPath = Path.Combine(reportsDir, $"{schemaPrefix}performance-validation-{timestamp}.md");
        reportWriter.WriteMarkdownReport(summary, markdownPath);

        var htmlPath = Path.Combine(reportsDir, $"{schemaPrefix}performance-validation-{timestamp}.html");
        reportWriter.WriteHtmlReport(summary, htmlPath);

        var txtPath = Path.Combine(reportsDir, $"{schemaPrefix}performance-validation-{timestamp}.txt");
        reportWriter.WriteTextReport(summary, txtPath);

        if (summary.FailedQueries > 0 || summary.RowCountMismatchQueries > 0)
        {
            hasAnyFailures = true;
        }
        else if (summary.WarningQueries > 0)
        {
            hasAnyWarnings = true;
        }

        Log.Information("✓ Schema pair validation complete");
        Log.Information("");
    }

    if (oracleSchemas.Length > 1)
    {
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  Multi-Schema Validation Summary");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("Total schema pairs validated: {Count}", oracleSchemas.Length);
        
        Log.Information("📝 Generating multi-schema summary report...");
        var summaryWriter = new MultiSchemaSummaryWriter();
        summaryWriter.WriteSummaryReport(allSchemaResults, reportsDir, timestamp);
        
        if (hasAnyFailures)
        {
            Log.Error("⚠️ One or more schemas have performance failures");
        }
        else if (hasAnyWarnings)
        {
            Log.Warning("⚠️ One or more schemas have performance warnings");
        }
        else
        {
            Log.Information("✓ All schemas validated successfully");
        }
    }

    connectionManager.Dispose();

    if (hasAnyFailures)
    {
        Log.Error("❌ Performance validation completed with failures");
        Environment.Exit(1);
    }
    else if (hasAnyWarnings)
    {
        Log.Warning("⚠ Performance validation completed with warnings");
        Environment.Exit(0);
    }
    else
    {
        Log.Information("✓ Performance validation completed successfully");
        Environment.Exit(0);
    }
}
catch (Exception ex)
{
    Log.Fatal(ex, "Fatal error during performance validation");
    Environment.Exit(1);
}
