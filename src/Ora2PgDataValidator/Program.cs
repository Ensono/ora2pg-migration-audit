using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataValidator.Processor;
using Ora2PgDataValidator.Writers;
using Ora2PgDataValidator.Comparison;
using Ora2Pg.Common.Util;
using Ora2PgDataValidator.src;

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

            ConfigurationValidator.ValidatePasswordSecurity(props);

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

        if (!ConfigurationValidator.ValidateSingleDatabaseModeConfig(props, targetDbStr))
        {
            Environment.Exit(1);
            return;
        }

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
            Log.Warning("No tables specified - discovering tables from database schema(s)");

            string schemasStr = targetDatabase == DatabaseType.Oracle
                ? props.Get("ORACLE_SCHEMA", "")
                : props.Get("POSTGRES_SCHEMA", "");

            if (string.IsNullOrWhiteSpace(schemasStr))
            {
                Log.Error("✗ No schema specified. Set ORACLE_SCHEMA or POSTGRES_SCHEMA in .env");
                Environment.Exit(1);
                return;
            }

            var schemas = schemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            
            if (schemas.Length > 1)
            {
                Log.Information("Multi-schema mode: Discovering tables from {Count} schemas", schemas.Length);
            }

            tables = new List<string>();
            foreach (var schema in schemas)
            {
                var schemaTables = connectionManager.GetTablesInSchema(targetDatabase, schema);
                Log.Information("  Discovered {Count} tables in schema {Schema}", schemaTables.Count, schema);
                
                foreach (var table in schemaTables)
                {
                    tables.Add($"{schema}.{table}");
                }
            }
            
            Log.Information("Total: Discovered {Count} tables across all schemas", tables.Count);
        }

        var tableFilter = ObjectFilter.FromProperties(props);
        var filteredTables = tableFilter.FilterTables(tables);
        var excludedCount = tables.Count - filteredTables.Count;
        if (excludedCount > 0)
        {
            Log.Information("Excluded {Count} table(s) based on table exclusion patterns or ignored objects", excludedCount);
        }

        tables = filteredTables;

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

        if (!ConfigurationValidator.ValidateComparisonModeConfig(props))
        {
            Environment.Exit(1);
            return;
        }

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

        if (!ConfigurationValidator.ValidateComparisonTargets(props))
        {
            Environment.Exit(1);
            return;
        }

        string tablesConfig = props.Get("TABLES_TO_COMPARE", props.Get("tables.to.compare", ""));

        Dictionary<string, string> tableMapping;

        if (string.IsNullOrWhiteSpace(tablesConfig))
        {
            tableMapping = new Dictionary<string, string>();
        }
        else if (tablesConfig.Trim().Equals("ALL", StringComparison.OrdinalIgnoreCase))
        {
            Log.Information("");
            Log.Information("5. Discovering all common tables between Oracle and PostgreSQL...");
            
            string oracleSchemasStr = props.Get("ORACLE_SCHEMA", "");
            string postgresSchemasStr = props.Get("POSTGRES_SCHEMA", "");

            if (string.IsNullOrWhiteSpace(oracleSchemasStr) || string.IsNullOrWhiteSpace(postgresSchemasStr))
            {
                Log.Error("✗ Schema names required for auto-discovery");
                Log.Error("  Set ORACLE_SCHEMA and POSTGRES_SCHEMA in .env");
                Environment.Exit(1);
                return;
            }

            var oracleSchemas = oracleSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var postgresSchemas = postgresSchemasStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            Log.Information("Parsed {OracleCount} Oracle schemas:", oracleSchemas.Length);
            for (int idx = 0; idx < oracleSchemas.Length; idx++)
            {
                Log.Information("  [{Index}] '{Schema}'", idx, oracleSchemas[idx]);
            }
            
            Log.Information("Parsed {PostgresCount} PostgreSQL schemas:", postgresSchemas.Length);
            for (int idx = 0; idx < postgresSchemas.Length; idx++)
            {
                Log.Information("  [{Index}] '{Schema}'", idx, postgresSchemas[idx]);
            }

            if (oracleSchemas.Length != postgresSchemas.Length)
            {
                Log.Error("✗ Number of Oracle schemas ({OracleCount}) must match number of PostgreSQL schemas ({PostgresCount})", 
                    oracleSchemas.Length, postgresSchemas.Length);
                Log.Error("  ORACLE_SCHEMA: {OracleSchemas}", oracleSchemasStr);
                Log.Error("  POSTGRES_SCHEMA: {PostgresSchemas}", postgresSchemasStr);
                Log.Error("  Schemas must be provided in matching order, comma-separated");
                Environment.Exit(1);
                return;
            }

            if (oracleSchemas.Length > 1)
            {
                Log.Information("Multi-schema mode: Testing {Count} schema pairs", oracleSchemas.Length);
            }

            var tableFilter = ObjectFilter.FromProperties(props);

            var schemaMappings = new List<(string OracleSchema, string PostgresSchema, Dictionary<string, string> TableMapping)>();
            
            for (int i = 0; i < oracleSchemas.Length; i++)
            {
                string oracleSchema = oracleSchemas[i];
                string postgresSchema = postgresSchemas[i];

                Log.Information("  Discovering tables in schema pair: {OracleSchema} → {PostgresSchema}", 
                    oracleSchema, postgresSchema);

                var oracleTables = connectionManager.GetTablesInSchema(DatabaseType.Oracle, oracleSchema);
                var postgresTables = connectionManager.GetTablesInSchema(DatabaseType.PostgreSQL, postgresSchema);

                var schemaTableMapping = new Dictionary<string, string>();
                int pairCount = 0;
                int excludedCount = 0;
                
                foreach (var oracleTable in oracleTables)
                {
                    var matchingPostgresTable = postgresTables
                        .FirstOrDefault(pt => string.Equals(pt, oracleTable, StringComparison.OrdinalIgnoreCase));
                    
                    if (matchingPostgresTable != null)
                    {
                        string oracleRef = $"{oracleSchema}.{oracleTable}";
                        string postgresRef = $"{postgresSchema}.{matchingPostgresTable}";
                        
                        if (tableFilter.IsTableExcluded(oracleRef, oracleSchema) ||
                            tableFilter.IsTableExcluded(postgresRef, postgresSchema))
                        {
                            excludedCount++;
                            continue;
                        }
                        
                        schemaTableMapping[oracleRef] = postgresRef;
                        pairCount++;
                    }
                }

                Log.Information("    Found {Count} common tables in this schema pair", pairCount);
                if (excludedCount > 0)
                {
                    Log.Information("    Excluded {Count} table(s) based on exclusion patterns", excludedCount);
                }
                schemaMappings.Add((oracleSchema, postgresSchema, schemaTableMapping));
            }

            Log.Information("Total: Found {Count} common tables across all schemas", 
                schemaMappings.Sum(sm => sm.TableMapping.Count));

            var processorForAll = new ComparisonDatabaseProcessor(connectionManager);
            var allSchemaResults = new List<DataValidatorSummary>();
            
            Log.Information("");
            Log.Information("About to process {Count} schema mappings:", schemaMappings.Count);
            for (int idx = 0; idx < schemaMappings.Count; idx++)
            {
                Log.Information("  [{Index}] {Oracle} → {Postgres} ({Tables} tables)",
                    idx, schemaMappings[idx].OracleSchema, schemaMappings[idx].PostgresSchema, 
                    schemaMappings[idx].TableMapping.Count);
            }
            
            foreach (var (oracleSchema, postgresSchema, schemaTableMapping) in schemaMappings)
            {
                Log.Information("");
                Log.Information("═══════════════════════════════════════════════════════════");
                Log.Information("  Processing Schema: {OracleSchema} → {PostgresSchema}", oracleSchema, postgresSchema);
                Log.Information("  Table count: {Count}", schemaTableMapping.Count);
                Log.Information("═══════════════════════════════════════════════════════════");
                
                if (schemaTableMapping.Count == 0)
                {
                    Log.Warning("⚠️  Schema {Schema} has no common tables - skipping validation but adding to summary", oracleSchema);
                    allSchemaResults.Add(new DataValidatorSummary
                    {
                        OracleSchema = oracleSchema,
                        PostgresSchema = postgresSchema,
                        TotalTables = 0,
                        SuccessfulValidations = 0,
                        FailedValidations = 0,
                        Results = new List<ComparisonResult>()
                    });
                    continue;
                }
                
                var (results, successCount, failCount) = processorForAll.ProcessAndCompareTables(schemaTableMapping, oracleSchema);
                
                allSchemaResults.Add(new DataValidatorSummary
                {
                    OracleSchema = oracleSchema,
                    PostgresSchema = postgresSchema,
                    TotalTables = schemaTableMapping.Count,
                    SuccessfulValidations = successCount,
                    FailedValidations = failCount,
                    Results = results
                });
                
                Log.Information("✓ Added result for {Schema}. Total results so far: {Count}", 
                    oracleSchema, allSchemaResults.Count);
            }
            
            if (schemaMappings.Count > 1)
            {
                Log.Information("");
                Log.Information("═══════════════════════════════════════════════════════════");
                Log.Information("  Multi-Schema Validation Summary");
                Log.Information("═══════════════════════════════════════════════════════════");
                Log.Information("Total schema pairs validated: {Count}", schemaMappings.Count);
                Log.Information("Total results collected for summary: {Count}", allSchemaResults.Count);
                
                for (int i = 0; i < allSchemaResults.Count; i++)
                {
                    var r = allSchemaResults[i];
                    Log.Information("  [{Index}] {Oracle} → {Postgres} - {Tables} tables, {Success} success, {Fail} failed",
                        i, r.OracleSchema ?? "(null)", r.PostgresSchema ?? "(null)", 
                        r.TotalTables, r.SuccessfulValidations, r.FailedValidations);
                }
                
                Log.Information("📝 Generating multi-schema summary report...");
                var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
                var reportsDir = props.GetReportsDirectory("Ora2PgDataValidator");
                var summaryWriter = new MultiSchemaSummaryWriter();
                summaryWriter.WriteSummaryReport(allSchemaResults, reportsDir, timestamp);
            }

            connectionManager.Dispose();
            return;
        }
        else
        {
            Log.Information("");
            Log.Information("5. Parsing table mapping configuration...");

            tableMapping = CaseConverter.ParseAndNormalizeMapping(tablesConfig);
            Log.Information("Parsed {Count} table mappings", tableMapping.Count);

            var tableFilter = ObjectFilter.FromProperties(props);
            var filteredMapping = new Dictionary<string, string>();
            int excludedMappings = 0;
            foreach (var entry in tableMapping)
            {
                if (tableFilter.IsTableExcluded(entry.Key) || tableFilter.IsTableExcluded(entry.Value))
                {
                    excludedMappings++;
                    continue;
                }

                filteredMapping[entry.Key] = entry.Value;
            }

            if (excludedMappings > 0)
            {
                Log.Information("Excluded {Count} table mapping(s) based on table exclusion patterns or ignored objects", excludedMappings);
            }

            tableMapping = filteredMapping;
            
            var schemaGroups = tableMapping
                .GroupBy(kvp => {
                    var oracleSchema = kvp.Key.Contains('.') ? kvp.Key.Split('.')[0] : "";
                    return oracleSchema;
                })
                .ToList();
            
            if (schemaGroups.Count > 1)
            {
                Log.Information("Detected tables from {Count} schemas - will generate per-schema reports", schemaGroups.Count);
            }
        }

        string viewsConfig = props.Get("VIEWS_TO_COMPARE", "");
        Dictionary<string, (string targetView, DatabaseObjectType objectType)> allObjectMapping = 
            tableMapping.ToDictionary(kvp => kvp.Key, kvp => (kvp.Value, DatabaseObjectType.Table));

        if (!string.IsNullOrWhiteSpace(viewsConfig))
        {
            Log.Information("");
            Log.Information("6. Processing views configuration...");

            Dictionary<string, string> viewMapping;

            if (viewsConfig.Trim().Equals("ALL", StringComparison.OrdinalIgnoreCase))
            {
                Log.Information("Discovering all common views between Oracle and PostgreSQL...");
                
                string oracleSchema = props.Get("ORACLE_SCHEMA", "");
                string postgresSchema = props.Get("POSTGRES_SCHEMA", "");

                var oracleViews = connectionManager.GetViewsInSchema(DatabaseType.Oracle, oracleSchema);
                var postgresViews = connectionManager.GetViewsInSchema(DatabaseType.PostgreSQL, postgresSchema);

                viewMapping = new Dictionary<string, string>();
                foreach (var oracleView in oracleViews)
                {
                    var matchingPostgresView = postgresViews
                        .FirstOrDefault(pv => string.Equals(pv, oracleView, StringComparison.OrdinalIgnoreCase));
                    
                    if (matchingPostgresView != null)
                    {
                        string oracleRef = $"{oracleSchema}.{oracleView}";
                        string postgresRef = $"{postgresSchema}.{matchingPostgresView}";
                        viewMapping[oracleRef] = postgresRef;
                    }
                }

                Log.Information("Found {Count} common views", viewMapping.Count);
            }
            else
            {
                viewMapping = CaseConverter.ParseAndNormalizeMapping(viewsConfig);
                Log.Information("Parsed {Count} view mappings", viewMapping.Count);

                var objectFilter = ObjectFilter.FromProperties(props);
                var filteredViewMapping = new Dictionary<string, string>();
                int excludedViewMappings = 0;
                foreach (var entry in viewMapping)
                {
                    if (objectFilter.IsViewExcluded(entry.Key) || objectFilter.IsViewExcluded(entry.Value))
                    {
                        excludedViewMappings++;
                        continue;
                    }

                    filteredViewMapping[entry.Key] = entry.Value;
                }

                if (excludedViewMappings > 0)
                {
                    Log.Information("Excluded {Count} view mapping(s) based on view exclusion patterns or ignored objects", excludedViewMappings);
                }

                viewMapping = filteredViewMapping;
            }

            foreach (var kvp in viewMapping)
            {
                allObjectMapping[kvp.Key] = (kvp.Value, DatabaseObjectType.View);
            }

            Log.Information("Total objects to compare: {Tables} tables + {Views} views = {Total}", 
                tableMapping.Count, viewMapping.Count, allObjectMapping.Count);
        }

        Log.Information("");
        Log.Information("{Step}. Configuration:", allObjectMapping.Count > tableMapping.Count ? "7" : "6");
        Log.Information("   Hash Algorithm: {Algorithm}", props.Get("HASH_ALGORITHM", props.Get("hash.algorithm", "SHA256")));
        Log.Information("   Batch Size: {BatchSize}", props.GetInt("BATCH_SIZE", props.GetInt("batch.size", 5000)));
        Log.Information("   Save Hashes to CSV: {SaveCsv}", props.GetBool("SAVE_HASHES_TO_CSV", props.GetBool("save.hashes.to.csv", true)));
        Log.Information("   Max Rows Per Table: {MaxRows}", props.GetInt("MAX_ROWS_PER_TABLE", props.GetInt("max.rows.per.table", 0)));

        Log.Information("");
        Log.Information("{Step}. Connection Pool Status:", allObjectMapping.Count > tableMapping.Count ? "8" : "7");
        Log.Information("   {OracleStats}", connectionManager.GetPoolStats(DatabaseType.Oracle));
        Log.Information("   {PostgresStats}", connectionManager.GetPoolStats(DatabaseType.PostgreSQL));

        var processor = new ComparisonDatabaseProcessor(connectionManager);
        
        if (allObjectMapping.Count > tableMapping.Count)
        {
            var schemaObjectGroups = allObjectMapping
                .GroupBy(kvp => {
                    var oracleSchema = kvp.Key.Contains('.') ? kvp.Key.Split('.')[0] : "";
                    return oracleSchema;
                })
                .ToList();
            
            foreach (var schemaGroup in schemaObjectGroups)
            {
                var schemaName = schemaGroup.Key;
                var schemaObjects = schemaGroup.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                
                if (schemaObjectGroups.Count > 1)
                {
                    Log.Information("");
                    Log.Information("═══════════════════════════════════════════════════════════");
                    Log.Information("  Processing Schema: {Schema}", schemaName);
                    Log.Information("═══════════════════════════════════════════════════════════");
                }
                
                processor.ProcessAndCompareObjects(schemaObjects, schemaName);
            }
        }
        else
        {
            var schemaTableGroups = tableMapping
                .GroupBy(kvp => {
                    var oracleSchema = kvp.Key.Contains('.') ? kvp.Key.Split('.')[0] : "";
                    return oracleSchema;
                })
                .ToList();
            
            foreach (var schemaGroup in schemaTableGroups)
            {
                var schemaName = schemaGroup.Key;
                var schemaTables = schemaGroup.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                
                if (schemaTableGroups.Count > 1)
                {
                    Log.Information("");
                    Log.Information("═══════════════════════════════════════════════════════════");
                    Log.Information("  Processing Schema: {Schema}", schemaName);
                    Log.Information("═══════════════════════════════════════════════════════════");
                }
                
                processor.ProcessAndCompareTables(schemaTables, schemaName);
            }
        }
    }
}
