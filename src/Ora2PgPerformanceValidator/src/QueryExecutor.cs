using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using Serilog;
using Ora2PgPerformanceValidator.Models;
using Ora2Pg.Common.Util;
using Ora2Pg.Common.Config;

namespace Ora2PgPerformanceValidator.Executors;

public class QueryExecutor
{
    private const int DefaultCommandTimeoutSeconds = 300;
    
    private readonly string _oracleConnectionString;
    private readonly string _postgresConnectionString;
    private readonly ILogger _logger = Log.ForContext<QueryExecutor>();
    private readonly int _warmupRuns;
    private readonly int _measurementRuns;
    private readonly int _thresholdPercent;
    private readonly ObjectFilter _objectFilter;
    private readonly string _oracleSchema;
    private readonly string _postgresSchema;
    private readonly HashSet<string> _oracleSkipColumns;
    private readonly HashSet<string> _postgresSkipColumns;

    public QueryExecutor(
        string oracleConnectionString, 
        string postgresConnectionString,
        ObjectFilter objectFilter,
        string oracleSchema,
        string postgresSchema,
        int warmupRuns = 1,
        int measurementRuns = 3,
        int thresholdPercent = 50)
    {
        _oracleConnectionString = oracleConnectionString;
        _postgresConnectionString = postgresConnectionString;
        _objectFilter = objectFilter;
        _oracleSchema = oracleSchema;
        _postgresSchema = postgresSchema;
        _warmupRuns = warmupRuns;
        _measurementRuns = measurementRuns;
        _thresholdPercent = thresholdPercent;
        
        var props = ApplicationProperties.Instance;
        var oracleSkipColumnsConfig = props.Get("ORACLE_SKIP_COLUMNS", string.Empty);
        var postgresSkipColumnsConfig = props.Get("POSTGRES_SKIP_COLUMNS", string.Empty);
        
        _oracleSkipColumns = new HashSet<string>(
            oracleSkipColumnsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            StringComparer.OrdinalIgnoreCase
        );
        
        _postgresSkipColumns = new HashSet<string>(
            postgresSkipColumnsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            StringComparer.OrdinalIgnoreCase
        );
        
        // Always skip DMS-added rowid columns in PostgreSQL (added by DMS for tables without PK)
        _postgresSkipColumns.Add("rowid");
        _logger.Information("Auto-skipping DMS 'rowid' column in PostgreSQL performance queries");
        
        if (_oracleSkipColumns.Any())
        {
            _logger.Information("Oracle columns to skip: {Columns}", string.Join(", ", _oracleSkipColumns));
        }
        
        if (_postgresSkipColumns.Any())
        {
            _logger.Information("PostgreSQL columns to skip: {Columns}", string.Join(", ", _postgresSkipColumns));
        }
    }

    public async Task<QueryPerformanceResult> ExecuteQueryPairAsync(
        string queryName,
        string oracleQuery,
        string postgresQuery)
    {
        var result = new QueryPerformanceResult
        {
            QueryName = queryName
        };

        _logger.Information("Executing query pair: {QueryName}", queryName);
        
        var captureObjectNames = ShouldCaptureObjectNames(queryName);
        var isAggregate = IsAggregateQuery(queryName);

        try
        {
            var (execTime, rowCount, objectNames) = await ExecuteOracleQueryAsync(oracleQuery, captureObjectNames, isAggregate);
            result.OracleExecuted = true;
            result.OracleExecutionTimeMs = execTime;
            result.OracleRowsAffected = rowCount;
            result.OracleObjectNames = objectNames;
            _logger.Information("  Oracle: {Time:F2}ms, {Rows} rows{Objects}", 
                execTime, rowCount, 
                captureObjectNames ? $", captured {objectNames.Count} object names" : "");
        }
        catch (Exception ex)
        {
            result.OracleExecuted = false;
            result.OracleError = ex.Message;
            _logger.Error("  Oracle failed: {Error}", ex.Message);
        }

        try
        {
            var (execTime, rowCount, objectNames) = await ExecutePostgresQueryAsync(postgresQuery, captureObjectNames, isAggregate);
            result.PostgresExecuted = true;
            result.PostgresExecutionTimeMs = execTime;
            result.PostgresRowsAffected = rowCount;
            result.PostgresObjectNames = objectNames;
            _logger.Information("  PostgreSQL: {Time:F2}ms, {Rows} rows{Objects}", 
                execTime, rowCount,
                captureObjectNames ? $", captured {objectNames.Count} object names" : "");
        }
        catch (Exception ex)
        {
            result.PostgresExecuted = false;
            result.PostgresError = ex.Message;
            _logger.Error("  PostgreSQL failed: {Error}", ex.Message);
        }

        AnalyzeResults(result);

        return result;
    }

    private bool ShouldCaptureObjectNames(string queryName)
    {
        return IsIndexQuery(queryName) || IsSequenceQuery(queryName) || IsConstraintQuery(queryName);
    }

    private bool IsAggregateQuery(string queryName)
    {
        // Queries that return a single row with an aggregate COUNT value
        return queryName.StartsWith("count_", StringComparison.OrdinalIgnoreCase) ||
               queryName.EndsWith("_count", StringComparison.OrdinalIgnoreCase) ||
               queryName.Equals("count", StringComparison.OrdinalIgnoreCase) ||
               queryName.Contains("02_count_tables", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<(double executionTimeMs, long rowCount, List<string> objectNames)> ExecuteOracleQueryAsync(string query, bool captureObjectNames = false, bool isAggregateQuery = false)
    {
        await using var conn = new OracleConnection(_oracleConnectionString);
        await conn.OpenAsync();
        
        for (int i = 0; i < _warmupRuns; i++)
        {
            await using var warmupCmd = new OracleCommand(query, conn);
            warmupCmd.CommandTimeout = DefaultCommandTimeoutSeconds;
            await using var warmupReader = await warmupCmd.ExecuteReaderAsync();
            while (await warmupReader.ReadAsync()) { }
        }

        var times = new List<double>();
        long rowCount = 0;
        var objectNames = new List<string>();
        int? tableNameColumnIndex = null;
        int? columnNameColumnIndex = null;
        bool checkedForColumns = false;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var cmd = new OracleCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            
            // Detect filterable columns on first measurement run
            if (!checkedForColumns)
            {
                for (int col = 0; col < reader.FieldCount; col++)
                {
                    var colName = reader.GetName(col);
                    if (colName.Equals("TABLE_NAME", StringComparison.OrdinalIgnoreCase))
                    {
                        tableNameColumnIndex = col;
                        _logger.Debug("Detected TABLE_NAME column at index {Index} - applying ObjectFilter", col);
                    }
                    else if (colName.Equals("COLUMN_NAME", StringComparison.OrdinalIgnoreCase))
                    {
                        columnNameColumnIndex = col;
                        _logger.Debug("Detected COLUMN_NAME column at index {Index} - applying column skip filter", col);
                    }
                }
                checkedForColumns = true;
            }
            
            while (await reader.ReadAsync())
            {
                // For aggregate queries (COUNT queries), read the value from first column instead of counting rows
                if (isAggregateQuery && i == 0 && reader.FieldCount > 0 && !reader.IsDBNull(0))
                {
                    try
                    {
                        currentRowCount = Convert.ToInt64(reader.GetValue(0));
                        _logger.Debug("Oracle - Read aggregate value: {Count}", currentRowCount);
                        break;
                    }
                    catch (FormatException ex)
                    {
                        _logger.Warning("Expected aggregate query but got non-numeric value: {Error}. Treating as regular query.", ex.Message);
                        isAggregateQuery = false;
                    }
                }
                
                // If query returns table names, filter using ObjectFilter
                if (tableNameColumnIndex.HasValue)
                {
                    var tableName = reader.GetString(tableNameColumnIndex.Value);
                    if (_objectFilter.IsTableExcluded(tableName, _oracleSchema))
                    {
                        continue; // Skip excluded table
                    }
                }
                
                // If query returns column names, filter using skip columns
                if (columnNameColumnIndex.HasValue && !reader.IsDBNull(columnNameColumnIndex.Value))
                {
                    var columnName = reader.GetString(columnNameColumnIndex.Value);
                    if (_oracleSkipColumns.Contains(columnName))
                    {
                        continue; // Skip excluded column
                    }
                }

                if (captureObjectNames && i == 0 && reader.FieldCount > 0)
                {
                    try
                    {
                        // Try to read the first column as a string for object names
                        // Handle various data types that might not convert directly to string
                        object value = reader.GetValue(0);
                        var objectName = value?.ToString() ?? string.Empty;
                        if (!string.IsNullOrEmpty(objectName))
                        {
                            objectNames.Add(objectName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.Debug("Could not read object name from first column: {Error}", ex.Message);
                    }
                }
                
                currentRowCount++;
            }
            
            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds);
            rowCount = currentRowCount; // Use last run's row count
        }

        if (captureObjectNames && objectNames.Count > 0)
        {
            _logger.Debug("Oracle - Captured {Count} object names from first column", objectNames.Count);
        }

        return (GetMedian(times), rowCount, objectNames);
    }

    private async Task<(double executionTimeMs, long rowCount, List<string> objectNames)> ExecutePostgresQueryAsync(string query, bool captureObjectNames = false, bool isAggregateQuery = false)
    {
        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();
        
        // Set search_path to include the target schema
        await using (var setPathCmd = new NpgsqlCommand($"SET search_path TO {_postgresSchema}, public", conn))
        {
            await setPathCmd.ExecuteNonQueryAsync();
        }
        
        for (int i = 0; i < _warmupRuns; i++)
        {
            await using var warmupCmd = new NpgsqlCommand(query, conn);
            warmupCmd.CommandTimeout = DefaultCommandTimeoutSeconds;
            await using var warmupReader = await warmupCmd.ExecuteReaderAsync();
            while (await warmupReader.ReadAsync()) { }
        }

        var times = new List<double>();
        long rowCount = 0;
        var objectNames = new List<string>();
        int? tableNameColumnIndex = null;
        int? columnNameColumnIndex = null;
        bool checkedForColumns = false;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var cmd = new NpgsqlCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            
            // Detect filterable columns on first measurement run
            if (!checkedForColumns)
            {
                for (int col = 0; col < reader.FieldCount; col++)
                {
                    var colName = reader.GetName(col);
                    if (colName.Equals("table_name", StringComparison.OrdinalIgnoreCase))
                    {
                        tableNameColumnIndex = col;
                        _logger.Debug("Detected table_name column at index {Index} - applying ObjectFilter", col);
                    }
                    else if (colName.Equals("column_name", StringComparison.OrdinalIgnoreCase))
                    {
                        columnNameColumnIndex = col;
                        _logger.Debug("Detected column_name column at index {Index} - applying column skip filter", col);
                    }
                }
                checkedForColumns = true;
            }
            
            while (await reader.ReadAsync())
            {
                // If query returns table names, filter using ObjectFilter
                if (tableNameColumnIndex.HasValue)
                {
                    var tableName = reader.GetString(tableNameColumnIndex.Value);
                    if (_objectFilter.IsTableExcluded(tableName, _postgresSchema))
                    {
                        continue; // Skip excluded table
                    }
                }
                
                // If query returns column names, filter using skip columns
                if (columnNameColumnIndex.HasValue && !reader.IsDBNull(columnNameColumnIndex.Value))
                {
                    var columnName = reader.GetString(columnNameColumnIndex.Value);
                    if (_postgresSkipColumns.Contains(columnName))
                    {
                        continue; // Skip excluded column
                    }
                }

                if (captureObjectNames && i == 0 && reader.FieldCount > 0)
                {
                    try
                    {
                        // Try to read the first column as a string for object names
                        // Handle various data types that might not convert directly to string
                        object value = reader.GetValue(0);
                        var objectName = value?.ToString() ?? string.Empty;
                        if (!string.IsNullOrEmpty(objectName))
                        {
                            objectNames.Add(objectName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.Debug("Could not read object name from first column: {Error}", ex.Message);
                    }
                }
                
                currentRowCount++;
            }
            
            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds);
            rowCount = currentRowCount;
        }

        if (captureObjectNames && objectNames.Count > 0)
        {
            _logger.Debug("PostgreSQL - Captured {Count} object names from first column", objectNames.Count);
        }

        return (GetMedian(times), rowCount, objectNames);
    }

    private double GetMedian(List<double> values)
    {
        if (values.Count == 0) return 0;
        
        var sorted = values.OrderBy(x => x).ToList();
        int mid = sorted.Count / 2;
        
        if (sorted.Count % 2 == 0)
            return (sorted[mid - 1] + sorted[mid]) / 2.0;
        
        return sorted[mid];
    }

    private void AnalyzeResults(QueryPerformanceResult result)
    {
        if (!result.OracleExecuted || !result.PostgresExecuted)
        {
            result.Status = PerformanceStatus.Failed;
            result.Notes = "One or both queries failed to execute";
            return;
        }

        if (result.OracleRowsAffected != result.PostgresRowsAffected)
        {
            // Special handling for index queries - DMS adds extra rowid indexes
            if (IsIndexQuery(result.QueryName) && result.PostgresRowsAffected > result.OracleRowsAffected)
            {
                var extraObjects = AnalyzeDmsGeneratedObjects(result.OracleObjectNames, result.PostgresObjectNames, "index", "es");
                if (extraObjects.isDmsGenerated)
                {
                    result.Status = PerformanceStatus.Passed;
                    result.Notes = extraObjects.notes;
                    _logger.Information("  ℹ️  Index query: {Extra} extra rowid index(es) added by DMS migration - marking as PASSED", extraObjects.count);
                    return;
                }
            }
            
            // Special handling for sequence queries - DMS adds extra rowid sequences
            if (IsSequenceQuery(result.QueryName) && result.PostgresRowsAffected > result.OracleRowsAffected)
            {
                var extraObjects = AnalyzeDmsGeneratedObjects(result.OracleObjectNames, result.PostgresObjectNames, "sequence", "s");
                if (extraObjects.isDmsGenerated)
                {
                    result.Status = PerformanceStatus.Passed;
                    result.Notes = extraObjects.notes;
                    _logger.Information("  ℹ️  Sequence query: {Extra} extra rowid sequence(s) added by DMS migration - marking as PASSED", extraObjects.count);
                    return;
                }
            }
            
            // Special handling for constraint queries - DMS adds extra rowid primary keys
            if (IsConstraintQuery(result.QueryName) && result.PostgresRowsAffected > result.OracleRowsAffected)
            {
                var extraObjects = AnalyzeDmsGeneratedObjects(result.OracleObjectNames, result.PostgresObjectNames, "constraint", "s");
                if (extraObjects.isDmsGenerated)
                {
                    result.Status = PerformanceStatus.Passed;
                    result.Notes = extraObjects.notes;
                    _logger.Information("  ℹ️  Constraint query: {Extra} extra rowid constraint(s) added by DMS migration - marking as PASSED", extraObjects.count);
                    return;
                }
            }
            
            result.Status = PerformanceStatus.RowCountMismatch;
            result.Notes = $"Row count mismatch: Oracle={result.OracleRowsAffected}, PostgreSQL={result.PostgresRowsAffected}";
            return;
        }

        if (result.OracleExecutionTimeMs > 0)
        {
            result.PerformanceDifferencePercent = 
                ((result.PostgresExecutionTimeMs - result.OracleExecutionTimeMs) / result.OracleExecutionTimeMs) * 100;
        }

        if (result.PerformanceDifferencePercent <= 0)
        {
            result.Status = PerformanceStatus.Passed;
            if (result.PerformanceDifferencePercent < 0)
            {
                result.Notes = $"PostgreSQL is {Math.Abs(result.PerformanceDifferencePercent):F1}% faster than Oracle ✓";
            }
            else
            {
                result.Notes = "Performance matches Oracle";
            }
        }
        else if (result.PerformanceDifferencePercent < _thresholdPercent)
        {
            result.Status = PerformanceStatus.Passed;
            result.Notes = $"PostgreSQL is {result.PerformanceDifferencePercent:F1}% slower (within acceptable range)";
        }
        else
        {
            result.Status = PerformanceStatus.Warning;
            result.Notes = $"PostgreSQL is {result.PerformanceDifferencePercent:F1}% slower than Oracle";
        }
    }

    private (bool isDmsGenerated, int count, string notes) AnalyzeDmsGeneratedObjects(
        List<string> oracleObjects, 
        List<string> postgresObjects, 
        string objectType,
        string pluralSuffix)
    {
        _logger.Debug("Analyzing DMS objects - Oracle count: {OracleCount}, PostgreSQL count: {PostgresCount}", 
            oracleObjects.Count, postgresObjects.Count);
        
        if (oracleObjects.Count == 0 && postgresObjects.Count == 0)
        {
            _logger.Warning("No object names captured for {ObjectType} - unable to perform detailed DMS analysis", objectType);
            return (false, 0, string.Empty);
        }
        
        // Find extra objects in PostgreSQL that aren't in Oracle
        var extraObjects = postgresObjects.Except(oracleObjects, StringComparer.OrdinalIgnoreCase).ToList();
        
        _logger.Debug("Extra {ObjectType} in PostgreSQL: {Count} - {Objects}", 
            objectType, extraObjects.Count, string.Join(", ", extraObjects));
        
        if (extraObjects.Count == 0)
            return (false, 0, string.Empty);
        
        // Check if all extra objects contain "rowid" (DMS-generated pattern)
        var dmsGeneratedObjects = extraObjects.Where(obj => obj.Contains("rowid", StringComparison.OrdinalIgnoreCase)).ToList();
        
        _logger.Debug("DMS-generated {ObjectType}: {Count} - {Objects}", 
            objectType, dmsGeneratedObjects.Count, string.Join(", ", dmsGeneratedObjects));
        
        if (dmsGeneratedObjects.Count == extraObjects.Count)
        {
            // All extra objects are DMS-generated
            var plural = dmsGeneratedObjects.Count > 1 ? pluralSuffix : "";
            var objectNames = string.Join(", ", dmsGeneratedObjects);
            var notes = $"✅ PASSED (with {dmsGeneratedObjects.Count} extra DMS-generated rowid {objectType}{plural} in PostgreSQL - expected behavior)\n" +
                       $"Extra {objectType}{plural}: {objectNames}";
            return (true, dmsGeneratedObjects.Count, notes);
        }
        
        // Some extra objects are not DMS-generated - this is a real mismatch
        _logger.Warning("Row count mismatch includes non-DMS objects: {DmsCount}/{TotalCount} are DMS-generated", 
            dmsGeneratedObjects.Count, extraObjects.Count);
        return (false, 0, string.Empty);
    }

    private bool IsIndexQuery(string queryName)
    {
        return queryName.Contains("index", StringComparison.OrdinalIgnoreCase) ||
               queryName.Contains("03_list_indexes", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsSequenceQuery(string queryName)
    {
        return queryName.Contains("sequence", StringComparison.OrdinalIgnoreCase) ||
               queryName.Contains("06_list_sequences", StringComparison.OrdinalIgnoreCase);
    }

    private bool IsConstraintQuery(string queryName)
    {
        return queryName.Contains("constraint", StringComparison.OrdinalIgnoreCase) ||
               queryName.Contains("04_list_constraints", StringComparison.OrdinalIgnoreCase);
    }
}
