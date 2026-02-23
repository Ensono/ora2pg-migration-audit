using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using Serilog;
using Ora2PgPerformanceValidator.Models;
using Ora2Pg.Common.Util;

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

        try
        {
            var (execTime, rowCount) = await ExecuteOracleQueryAsync(oracleQuery);
            result.OracleExecuted = true;
            result.OracleExecutionTimeMs = execTime;
            result.OracleRowsAffected = rowCount;
            _logger.Information("  Oracle: {Time:F2}ms, {Rows} rows", execTime, rowCount);
        }
        catch (Exception ex)
        {
            result.OracleExecuted = false;
            result.OracleError = ex.Message;
            _logger.Error("  Oracle failed: {Error}", ex.Message);
        }

        try
        {
            var (execTime, rowCount) = await ExecutePostgresQueryAsync(postgresQuery);
            result.PostgresExecuted = true;
            result.PostgresExecutionTimeMs = execTime;
            result.PostgresRowsAffected = rowCount;
            _logger.Information("  PostgreSQL: {Time:F2}ms, {Rows} rows", execTime, rowCount);
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

    private async Task<(double executionTimeMs, long rowCount)> ExecuteOracleQueryAsync(string query)
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
        int? tableNameColumnIndex = null;
        bool checkedForTableColumn = false;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var cmd = new OracleCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            
            // Detect table_name column on first measurement run
            if (!checkedForTableColumn)
            {
                for (int col = 0; col < reader.FieldCount; col++)
                {
                    if (reader.GetName(col).Equals("TABLE_NAME", StringComparison.OrdinalIgnoreCase))
                    {
                        tableNameColumnIndex = col;
                        _logger.Debug("Detected TABLE_NAME column at index {Index} - applying ObjectFilter", col);
                        break;
                    }
                }
                checkedForTableColumn = true;
            }
            
            while (await reader.ReadAsync())
            {
                // If query returns table names, filter using ObjectFilter
                if (tableNameColumnIndex.HasValue)
                {
                    var tableName = reader.GetString(tableNameColumnIndex.Value);
                    if (_objectFilter.IsTableExcluded(tableName, _oracleSchema))
                    {
                        continue; // Skip excluded table
                    }
                }
                currentRowCount++;
            }
            
            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds);
            rowCount = currentRowCount; // Use last run's row count
        }

        return (GetMedian(times), rowCount);
    }

    private async Task<(double executionTimeMs, long rowCount)> ExecutePostgresQueryAsync(string query)
    {
        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();
        
        for (int i = 0; i < _warmupRuns; i++)
        {
            await using var warmupCmd = new NpgsqlCommand(query, conn);
            warmupCmd.CommandTimeout = DefaultCommandTimeoutSeconds;
            await using var warmupReader = await warmupCmd.ExecuteReaderAsync();
            while (await warmupReader.ReadAsync()) { }
        }

        var times = new List<double>();
        long rowCount = 0;
        int? tableNameColumnIndex = null;
        bool checkedForTableColumn = false;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var cmd = new NpgsqlCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            
            // Detect table_name column on first measurement run
            if (!checkedForTableColumn)
            {
                for (int col = 0; col < reader.FieldCount; col++)
                {
                    if (reader.GetName(col).Equals("table_name", StringComparison.OrdinalIgnoreCase))
                    {
                        tableNameColumnIndex = col;
                        _logger.Debug("Detected table_name column at index {Index} - applying ObjectFilter", col);
                        break;
                    }
                }
                checkedForTableColumn = true;
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
                currentRowCount++;
            }
            
            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds);
            rowCount = currentRowCount;
        }

        return (GetMedian(times), rowCount);
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
            result.Status = PerformanceStatus.RowCountMismatch;
            result.Notes = $"Row count mismatch: Oracle={result.OracleRowsAffected}, PostgreSQL={result.PostgresRowsAffected}";
            return;
        }

        var maxTime = Math.Max(result.OracleExecutionTimeMs, result.PostgresExecutionTimeMs);
        var minTime = Math.Min(result.OracleExecutionTimeMs, result.PostgresExecutionTimeMs);
        
        if (maxTime > 0)
        {
            result.PerformanceDifferencePercent = ((maxTime - minTime) / maxTime) * 100;
        }

        if (result.PerformanceDifferencePercent < _thresholdPercent)
        {
            result.Status = PerformanceStatus.Passed;
            result.Notes = "Performance within acceptable range";
        }
        else
        {
            result.Status = PerformanceStatus.Warning;
            var slower = result.OracleExecutionTimeMs > result.PostgresExecutionTimeMs ? "Oracle" : "PostgreSQL";
            result.Notes = $"Significant performance difference: {slower} is {result.PerformanceDifferencePercent:F1}% slower";
        }
    }
}
