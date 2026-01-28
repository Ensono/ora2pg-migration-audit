using System.Diagnostics;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using Serilog;
using Ora2PgPerformanceValidator.Models;

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

    public QueryExecutor(
        string oracleConnectionString, 
        string postgresConnectionString,
        int warmupRuns = 1,
        int measurementRuns = 3,
        int thresholdPercent = 50)
    {
        _oracleConnectionString = oracleConnectionString;
        _postgresConnectionString = postgresConnectionString;
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
        for (int i = 0; i < _warmupRuns; i++)
        {
            await using var warmupConn = new OracleConnection(_oracleConnectionString);
            await warmupConn.OpenAsync();
            await using var warmupCmd = new OracleCommand(query, warmupConn);
            warmupCmd.CommandTimeout = DefaultCommandTimeoutSeconds;
            await warmupCmd.ExecuteNonQueryAsync();
        }

        var times = new List<double>();
        long rowCount = 0;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var conn = new OracleConnection(_oracleConnectionString);
            await conn.OpenAsync();
            await using var cmd = new OracleCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            while (await reader.ReadAsync())
            {
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
        for (int i = 0; i < _warmupRuns; i++)
        {
            await using var warmupConn = new NpgsqlConnection(_postgresConnectionString);
            await warmupConn.OpenAsync();
            await using var warmupCmd = new NpgsqlCommand(query, warmupConn);
            warmupCmd.CommandTimeout = DefaultCommandTimeoutSeconds;
            await warmupCmd.ExecuteNonQueryAsync();
        }

        var times = new List<double>();
        long rowCount = 0;

        for (int i = 0; i < _measurementRuns; i++)
        {
            await using var conn = new NpgsqlConnection(_postgresConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(query, conn);
            cmd.CommandTimeout = DefaultCommandTimeoutSeconds;

            var sw = Stopwatch.StartNew();
            
            await using var reader = await cmd.ExecuteReaderAsync();
            long currentRowCount = 0;
            while (await reader.ReadAsync())
            {
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
        else
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
