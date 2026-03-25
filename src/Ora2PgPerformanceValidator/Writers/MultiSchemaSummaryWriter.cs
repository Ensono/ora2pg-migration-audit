using Ora2Pg.Common.Config;
using Ora2PgPerformanceValidator.Models;
using Serilog;

namespace Ora2PgPerformanceValidator.Writers;

public class MultiSchemaSummaryWriter
{
    public void WriteSummaryReport(
        List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults,
        string reportsDir,
        string timestamp)
    {
        if (allResults == null || allResults.Count == 0)
        {
            Log.Warning("No results to write in multi-schema summary report");
            return;
        }
        
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "").ToLower();
        var dbPrefix = !string.IsNullOrWhiteSpace(dbName) ? $"{dbName}-" : "";
        
        var summaryPath = Path.Combine(reportsDir, $"{dbPrefix}summary-performance-validation-{timestamp}.md");
        
        Directory.CreateDirectory(reportsDir);
        
        using var writer = new StreamWriter(summaryPath);
        
        WriteHeader(writer, allResults, timestamp);
        WriteOverallStatus(writer, allResults);
        WriteSchemaBreakdown(writer, allResults);
        WriteSlowestQueries(writer, allResults);
        WriteQuickLinks(writer, allResults, timestamp);
        
        Log.Information("📝 Multi-schema summary report generated: {Path}", summaryPath);
    }

    private void WriteHeader(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults, string timestamp)
    {
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "");
        
        writer.WriteLine("# Multi-Schema Performance Validation Summary");
        writer.WriteLine();
        writer.WriteLine($"**Database:** {dbName}");
        writer.WriteLine($"**Validation Date:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        writer.WriteLine($"**Total Schema Pairs:** {allResults.Count}");
        writer.WriteLine();
        
        writer.WriteLine("## Schemas Tested");
        writer.WriteLine();
        foreach (var (oracleSchema, postgresSchema, _) in allResults)
        {
            writer.WriteLine($"- Oracle: `{oracleSchema}` → PostgreSQL: `{postgresSchema}`");
        }
        writer.WriteLine();
    }

    private void WriteOverallStatus(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults)
    {
        var schemasWithFailures = allResults.Count(r => r.Summary.FailedQueries > 0 || r.Summary.RowCountMismatchQueries > 0);
        var schemasWithWarnings = allResults.Count(r => r.Summary.WarningQueries > 0 && r.Summary.FailedQueries == 0 && r.Summary.RowCountMismatchQueries == 0);
        var schemasClean = allResults.Count(r => r.Summary.PassedQueries > 0 && r.Summary.WarningQueries == 0 && r.Summary.FailedQueries == 0);
        
        var totalQueries = allResults.Sum(r => r.Summary.TotalQueries);
        var totalPassed = allResults.Sum(r => r.Summary.PassedQueries);
        var totalWarnings = allResults.Sum(r => r.Summary.WarningQueries);
        var totalFailed = allResults.Sum(r => r.Summary.FailedQueries);
        var totalMismatches = allResults.Sum(r => r.Summary.RowCountMismatchQueries);
        var avgOracleTime = allResults.Average(r => r.Summary.AverageOracleExecutionTimeMs);
        var avgPostgresTime = allResults.Average(r => r.Summary.AveragePostgresExecutionTimeMs);
        
        writer.WriteLine("## Overall Status");
        writer.WriteLine();
        writer.WriteLine($"| Metric | Count |");
        writer.WriteLine($"|--------|-------|");
        writer.WriteLine($"| ❌ Schemas with Failures | {schemasWithFailures} |");
        writer.WriteLine($"| ⚠️ Schemas with Warnings | {schemasWithWarnings} |");
        writer.WriteLine($"| ✅ Clean Schemas | {schemasClean} |");
        writer.WriteLine($"| **Total Queries** | **{totalQueries:N0}** |");
        writer.WriteLine($"| ✅ Passed Queries | {totalPassed:N0} |");
        writer.WriteLine($"| ⚠️ Warning Queries | {totalWarnings:N0} |");
        writer.WriteLine($"| ❌ Failed Queries | {totalFailed:N0} |");
        writer.WriteLine($"| ❌ Row Count Mismatches | {totalMismatches:N0} |");
        writer.WriteLine($"| **Avg Oracle Time (ms)** | **{avgOracleTime:F2}** |");
        writer.WriteLine($"| **Avg PostgreSQL Time (ms)** | **{avgPostgresTime:F2}** |");
        
        var avgPerformanceDiff = avgPostgresTime > 0 
            ? ((avgPostgresTime - avgOracleTime) / avgOracleTime * 100)
            : 0;
        var perfIcon = avgPerformanceDiff > 0 ? "⚠️" : "✅";
        writer.WriteLine($"| {perfIcon} **Avg Performance Difference** | **{avgPerformanceDiff:+0.00;-0.00;0}%** |");
        writer.WriteLine();
    }

    private void WriteSchemaBreakdown(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults)
    {
        writer.WriteLine("## Schema-by-Schema Breakdown");
        writer.WriteLine();
        writer.WriteLine("| Oracle Schema | PostgreSQL Schema | Queries | Passed | Warnings | Failed | Mismatches | Avg Oracle (ms) | Avg PostgreSQL (ms) | Status |");
        writer.WriteLine("|---------------|-------------------|---------|--------|----------|--------|------------|-----------------|---------------------|--------|");
        
        foreach (var (oracleSchema, postgresSchema, summary) in allResults)
        {
            var statusIcon = summary.FailedQueries > 0 || summary.RowCountMismatchQueries > 0 ? "❌" : 
                           summary.WarningQueries > 0 ? "⚠️" : "✅";
            var status = summary.FailedQueries > 0 || summary.RowCountMismatchQueries > 0 ? "FAILED" :
                        summary.WarningQueries > 0 ? "WARNING" : "PASSED";
            
            writer.WriteLine($"| {oracleSchema} | {postgresSchema} | {summary.TotalQueries:N0} | " +
                           $"{summary.PassedQueries:N0} | {summary.WarningQueries:N0} | " +
                           $"{summary.FailedQueries:N0} | {summary.RowCountMismatchQueries:N0} | " +
                           $"{summary.AverageOracleExecutionTimeMs:F2} | {summary.AveragePostgresExecutionTimeMs:F2} | " +
                           $"{statusIcon} {status} |");
        }
        writer.WriteLine();
    }

    private void WriteSlowestQueries(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults)
    {
        writer.WriteLine("## Query Performance Analysis (Across All Schemas)");
        writer.WriteLine();
        
        var allQueries = new List<(string Schema, string QueryName, double OracleTime, double PostgresTime, double DiffPercent)>();
        
        foreach (var (oracleSchema, postgresSchema, summary) in allResults)
        {
            foreach (var result in summary.Results)
            {
                if (result.OracleExecuted && result.PostgresExecuted)
                {
                    allQueries.Add((
                        oracleSchema,
                        result.QueryName,
                        result.OracleExecutionTimeMs,
                        result.PostgresExecutionTimeMs,
                        result.PerformanceDifferencePercent
                    ));
                }
            }
        }
        
        if (!allQueries.Any())
        {
            writer.WriteLine("No query results available.");
            writer.WriteLine();
            return;
        }
        
        writer.WriteLine("### Top 10 Slowest Queries (By Execution Time)");
        writer.WriteLine();
        writer.WriteLine("| Schema | Query | Oracle (ms) | PostgreSQL (ms) | Difference |");
        writer.WriteLine("|--------|-------|-------------|-----------------|------------|");
        
        var slowestQueries = allQueries
            .OrderByDescending(q => Math.Max(q.OracleTime, q.PostgresTime))
            .Take(10);
        
        foreach (var (schema, queryName, oracleTime, postgresTime, diffPercent) in slowestQueries)
        {
            var diffIcon = diffPercent > 0 ? "⚠️" : "✅";
            writer.WriteLine($"| {schema} | {queryName} | {oracleTime:F2} | {postgresTime:F2} | {diffIcon} {diffPercent:+0.00;-0.00;0}% |");
        }
        
        writer.WriteLine();
        
        var improvements = allQueries
            .Where(q => q.DiffPercent < 0)
            .OrderBy(q => q.DiffPercent)  // Most negative = biggest improvement
            .Take(10)
            .ToList();
        
        if (improvements.Any())
        {
            writer.WriteLine("### Top 10 Performance Improvements (PostgreSQL Faster)");
            writer.WriteLine();
            writer.WriteLine("| Schema | Query | Oracle (ms) | PostgreSQL (ms) | Improvement |");
            writer.WriteLine("|--------|-------|-------------|-----------------|-------------|");
            
            foreach (var (schema, queryName, oracleTime, postgresTime, diffPercent) in improvements)
            {
                writer.WriteLine($"| {schema} | {queryName} | {oracleTime:F2} | {postgresTime:F2} | ✅ {diffPercent:F2}% |");
            }
            
            writer.WriteLine();
        }
        
        var regressions = allQueries
            .Where(q => q.DiffPercent > 0)
            .OrderByDescending(q => q.DiffPercent)  // Most positive = biggest regression
            .Take(10)
            .ToList();
        
        if (regressions.Any())
        {
            writer.WriteLine("### Top 10 Performance Regressions (PostgreSQL Slower)");
            writer.WriteLine();
            writer.WriteLine("| Schema | Query | Oracle (ms) | PostgreSQL (ms) | Regression |");
            writer.WriteLine("|--------|-------|-------------|-----------------|------------|");
            
            foreach (var (schema, queryName, oracleTime, postgresTime, diffPercent) in regressions)
            {
                writer.WriteLine($"| {schema} | {queryName} | {oracleTime:F2} | {postgresTime:F2} | ⚠️ +{diffPercent:F2}% |");
            }
            
            writer.WriteLine();
        }
    }

    private void WriteQuickLinks(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, PerformanceTestSummary Summary)> allResults, string timestamp)
    {
        writer.WriteLine("## Quick Links to Individual Reports");
        writer.WriteLine();
        
        foreach (var (oracleSchema, postgresSchema, summary) in allResults)
        {
            var statusIcon = summary.FailedQueries > 0 || summary.RowCountMismatchQueries > 0 ? "❌" : 
                           summary.WarningQueries > 0 ? "⚠️" : "✅";
            var reportFile = $"{oracleSchema.ToLower()}-performance-validation-{timestamp}.md";
            
            writer.WriteLine($"- {statusIcon} [{oracleSchema} → {postgresSchema}]({reportFile})");
        }
        writer.WriteLine();
    }
}
