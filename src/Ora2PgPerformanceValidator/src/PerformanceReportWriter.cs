using Ora2PgPerformanceValidator.Models;
using Ora2Pg.Common.Writers;
using Serilog;

namespace Ora2PgPerformanceValidator.Writers;

public class PerformanceReportWriter : BaseHtmlReportWriter
{
    private readonly ILogger _logger = Log.ForContext<PerformanceReportWriter>();

    private static string GetPerformanceIndicator(double perfDiff, double threshold)
    {
        if (perfDiff < 0)
        {
            return "✓ faster";
        }
        else if (perfDiff <= threshold)
        {
            return "≈ slower (within threshold)";
        }
        else
        {
            return "⚠ slower";
        }
    }

    public void WriteConsoleReport(PerformanceTestSummary summary)
    {
        _logger.Information("");
        _logger.Information("═══════════════════════════════════════════════════════════");
        _logger.Information("  PERFORMANCE TEST SUMMARY");
        _logger.Information("═══════════════════════════════════════════════════════════");
        _logger.Information("Test Duration: {Duration:F2}s", summary.TotalTestDurationMs / 1000);
        _logger.Information("Total Queries: {Total}", summary.TotalQueries);
        _logger.Information("✓ Passed: {Passed}", summary.PassedQueries);
        _logger.Information("⚠ Warning: {Warning}", summary.WarningQueries);
        _logger.Information("❌ Failed: {Failed}", summary.FailedQueries);
        _logger.Information("🔴 Row Count Mismatch: {Mismatch}", summary.RowCountMismatchQueries);
        _logger.Information("");
        _logger.Information("Average Execution Time:");
        _logger.Information("  Oracle: {OracleAvg:F2}ms", summary.AverageOracleExecutionTimeMs);
        _logger.Information("  PostgreSQL: {PostgresAvg:F2}ms", summary.AveragePostgresExecutionTimeMs);
        
        // Calculate performance difference
        if (summary.AverageOracleExecutionTimeMs > 0)
        {
            var perfDiff = ((summary.AveragePostgresExecutionTimeMs - summary.AverageOracleExecutionTimeMs) / 
                           summary.AverageOracleExecutionTimeMs) * 100;
            var indicator = GetPerformanceIndicator(perfDiff, summary.ThresholdPercent);
            _logger.Information("  PostgreSQL vs Oracle: {Diff:F1}% {Indicator}", Math.Abs(perfDiff), indicator);
        }
        
        _logger.Information("");
        
        // Table performance breakdown
        var tableQueries = summary.Results.Where(r => 
            r.QueryName.StartsWith("table_count_") ||
            r.QueryName.StartsWith("table_sample_") ||
            r.QueryName.StartsWith("table_pk_lookup_") ||
            r.QueryName.StartsWith("table_ordered_")).ToList();
        
        if (tableQueries.Any())
        {
            _logger.Information("Table Performance Tests:");
            _logger.Information("  Total: {Count}", tableQueries.Count);
            _logger.Information("  ✓ Passed: {Passed}", tableQueries.Count(q => q.Status == PerformanceStatus.Passed));
            _logger.Information("  ⚠ Warning: {Warning}", tableQueries.Count(q => q.Status == PerformanceStatus.Warning));
            _logger.Information("  ❌ Failed: {Failed}", tableQueries.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch));
            
            // Breakdown by test type
            var countTests = tableQueries.Where(q => q.QueryName.StartsWith("table_count_")).ToList();
            var sampleTests = tableQueries.Where(q => q.QueryName.StartsWith("table_sample_")).ToList();
            var pkTests = tableQueries.Where(q => q.QueryName.StartsWith("table_pk_lookup_")).ToList();
            var orderedTests = tableQueries.Where(q => q.QueryName.StartsWith("table_ordered_")).ToList();
            
            _logger.Information("");
            _logger.Information("  By Test Type:");
            if (countTests.Any()) _logger.Information("    COUNT tests: {Count}", countTests.Count);
            if (sampleTests.Any()) _logger.Information("    SAMPLE tests: {Count}", sampleTests.Count);
            if (pkTests.Any()) _logger.Information("    PK LOOKUP tests: {Count}", pkTests.Count);
            if (orderedTests.Any()) _logger.Information("    ORDERED SCAN tests: {Count}", orderedTests.Count);
            
            // Show slowest tables
            var slowTables = tableQueries
                .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs > q.OracleExecutionTimeMs)
                .OrderByDescending(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
                .Take(5)
                .ToList();
            
            if (slowTables.Any())
            {
                _logger.Information("");
                _logger.Information("  Slowest Queries in PostgreSQL:");
                foreach (var query in slowTables)
                {
                    var diff = ((query.PostgresExecutionTimeMs - query.OracleExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                    _logger.Information("    {Query}: +{Diff:F1}% slower ({PgTime:F2}ms vs {OraTime:F2}ms)", 
                        query.QueryName.Replace("table_", ""), 
                        diff, 
                        query.PostgresExecutionTimeMs, 
                        query.OracleExecutionTimeMs);
                }
            }
        }
        
        _logger.Information("");
        _logger.Information("ℹ️  Performance Threshold: >{Threshold}% difference = Warning", summary.ThresholdPercent);
        _logger.Information("═══════════════════════════════════════════════════════════");
    }

    public void WriteMarkdownReport(PerformanceTestSummary summary, string filePath)
    {
        var lines = new List<string>();

        lines.Add("# Oracle to PostgreSQL Performance Validation Report");
        lines.Add("");
        lines.Add($"**Generated:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        
        if (!string.IsNullOrEmpty(summary.OracleDatabase))
        {
            lines.Add($"**Oracle Database:** {summary.OracleDatabase}");
        }
        if (!string.IsNullOrEmpty(summary.PostgresDatabase))
        {
            lines.Add($"**PostgreSQL Database:** {summary.PostgresDatabase}");
        }
        if (!string.IsNullOrEmpty(summary.OracleSchema))
        {
            lines.Add($"**Oracle Schema:** {summary.OracleSchema}");
        }
        if (!string.IsNullOrEmpty(summary.PostgresSchema))
        {
            lines.Add($"**PostgreSQL Schema:** {summary.PostgresSchema}");
        }
        
        lines.Add($"**Test Duration:** {summary.TotalTestDurationMs / 1000:F2} seconds");
        lines.Add("");
        lines.Add($"> **Performance Threshold:** Queries with >{summary.ThresholdPercent}% execution time difference are flagged as warnings.");
        lines.Add("");

        // Summary table
        lines.Add("## Summary");
        lines.Add("");
        lines.Add("| Metric | Count |");
        lines.Add("|--------|-------|");
        lines.Add($"| Total Queries | {summary.TotalQueries} |");
        lines.Add($"| ✓ Passed | {summary.PassedQueries} |");
        lines.Add($"| ⚠ Warning | {summary.WarningQueries} |");
        lines.Add($"| ❌ Failed | {summary.FailedQueries} |");
        lines.Add($"| 🔴 Row Count Mismatch | {summary.RowCountMismatchQueries} |");
        lines.Add("");
        
        // Performance comparison
        if (summary.AverageOracleExecutionTimeMs > 0)
        {
            var perfDiff = ((summary.AveragePostgresExecutionTimeMs - summary.AverageOracleExecutionTimeMs) / 
                           summary.AverageOracleExecutionTimeMs) * 100;
            var indicator = GetPerformanceIndicator(perfDiff, summary.ThresholdPercent);
            lines.Add("| **Performance Metric** | **Value** |");
            lines.Add("|------------------------|-----------|");
            lines.Add($"| Average Oracle Time | {summary.AverageOracleExecutionTimeMs:F2}ms |");
            lines.Add($"| Average PostgreSQL Time | {summary.AveragePostgresExecutionTimeMs:F2}ms |");
            lines.Add($"| **PostgreSQL vs Oracle** | **{Math.Abs(perfDiff):F1}% {indicator}** |");
            lines.Add("");
        }
        else
        {
            lines.Add($"| Average Oracle Time | {summary.AverageOracleExecutionTimeMs:F2}ms |");
            lines.Add($"| Average PostgreSQL Time | {summary.AveragePostgresExecutionTimeMs:F2}ms |");
            lines.Add("");
        }
        
        // Table performance breakdown
        var tableQueries = summary.Results.Where(r => 
            r.QueryName.StartsWith("table_count_") ||
            r.QueryName.StartsWith("table_sample_") ||
            r.QueryName.StartsWith("table_pk_lookup_") ||
            r.QueryName.StartsWith("table_ordered_")).ToList();
        
        if (tableQueries.Any())
        {
            lines.Add("## Table Performance Tests");
            lines.Add("");
            lines.Add("| Test Type | Total | Passed | Warning | Failed |");
            lines.Add("|-----------|-------|--------|---------|--------|");
            
            var countTests = tableQueries.Where(q => q.QueryName.StartsWith("table_count_")).ToList();
            var sampleTests = tableQueries.Where(q => q.QueryName.StartsWith("table_sample_")).ToList();
            var pkTests = tableQueries.Where(q => q.QueryName.StartsWith("table_pk_lookup_")).ToList();
            var orderedTests = tableQueries.Where(q => q.QueryName.StartsWith("table_ordered_")).ToList();
            
            if (countTests.Any())
                lines.Add($"| COUNT (Full Scan) | {countTests.Count} | {countTests.Count(q => q.Status == PerformanceStatus.Passed)} | {countTests.Count(q => q.Status == PerformanceStatus.Warning)} | {countTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)} |");
            
            if (sampleTests.Any())
                lines.Add($"| SAMPLE (LIMIT) | {sampleTests.Count} | {sampleTests.Count(q => q.Status == PerformanceStatus.Passed)} | {sampleTests.Count(q => q.Status == PerformanceStatus.Warning)} | {sampleTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)} |");
            
            if (pkTests.Any())
                lines.Add($"| PK LOOKUP (Index) | {pkTests.Count} | {pkTests.Count(q => q.Status == PerformanceStatus.Passed)} | {pkTests.Count(q => q.Status == PerformanceStatus.Warning)} | {pkTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)} |");
            
            if (orderedTests.Any())
                lines.Add($"| ORDERED SCAN (Sort) | {orderedTests.Count} | {orderedTests.Count(q => q.Status == PerformanceStatus.Passed)} | {orderedTests.Count(q => q.Status == PerformanceStatus.Warning)} | {orderedTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)} |");
            
            lines.Add($"| **TOTAL** | **{tableQueries.Count}** | **{tableQueries.Count(q => q.Status == PerformanceStatus.Passed)}** | **{tableQueries.Count(q => q.Status == PerformanceStatus.Warning)}** | **{tableQueries.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}** |");
            lines.Add("");
            
            // Top 10 slowest tables
            var slowTables = tableQueries
                .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs > q.OracleExecutionTimeMs)
                .OrderByDescending(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
                .Take(10)
                .ToList();
            
            if (slowTables.Any())
            {
                lines.Add("### Top 10 Slowest Queries in PostgreSQL");
                lines.Add("");
                lines.Add("| Query | Oracle Time | PostgreSQL Time | Difference | Status |");
                lines.Add("|-------|-------------|-----------------|------------|--------|");
                
                foreach (var query in slowTables)
                {
                    var diff = ((query.PostgresExecutionTimeMs - query.OracleExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                    var statusIcon = query.Status switch
                    {
                        PerformanceStatus.Passed => "✓",
                        PerformanceStatus.Warning => "⚠",
                        _ => "❌"
                    };
                    lines.Add($"| {query.QueryName.Replace("table_", "")} | {query.OracleExecutionTimeMs:F2}ms | {query.PostgresExecutionTimeMs:F2}ms | +{diff:F1}% | {statusIcon} |");
                }
                lines.Add("");
            }
            
            // Top 10 fastest tables
            var fastTables = tableQueries
                .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs < q.OracleExecutionTimeMs)
                .OrderBy(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
                .Take(10)
                .ToList();
            
            if (fastTables.Any())
            {
                lines.Add("### Top 10 Fastest Queries in PostgreSQL");
                lines.Add("");
                lines.Add("| Query | Oracle Time | PostgreSQL Time | Improvement | Status |");
                lines.Add("|-------|-------------|-----------------|-------------|--------|");
                
                foreach (var query in fastTables)
                {
                    var diff = ((query.OracleExecutionTimeMs - query.PostgresExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                    var statusIcon = query.Status switch
                    {
                        PerformanceStatus.Passed => "✓",
                        PerformanceStatus.Warning => "⚠",
                        _ => "❌"
                    };
                    lines.Add($"| {query.QueryName.Replace("table_", "")} | {query.OracleExecutionTimeMs:F2}ms | {query.PostgresExecutionTimeMs:F2}ms | -{diff:F1}% | {statusIcon} |");
                }
                lines.Add("");
            }
        }

        // Detailed results
        lines.Add("## Query Results");
        lines.Add("");

        foreach (var result in summary.Results.OrderBy(r => r.QueryName))
        {
            var statusIcon = result.Status switch
            {
                PerformanceStatus.Passed => "✓",
                PerformanceStatus.Warning => "⚠",
                PerformanceStatus.Failed => "❌",
                PerformanceStatus.RowCountMismatch => "🔴",
                _ => "?"
            };

            lines.Add($"### {statusIcon} {result.QueryName}");
            lines.Add("");
            lines.Add("| Database | Execution Time | Rows | Status |");
            lines.Add("|----------|----------------|------|--------|");
            
            var oracleStatus = result.OracleExecuted ? "✓" : "❌";
            var postgresStatus = result.PostgresExecuted ? "✓" : "❌";
            
            lines.Add($"| Oracle | {result.OracleExecutionTimeMs:F2}ms | {result.OracleRowsAffected} | {oracleStatus} |");
            lines.Add($"| PostgreSQL | {result.PostgresExecutionTimeMs:F2}ms | {result.PostgresRowsAffected} | {postgresStatus} |");
            lines.Add("");

            if (result.OracleExecuted && result.PostgresExecuted)
            {
                lines.Add($"**Performance Difference:** {result.PerformanceDifferencePercent:F1}%");
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.Notes))
            {
                lines.Add($"**Notes:**");
                lines.Add("");

                foreach (var line in result.Notes.Split('\n'))
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        lines.Add(line);
                    }
                }
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.OracleError))
            {
                lines.Add($"**Oracle Error:** {result.OracleError}");
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.PostgresError))
            {
                lines.Add($"**PostgreSQL Error:** {result.PostgresError}");
                lines.Add("");
            }

            lines.Add("---");
            lines.Add("");
        }

        File.WriteAllLines(filePath, lines);
        _logger.Information("📄 Markdown report saved to: {Path}", filePath);
    }

    public void WriteHtmlReport(PerformanceTestSummary summary, string filePath)
    {
        var html = GenerateHtmlReport(summary);
        File.WriteAllText(filePath, html);
        _logger.Information("📄 HTML report saved to: {Path}", filePath);
    }

    public void WriteTextReport(PerformanceTestSummary summary, string filePath)
    {
        var lines = new List<string>();

        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("  ORACLE TO POSTGRESQL PERFORMANCE VALIDATION REPORT");
        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("");
        lines.Add($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        lines.Add($"Test Duration: {summary.TotalTestDurationMs / 1000:F2} seconds");
        lines.Add("");
        lines.Add($"Performance Threshold: Queries with >{summary.ThresholdPercent}% execution time");
        lines.Add("difference are flagged as warnings.");
        lines.Add("");

        // Summary section
        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("  SUMMARY");
        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add($"Total Queries:          {summary.TotalQueries}");
        lines.Add($"✓ Passed:               {summary.PassedQueries}");
        lines.Add($"⚠ Warning:              {summary.WarningQueries}");
        lines.Add($"❌ Failed:              {summary.FailedQueries}");
        lines.Add($"🔴 Row Count Mismatch:  {summary.RowCountMismatchQueries}");
        lines.Add("");
        lines.Add("Average Execution Time:");
        lines.Add($"  Oracle:     {summary.AverageOracleExecutionTimeMs:F2}ms");
        lines.Add($"  PostgreSQL: {summary.AveragePostgresExecutionTimeMs:F2}ms");
        
        // Performance comparison
        if (summary.AverageOracleExecutionTimeMs > 0)
        {
            var perfDiff = ((summary.AveragePostgresExecutionTimeMs - summary.AverageOracleExecutionTimeMs) / 
                           summary.AverageOracleExecutionTimeMs) * 100;
            var indicator = GetPerformanceIndicator(perfDiff, summary.ThresholdPercent);
            lines.Add($"  Difference: {Math.Abs(perfDiff):F1}% {indicator}");
        }
        lines.Add("");

        // Table performance breakdown
        var tableQueries = summary.Results.Where(r => 
            r.QueryName.StartsWith("table_count_") ||
            r.QueryName.StartsWith("table_sample_") ||
            r.QueryName.StartsWith("table_pk_lookup_") ||
            r.QueryName.StartsWith("table_ordered_")).ToList();

        if (tableQueries.Any())
        {
            lines.Add("═══════════════════════════════════════════════════════════");
            lines.Add("  TABLE PERFORMANCE TESTS");
            lines.Add("═══════════════════════════════════════════════════════════");
            lines.Add("");
            
            var countTests = tableQueries.Where(q => q.QueryName.StartsWith("table_count_")).ToList();
            var sampleTests = tableQueries.Where(q => q.QueryName.StartsWith("table_sample_")).ToList();
            var pkTests = tableQueries.Where(q => q.QueryName.StartsWith("table_pk_lookup_")).ToList();
            var orderedTests = tableQueries.Where(q => q.QueryName.StartsWith("table_ordered_")).ToList();
            
            lines.Add("Test Type              Total    Passed   Warning  Failed");
            lines.Add("───────────────────────────────────────────────────────────");
            
            if (countTests.Any())
                lines.Add($"{"COUNT (Full Scan)",-20}   {countTests.Count,-6}   {countTests.Count(q => q.Status == PerformanceStatus.Passed),-6}   {countTests.Count(q => q.Status == PerformanceStatus.Warning),-6}   {countTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch),-6}");
            
            if (sampleTests.Any())
                lines.Add($"{"SAMPLE (LIMIT)",-20}   {sampleTests.Count,-6}   {sampleTests.Count(q => q.Status == PerformanceStatus.Passed),-6}   {sampleTests.Count(q => q.Status == PerformanceStatus.Warning),-6}   {sampleTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch),-6}");
            
            if (pkTests.Any())
                lines.Add($"{"PK LOOKUP (Index)",-20}   {pkTests.Count,-6}   {pkTests.Count(q => q.Status == PerformanceStatus.Passed),-6}   {pkTests.Count(q => q.Status == PerformanceStatus.Warning),-6}   {pkTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch),-6}");
            
            if (orderedTests.Any())
                lines.Add($"{"ORDERED SCAN (Sort)",-20}   {orderedTests.Count,-6}   {orderedTests.Count(q => q.Status == PerformanceStatus.Passed),-6}   {orderedTests.Count(q => q.Status == PerformanceStatus.Warning),-6}   {orderedTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch),-6}");
            
            lines.Add("───────────────────────────────────────────────────────────");
            lines.Add($"{"TOTAL",-20}   {tableQueries.Count,-6}   {tableQueries.Count(q => q.Status == PerformanceStatus.Passed),-6}   {tableQueries.Count(q => q.Status == PerformanceStatus.Warning),-6}   {tableQueries.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch),-6}");
            lines.Add("");
            
            // Top 10 slowest tables
            var slowTables = tableQueries
                .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs > q.OracleExecutionTimeMs)
                .OrderByDescending(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
                .Take(10)
                .ToList();
            
            if (slowTables.Any())
            {
                lines.Add("TOP 10 SLOWEST QUERIES IN POSTGRESQL");
                lines.Add("───────────────────────────────────────────────────────────");
                foreach (var query in slowTables)
                {
                    var diff = ((query.PostgresExecutionTimeMs - query.OracleExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                    var statusIcon = query.Status switch
                    {
                        PerformanceStatus.Passed => "✓",
                        PerformanceStatus.Warning => "⚠",
                        _ => "❌"
                    };
                    var queryName = query.QueryName.Replace("table_", "");
                    lines.Add($"{statusIcon} {queryName,-35}");
                    lines.Add($"  Oracle: {query.OracleExecutionTimeMs,8:F2}ms | PostgreSQL: {query.PostgresExecutionTimeMs,8:F2}ms | +{diff:F1}% slower");
                }
                lines.Add("");
            }
            
            // Top 10 fastest tables
            var fastTables = tableQueries
                .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs < q.OracleExecutionTimeMs)
                .OrderBy(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
                .Take(10)
                .ToList();
            
            if (fastTables.Any())
            {
                lines.Add("TOP 10 FASTEST QUERIES IN POSTGRESQL");
                lines.Add("───────────────────────────────────────────────────────────");
                foreach (var query in fastTables)
                {
                    var diff = ((query.OracleExecutionTimeMs - query.PostgresExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                    var statusIcon = query.Status switch
                    {
                        PerformanceStatus.Passed => "✓",
                        PerformanceStatus.Warning => "⚠",
                        _ => "❌"
                    };
                    var queryName = query.QueryName.Replace("table_", "");
                    lines.Add($"{statusIcon} {queryName,-35}");
                    lines.Add($"  Oracle: {query.OracleExecutionTimeMs,8:F2}ms | PostgreSQL: {query.PostgresExecutionTimeMs,8:F2}ms | -{diff:F1}% faster");
                }
                lines.Add("");
            }
        }

        // Detailed results
        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("  QUERY RESULTS");
        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("");

        foreach (var result in summary.Results.OrderBy(r => r.QueryName))
        {
            var statusIcon = result.Status switch
            {
                PerformanceStatus.Passed => "✓",
                PerformanceStatus.Warning => "⚠",
                PerformanceStatus.Failed => "❌",
                PerformanceStatus.RowCountMismatch => "🔴",
                _ => "?"
            };

            lines.Add($"{statusIcon} {result.QueryName}");
            lines.Add(new string('-', 60));
            
            var oracleStatus = result.OracleExecuted ? "✓" : "❌";
            var postgresStatus = result.PostgresExecuted ? "✓" : "❌";
            
            lines.Add($"Oracle:");
            lines.Add($"  Status:         {oracleStatus}");
            lines.Add($"  Execution Time: {result.OracleExecutionTimeMs:F2}ms");
            lines.Add($"  Rows:           {result.OracleRowsAffected}");
            
            lines.Add($"PostgreSQL:");
            lines.Add($"  Status:         {postgresStatus}");
            lines.Add($"  Execution Time: {result.PostgresExecutionTimeMs:F2}ms");
            lines.Add($"  Rows:           {result.PostgresRowsAffected}");
            lines.Add("");

            if (result.OracleExecuted && result.PostgresExecuted)
            {
                lines.Add($"Performance Difference: {result.PerformanceDifferencePercent:F1}%");
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.Notes))
            {
                lines.Add($"Notes:");

                foreach (var line in result.Notes.Split('\n'))
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        lines.Add($"  {line}");
                    }
                }
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.OracleError))
            {
                lines.Add($"Oracle Error: {result.OracleError}");
                lines.Add("");
            }

            if (!string.IsNullOrEmpty(result.PostgresError))
            {
                lines.Add($"PostgreSQL Error: {result.PostgresError}");
                lines.Add("");
            }

            lines.Add("");
        }

        lines.Add("═══════════════════════════════════════════════════════════");
        lines.Add("  END OF REPORT");
        lines.Add("═══════════════════════════════════════════════════════════");

        File.WriteAllLines(filePath, lines);
        _logger.Information("📄 Text report saved to: {Path}", filePath);
    }

    private string GenerateHtmlReport(PerformanceTestSummary summary)
    {
        var statusClass = summary.FailedQueries > 0 || summary.RowCountMismatchQueries > 0 ? "failed" : 
                         summary.WarningQueries > 0 ? "warning" : "success";

        // Calculate performance comparison for summary section
        var performanceComparisonRow = "";
        if (summary.AverageOracleExecutionTimeMs > 0)
        {
            var perfDiff = ((summary.AveragePostgresExecutionTimeMs - summary.AverageOracleExecutionTimeMs) / 
                           summary.AverageOracleExecutionTimeMs) * 100;
            var indicator = GetPerformanceIndicator(perfDiff, summary.ThresholdPercent);
            performanceComparisonRow = $"<tr><td><strong>PostgreSQL vs Oracle</strong></td><td><strong>{Math.Abs(perfDiff):F1}% {indicator}</strong></td></tr>";
        }

        var resultsHtml = string.Join("\n", summary.Results.OrderBy(r => r.QueryName).Select(result =>
        {
            var statusClass = result.Status switch
            {
                PerformanceStatus.Passed => "success",
                PerformanceStatus.Warning => "warning",
                PerformanceStatus.Failed => "failed",
                PerformanceStatus.RowCountMismatch => "failed",
                _ => ""
            };

            var statusIcon = result.Status switch
            {
                PerformanceStatus.Passed => "✓",
                PerformanceStatus.Warning => "⚠",
                PerformanceStatus.Failed => "❌",
                PerformanceStatus.RowCountMismatch => "🔴",
                _ => "?"
            };

            var oracleStatus = result.OracleExecuted ? "✓" : "❌";
            var postgresStatus = result.PostgresExecuted ? "✓" : "❌";

            var errorHtml = "";
            if (!string.IsNullOrEmpty(result.OracleError))
            {
                errorHtml += $"<div class='error-message'><strong>Oracle Error:</strong> {result.OracleError}</div>";
            }
            if (!string.IsNullOrEmpty(result.PostgresError))
            {
                errorHtml += $"<div class='error-message'><strong>PostgreSQL Error:</strong> {result.PostgresError}</div>";
            }

            var perfDiffHtml = "";
            if (result.OracleExecuted && result.PostgresExecuted)
            {
                perfDiffHtml = $"<div class='perf-diff'>Performance Difference: {result.PerformanceDifferencePercent:F1}%</div>";
            }

            var notesHtml = "";
            if (!string.IsNullOrEmpty(result.Notes))
            {
                var notesFormatted = result.Notes.Replace("\n", "<br>");
                notesHtml = $"<div class='notes'><strong>Notes:</strong><br>{notesFormatted}</div>";
            }

            return $@"
                <div class='query-result {statusClass}'>
                    <h3>{statusIcon} {result.QueryName}</h3>
                    <table>
                        <tr>
                            <th>Database</th>
                            <th>Execution Time</th>
                            <th>Rows</th>
                            <th>Status</th>
                        </tr>
                        <tr>
                            <td>Oracle</td>
                            <td>{result.OracleExecutionTimeMs:F2}ms</td>
                            <td>{result.OracleRowsAffected}</td>
                            <td>{oracleStatus}</td>
                        </tr>
                        <tr>
                            <td>PostgreSQL</td>
                            <td>{result.PostgresExecutionTimeMs:F2}ms</td>
                            <td>{result.PostgresRowsAffected}</td>
                            <td>{postgresStatus}</td>
                        </tr>
                    </table>
                    {perfDiffHtml}
                    {notesHtml}
                    {errorHtml}
                </div>";
        }));

        return $@"<!DOCTYPE html>
<html>
<head>
    <meta charset='UTF-8'>
    <title>Performance Validation Report</title>
    <style>
        {DefaultCss}
        .perf-diff {{
            margin: 10px 0;
            padding: 8px;
            background: #f0f0f0;
            border-radius: 4px;
            font-weight: bold;
        }}
        .error-message {{
            margin: 10px 0;
            padding: 8px;
            background: #ffebee;
            border-left: 4px solid #f44336;
            color: #c62828;
        }}
        .notes {{
            margin: 10px 0;
            font-style: italic;
            color: #666;
        }}
        .query-result {{
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: white;
        }}
        .query-result.success {{ border-left: 4px solid #28a745; }}
        .query-result.warning {{ border-left: 4px solid #ffc107; }}
        .query-result.failed {{ border-left: 4px solid #dc3545; }}
        .summary {{ 
            margin: 20px 0; 
            padding: 20px; 
            border-radius: 4px; 
            border: 2px solid #ddd;
        }}
        .summary.success {{ 
            background: #d4edda; 
            border-color: #28a745;
        }}
        .summary.warning {{ 
            background: #fffbf0; 
            border-color: #ff9800;
        }}
        .summary.failed {{ 
            background: #f8d7da; 
            border-color: #dc3545;
        }}
        .summary table td,
        .summary table th {{
            color: #212529;
        }}
        .summary h2,
        .summary h3 {{
            color: #212529;
        }}
        .threshold-info {{
            margin: 20px 0;
            padding: 15px;
            background: #e3f2fd;
            border-left: 4px solid #2196f3;
            border-radius: 4px;
        }}
        .threshold-info strong {{ color: #1565c0; }}
    </style>
</head>
<body>
    <div class='container'>
        <h1>Oracle to PostgreSQL Performance Validation Report</h1>
        <div class='metadata'>
            <p><strong>Generated:</strong> {DateTime.Now:yyyy-MM-dd HH:mm:ss}</p>
            {(string.IsNullOrEmpty(summary.OracleDatabase) ? "" : $"<p><strong>Oracle Database:</strong> {summary.OracleDatabase}</p>")}
            {(string.IsNullOrEmpty(summary.PostgresDatabase) ? "" : $"<p><strong>PostgreSQL Database:</strong> {summary.PostgresDatabase}</p>")}
            {(string.IsNullOrEmpty(summary.OracleSchema) ? "" : $"<p><strong>Oracle Schema:</strong> {summary.OracleSchema}</p>")}
            {(string.IsNullOrEmpty(summary.PostgresSchema) ? "" : $"<p><strong>PostgreSQL Schema:</strong> {summary.PostgresSchema}</p>")}
            <p><strong>Test Duration:</strong> {summary.TotalTestDurationMs / 1000:F2} seconds</p>
        </div>

        <div class='threshold-info'>
            <strong>ℹ️ Performance Threshold:</strong> Queries with <strong>>{summary.ThresholdPercent}%</strong> execution time difference are flagged as warnings.
        </div>

        <div class='summary {statusClass}'>
            <h2>Summary</h2>
            <table>
                <tr><th>Metric</th><th>Count</th></tr>
                <tr><td>Total Queries</td><td>{summary.TotalQueries}</td></tr>
                <tr><td>✓ Passed</td><td>{summary.PassedQueries}</td></tr>
                <tr><td>⚠ Warning</td><td>{summary.WarningQueries}</td></tr>
                <tr><td>❌ Failed</td><td>{summary.FailedQueries}</td></tr>
                <tr><td>🔴 Row Count Mismatch</td><td>{summary.RowCountMismatchQueries}</td></tr>
            </table>
            <h3>Average Execution Times</h3>
            <table>
                <tr><td>Oracle</td><td>{summary.AverageOracleExecutionTimeMs:F2}ms</td></tr>
                <tr><td>PostgreSQL</td><td>{summary.AveragePostgresExecutionTimeMs:F2}ms</td></tr>
                {performanceComparisonRow}
            </table>
        </div>

        {GenerateTablePerformanceHtml(summary)}

        <h2>Query Results</h2>
        {resultsHtml}
    </div>
</body>
</html>";
    }

    private string GenerateTablePerformanceHtml(PerformanceTestSummary summary)
    {
        var tableQueries = summary.Results.Where(r => 
            r.QueryName.StartsWith("table_count_") ||
            r.QueryName.StartsWith("table_sample_") ||
            r.QueryName.StartsWith("table_pk_lookup_") ||
            r.QueryName.StartsWith("table_ordered_")).ToList();

        if (!tableQueries.Any())
        {
            return "";
        }

        var countTests = tableQueries.Where(q => q.QueryName.StartsWith("table_count_")).ToList();
        var sampleTests = tableQueries.Where(q => q.QueryName.StartsWith("table_sample_")).ToList();
        var pkTests = tableQueries.Where(q => q.QueryName.StartsWith("table_pk_lookup_")).ToList();
        var orderedTests = tableQueries.Where(q => q.QueryName.StartsWith("table_ordered_")).ToList();

        var breakdownHtml = "";
        if (countTests.Any())
            breakdownHtml += $@"<tr>
                <td>COUNT (Full Scan)</td>
                <td>{countTests.Count}</td>
                <td>{countTests.Count(q => q.Status == PerformanceStatus.Passed)}</td>
                <td>{countTests.Count(q => q.Status == PerformanceStatus.Warning)}</td>
                <td>{countTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}</td>
            </tr>";

        if (sampleTests.Any())
            breakdownHtml += $@"<tr>
                <td>SAMPLE (LIMIT)</td>
                <td>{sampleTests.Count}</td>
                <td>{sampleTests.Count(q => q.Status == PerformanceStatus.Passed)}</td>
                <td>{sampleTests.Count(q => q.Status == PerformanceStatus.Warning)}</td>
                <td>{sampleTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}</td>
            </tr>";

        if (pkTests.Any())
            breakdownHtml += $@"<tr>
                <td>PK LOOKUP (Index)</td>
                <td>{pkTests.Count}</td>
                <td>{pkTests.Count(q => q.Status == PerformanceStatus.Passed)}</td>
                <td>{pkTests.Count(q => q.Status == PerformanceStatus.Warning)}</td>
                <td>{pkTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}</td>
            </tr>";

        if (orderedTests.Any())
            breakdownHtml += $@"<tr>
                <td>ORDERED SCAN (Sort)</td>
                <td>{orderedTests.Count}</td>
                <td>{orderedTests.Count(q => q.Status == PerformanceStatus.Passed)}</td>
                <td>{orderedTests.Count(q => q.Status == PerformanceStatus.Warning)}</td>
                <td>{orderedTests.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}</td>
            </tr>";

        breakdownHtml += $@"<tr style='font-weight: bold; background-color: #f5f5f5;'>
            <td>TOTAL</td>
            <td>{tableQueries.Count}</td>
            <td>{tableQueries.Count(q => q.Status == PerformanceStatus.Passed)}</td>
            <td>{tableQueries.Count(q => q.Status == PerformanceStatus.Warning)}</td>
            <td>{tableQueries.Count(q => q.Status == PerformanceStatus.Failed || q.Status == PerformanceStatus.RowCountMismatch)}</td>
        </tr>";

        // Top 10 slowest tables
        var slowTablesHtml = "";
        var slowTables = tableQueries
            .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs > q.OracleExecutionTimeMs)
            .OrderByDescending(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
            .Take(10)
            .ToList();

        if (slowTables.Any())
        {
            slowTablesHtml = @"
            <h3>Top 10 Slowest Queries in PostgreSQL</h3>
            <table>
                <tr>
                    <th>Query</th>
                    <th>Oracle Time</th>
                    <th>PostgreSQL Time</th>
                    <th>Difference</th>
                    <th>Status</th>
                </tr>";

            foreach (var query in slowTables)
            {
                var diff = ((query.PostgresExecutionTimeMs - query.OracleExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                var statusIcon = query.Status switch
                {
                    PerformanceStatus.Passed => "✓",
                    PerformanceStatus.Warning => "⚠",
                    _ => "❌"
                };
                var rowClass = query.Status switch
                {
                    PerformanceStatus.Passed => "",
                    PerformanceStatus.Warning => "style='background-color: #fff3cd;'",
                    _ => "style='background-color: #f8d7da;'"
                };

                slowTablesHtml += $@"
                <tr {rowClass}>
                    <td>{query.QueryName.Replace("table_", "")}</td>
                    <td>{query.OracleExecutionTimeMs:F2}ms</td>
                    <td>{query.PostgresExecutionTimeMs:F2}ms</td>
                    <td>+{diff:F1}%</td>
                    <td>{statusIcon}</td>
                </tr>";
            }

            slowTablesHtml += "</table>";
        }

        // Top 10 fastest tables
        var fastTablesHtml = "";
        var fastTables = tableQueries
            .Where(q => q.OracleExecuted && q.PostgresExecuted && q.PostgresExecutionTimeMs < q.OracleExecutionTimeMs)
            .OrderBy(q => q.PostgresExecutionTimeMs - q.OracleExecutionTimeMs)
            .Take(10)
            .ToList();

        if (fastTables.Any())
        {
            fastTablesHtml = @"
            <h3>Top 10 Fastest Queries in PostgreSQL</h3>
            <table>
                <tr>
                    <th>Query</th>
                    <th>Oracle Time</th>
                    <th>PostgreSQL Time</th>
                    <th>Improvement</th>
                    <th>Status</th>
                </tr>";

            foreach (var query in fastTables)
            {
                var diff = ((query.OracleExecutionTimeMs - query.PostgresExecutionTimeMs) / query.OracleExecutionTimeMs) * 100;
                var statusIcon = query.Status switch
                {
                    PerformanceStatus.Passed => "✓",
                    PerformanceStatus.Warning => "⚠",
                    _ => "❌"
                };
                var rowClass = query.Status switch
                {
                    PerformanceStatus.Passed => "style='background-color: #d4edda;'",
                    PerformanceStatus.Warning => "style='background-color: #fff3cd;'",
                    _ => "style='background-color: #f8d7da;'"
                };

                fastTablesHtml += $@"
                <tr {rowClass}>
                    <td>{query.QueryName.Replace("table_", "")}</td>
                    <td>{query.OracleExecutionTimeMs:F2}ms</td>
                    <td>{query.PostgresExecutionTimeMs:F2}ms</td>
                    <td>-{diff:F1}%</td>
                    <td>{statusIcon}</td>
                </tr>";
            }

            fastTablesHtml += "</table>";
        }

        return $@"
        <div class='summary'>
            <h2>Table Performance Tests</h2>
            <table>
                <tr>
                    <th>Test Type</th>
                    <th>Total</th>
                    <th>Passed</th>
                    <th>Warning</th>
                    <th>Failed</th>
                </tr>
                {breakdownHtml}
            </table>
            {slowTablesHtml}
            {fastTablesHtml}
        </div>";
    }
}
