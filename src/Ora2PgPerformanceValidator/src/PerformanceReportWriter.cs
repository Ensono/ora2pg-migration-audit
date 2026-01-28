using Ora2PgPerformanceValidator.Models;
using Ora2Pg.Common.Writers;
using Serilog;

namespace Ora2PgPerformanceValidator.Writers;

public class PerformanceReportWriter : BaseHtmlReportWriter
{
    private readonly ILogger _logger = Log.ForContext<PerformanceReportWriter>();

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
        lines.Add($"| Average Oracle Time | {summary.AverageOracleExecutionTimeMs:F2}ms |");
        lines.Add($"| Average PostgreSQL Time | {summary.AveragePostgresExecutionTimeMs:F2}ms |");
        lines.Add("");

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
                lines.Add($"**Notes:** {result.Notes}");
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
        lines.Add("");

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
                lines.Add($"Notes: {result.Notes}");
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
                    <div class='notes'><strong>Notes:</strong> {result.Notes}</div>
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
        .summary {{ margin: 20px 0; padding: 20px; border-radius: 4px; }}
        .summary.success {{ background: #d4edda; }}
        .summary.warning {{ background: #fff3cd; }}
        .summary.failed {{ background: #f8d7da; }}
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
            </table>
        </div>

        <h2>Query Results</h2>
        {resultsHtml}
    </div>
</body>
</html>";
    }
}
