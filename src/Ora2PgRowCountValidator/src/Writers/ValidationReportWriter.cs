using Ora2PgRowCountValidator.Models;
using Serilog;
using System.Text;

namespace Ora2PgRowCountValidator.src.Writers;


public class ValidationReportWriter
{
    public async Task WriteReportsAsync(ValidationResult result, string baseOutputPath)
    {

        var mdPath = baseOutputPath.EndsWith(".md") ? baseOutputPath : $"{baseOutputPath}.md";
        var txtPath = baseOutputPath.EndsWith(".txt") ? baseOutputPath : $"{baseOutputPath}.txt";

        await WriteMarkdownReportAsync(result, mdPath);
        await WriteTextReportAsync(result, txtPath);
    }

    public async Task WriteMarkdownReportAsync(ValidationResult result, string outputPath)
    {
        var sb = new StringBuilder();
        

        sb.AppendLine("# Oracle to PostgreSQL Row Count Validation Report");
        sb.AppendLine();
        sb.AppendLine($"**Generated:** {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        
        if (!string.IsNullOrEmpty(result.OracleDatabase))
        {
            sb.AppendLine($"**Oracle Database:** {result.OracleDatabase}");
        }
        if (!string.IsNullOrEmpty(result.PostgresDatabase))
        {
            sb.AppendLine($"**PostgreSQL Database:** {result.PostgresDatabase}");
        }
        
        sb.AppendLine($"**Oracle Schema:** {result.OracleSchema}");
        sb.AppendLine($"**PostgreSQL Schema:** {result.PostgresSchema}");
        sb.AppendLine();

        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine($"| Metric | Count |");
        sb.AppendLine("|--------|-------|");
        sb.AppendLine($"| **Overall Status** | **{GetStatusEmoji(result.OverallStatus)} {result.OverallStatus}** |");
        sb.AppendLine($"| Total Tables Validated | {result.TotalTablesValidated} |");
        sb.AppendLine($"| \u2705 Tables with Matching Counts | {result.TablesWithMatchingCounts} |");  // ✅
        sb.AppendLine($"| \u274C Tables with Mismatched Counts | {result.TablesWithMismatchedCounts} |");  // ❌
        sb.AppendLine($"| \u26A0\uFE0F Tables Only in Oracle | {result.TablesOnlyInOracle} |");  // ⚠️
        sb.AppendLine($"| \u2139\uFE0F Tables Only in PostgreSQL | {result.TablesOnlyInPostgres} |");  // ℹ️
        sb.AppendLine($"| **Total Oracle Rows** | **{result.TotalOracleRows:N0}** |");
        sb.AppendLine($"| **Total PostgreSQL Rows** | **{result.TotalPostgresRows:N0}** |");
        sb.AppendLine($"| **Row Difference** | **{Math.Abs(result.TotalPostgresRows - result.TotalOracleRows):N0}** |");
        sb.AppendLine();

        // Add severity summary with threshold explanations
        sb.AppendLine("### Severity Breakdown");
        sb.AppendLine();
        sb.AppendLine("| Severity | Count | Description |");
        sb.AppendLine("|----------|-------|-------------|");
        sb.AppendLine($"| \u274C Critical Issues | {result.CriticalIssues} | >10% row count difference |");
        sb.AppendLine($"| \U0001F534 Errors | {result.Errors} | 1-10% row count difference |");
        sb.AppendLine($"| \u26A0\uFE0F Warnings | {result.Warnings} | <1% row count difference |");
        sb.AppendLine($"| \u2139\uFE0F Info | {result.InfoMessages} | Matching counts |");
        sb.AppendLine();

        if (result.Issues.Any())
        {
            WriteSeveritySection(sb, result, ValidationSeverity.Critical, "\u274C Critical Issues (>10% row count difference)");  // ❌
            WriteSeveritySection(sb, result, ValidationSeverity.Error, "\U0001F534 Errors (1-10% row count difference)");  // 🔴
            WriteSeveritySection(sb, result, ValidationSeverity.Warning, "\u26A0\uFE0F Warnings (<1% row count difference)");  // ⚠️
            WriteSeveritySection(sb, result, ValidationSeverity.Info, "\u2139\uFE0F Tables with Matching Counts");  // ℹ️
        }

        WriteIssueTypeSummary(sb, result);

        await File.WriteAllTextAsync(outputPath, sb.ToString());
        Log.Information($"📄 Markdown report written to: {outputPath}");
    }

    public async Task WriteTextReportAsync(ValidationResult result, string outputPath)
    {
        var sb = new StringBuilder();

        sb.AppendLine("===============================================================================");
        sb.AppendLine("  ORACLE TO POSTGRESQL ROW COUNT VALIDATION REPORT");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();
        sb.AppendLine($"Generated:          {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        
        if (!string.IsNullOrEmpty(result.OracleDatabase))
        {
            sb.AppendLine($"Oracle Database:    {result.OracleDatabase}");
        }
        if (!string.IsNullOrEmpty(result.PostgresDatabase))
        {
            sb.AppendLine($"PostgreSQL Database:{result.PostgresDatabase}");
        }
        
        sb.AppendLine($"Oracle Schema:      {result.OracleSchema}");
        sb.AppendLine($"PostgreSQL Schema:  {result.PostgresSchema}");
        sb.AppendLine();

        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine("  SUMMARY");
        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine($"Overall Status:                 {result.OverallStatus}");
        sb.AppendLine($"Total Tables Validated:         {result.TotalTablesValidated}");
        sb.AppendLine($"Tables with Matching Counts:    {result.TablesWithMatchingCounts}");
        sb.AppendLine($"Tables with Mismatched Counts:  {result.TablesWithMismatchedCounts}");
        sb.AppendLine($"Tables Only in Oracle:          {result.TablesOnlyInOracle}");
        sb.AppendLine($"Tables Only in PostgreSQL:      {result.TablesOnlyInPostgres}");
        sb.AppendLine();
        sb.AppendLine($"Total Oracle Rows:              {result.TotalOracleRows:N0}");
        sb.AppendLine($"Total PostgreSQL Rows:          {result.TotalPostgresRows:N0}");
        sb.AppendLine($"Row Difference:                 {Math.Abs(result.TotalPostgresRows - result.TotalOracleRows):N0}");
        sb.AppendLine();

        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine("  SEVERITY BREAKDOWN");
        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine($"Critical Issues:    {result.CriticalIssues,5}  (>10% row count difference)");
        sb.AppendLine($"Errors:             {result.Errors,5}  (1-10% row count difference)");
        sb.AppendLine($"Warnings:           {result.Warnings,5}  (<1% row count difference)");
        sb.AppendLine($"Info:               {result.InfoMessages,5}  (matching counts)");
        sb.AppendLine();

        if (result.Issues.Any())
        {
            WriteSeveritySectionText(sb, result, ValidationSeverity.Critical, "CRITICAL ISSUES (>10% row count difference)");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Error, "ERRORS (1-10% row count difference)");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Warning, "WARNINGS (<1% row count difference)");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Info, "TABLES WITH MATCHING COUNTS");
        }

        WriteIssueTypeSummaryText(sb, result);

        await File.WriteAllTextAsync(outputPath, sb.ToString());
        Log.Information($"📄 Text report written to: {outputPath}");
    }

    private void WriteSeveritySection(StringBuilder sb, ValidationResult result, ValidationSeverity severity, string title)
    {
        var issues = result.Issues.Where(i => i.Severity == severity).ToList();
        if (!issues.Any()) return;

        sb.AppendLine($"## {title} ({issues.Count})");
        sb.AppendLine();

        foreach (var issue in issues.OrderBy(i => i.TableName))
        {
            sb.AppendLine($"### {issue.TableName}");
            sb.AppendLine();
            sb.AppendLine($"**Issue Type:** {issue.IssueType}");
            sb.AppendLine($"**Oracle Rows:** {issue.OracleRowCount?.ToString("N0") ?? "N/A"}");
            sb.AppendLine($"**PostgreSQL Rows:** {issue.PostgresRowCount?.ToString("N0") ?? "N/A"}");
            
            if (issue.Difference.HasValue)
            {
                sb.AppendLine($"**Difference:** {issue.Difference.Value:N0} rows ({issue.PercentageDifference:F2}%)");
            }
            
            sb.AppendLine();
            sb.AppendLine($"**Message:** {issue.Message}");
            sb.AppendLine();
            
            if (!string.IsNullOrEmpty(issue.Recommendation))
            {
                sb.AppendLine($"**Recommendation:** {issue.Recommendation}");
                sb.AppendLine();
            }

            if (issue.PartitionRowCounts.Any())
            {
                sb.AppendLine("**Partition Breakdown:**");
                sb.AppendLine();
                sb.AppendLine("| Partition | Rows |");
                sb.AppendLine("|-----------|------|");
                foreach (var partition in issue.PartitionRowCounts.OrderBy(p => p.PartitionName))
                {
                    sb.AppendLine($"| {partition.PartitionName} | {partition.RowCount:N0} |");
                }
                sb.AppendLine();
            }

            if (issue.HasDetailedComparison)
            {
                if (issue.MissingInPostgres.Any())
                {
                    sb.AppendLine($"**\u274C Missing in PostgreSQL ({issue.MissingInPostgres.Count} sample rows):**");  // ❌
                    sb.AppendLine();
                    sb.AppendLine("| Primary Key Values |");
                    sb.AppendLine("|--------------------|");
                    foreach (var row in issue.MissingInPostgres)
                    {
                        sb.AppendLine($"| `{row.PrimaryKeyDisplay}` |");
                    }
                    sb.AppendLine();
                }

                if (issue.ExtraInPostgres.Any())
                {
                    sb.AppendLine($"**\u2795 Extra in PostgreSQL ({issue.ExtraInPostgres.Count} sample rows):**");  // ➕
                    sb.AppendLine();
                    sb.AppendLine("| Primary Key Values |");
                    sb.AppendLine("|--------------------|");
                    foreach (var row in issue.ExtraInPostgres)
                    {
                        sb.AppendLine($"| `{row.PrimaryKeyDisplay}` |");
                    }
                    sb.AppendLine();
                }
            }
            else if (!string.IsNullOrEmpty(issue.DetailedComparisonSkippedReason))
            {
                sb.AppendLine($"**\u2139\uFE0F Detailed Row Comparison:** {issue.DetailedComparisonSkippedReason}");  // ℹ️
                sb.AppendLine();
            }

            sb.AppendLine("---");
            sb.AppendLine();
        }
    }

    private void WriteSeveritySectionText(StringBuilder sb, ValidationResult result, ValidationSeverity severity, string title)
    {
        var issues = result.Issues.Where(i => i.Severity == severity).ToList();
        if (!issues.Any()) return;

        sb.AppendLine("===============================================================================");
        sb.AppendLine($"  {title} ({issues.Count})");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();

        foreach (var issue in issues.OrderBy(i => i.TableName))
        {
            sb.AppendLine($"Table:           {issue.TableName}");
            sb.AppendLine($"Issue Type:      {issue.IssueType}");
            sb.AppendLine($"Oracle Rows:     {issue.OracleRowCount?.ToString("N0") ?? "N/A"}");
            sb.AppendLine($"PostgreSQL Rows: {issue.PostgresRowCount?.ToString("N0") ?? "N/A"}");
            
            if (issue.Difference.HasValue)
            {
                sb.AppendLine($"Difference:      {issue.Difference.Value:N0} rows ({issue.PercentageDifference:F2}%)");
            }
            
            sb.AppendLine();
            sb.AppendLine($"Message:");
            sb.AppendLine($"  {issue.Message}");
            sb.AppendLine();
            
            if (!string.IsNullOrEmpty(issue.Recommendation))
            {
                sb.AppendLine($"Recommendation:");
                sb.AppendLine($"  {issue.Recommendation}");
                sb.AppendLine();
            }

            if (issue.PartitionRowCounts.Any())
            {
                sb.AppendLine("Partition Breakdown:");
                foreach (var partition in issue.PartitionRowCounts.OrderBy(p => p.PartitionName))
                {
                    sb.AppendLine($"  - {partition.PartitionName}: {partition.RowCount:N0}");
                }
                sb.AppendLine();
            }

            if (issue.HasDetailedComparison)
            {
                if (issue.MissingInPostgres.Any())
                {
                    sb.AppendLine($"Missing in PostgreSQL ({issue.MissingInPostgres.Count} sample rows):");
                    foreach (var row in issue.MissingInPostgres)
                    {
                        sb.AppendLine($"  - {row.PrimaryKeyDisplay}");
                    }
                    sb.AppendLine();
                }

                if (issue.ExtraInPostgres.Any())
                {
                    sb.AppendLine($"Extra in PostgreSQL ({issue.ExtraInPostgres.Count} sample rows):");
                    foreach (var row in issue.ExtraInPostgres)
                    {
                        sb.AppendLine($"  - {row.PrimaryKeyDisplay}");
                    }
                    sb.AppendLine();
                }
            }
            else if (!string.IsNullOrEmpty(issue.DetailedComparisonSkippedReason))
            {
                sb.AppendLine($"Detailed Row Comparison:");
                sb.AppendLine($"  {issue.DetailedComparisonSkippedReason}");
                sb.AppendLine();
            }

            sb.AppendLine("-------------------------------------------------------------------------------");
            sb.AppendLine();
        }
    }

    private void WriteIssueTypeSummary(StringBuilder sb, ValidationResult result)
    {
        sb.AppendLine("## Issues by Type");
        sb.AppendLine();

        var typeGroups = result.Issues
            .GroupBy(i => i.IssueType)
            .OrderByDescending(g => g.Count())
            .ToList();

        sb.AppendLine("| Issue Type | Count | Critical | Errors | Warnings | Info |");
        sb.AppendLine("|------------|-------|----------|--------|----------|------|");

        foreach (var group in typeGroups)
        {
            var critical = group.Count(i => i.Severity == ValidationSeverity.Critical);
            var errors = group.Count(i => i.Severity == ValidationSeverity.Error);
            var warnings = group.Count(i => i.Severity == ValidationSeverity.Warning);
            var info = group.Count(i => i.Severity == ValidationSeverity.Info);

            sb.AppendLine($"| {group.Key} | {group.Count()} | {critical} | {errors} | {warnings} | {info} |");
        }

        sb.AppendLine();
    }

    private void WriteIssueTypeSummaryText(StringBuilder sb, ValidationResult result)
    {
        sb.AppendLine("===============================================================================");
        sb.AppendLine("  ISSUES BY TYPE");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();

        var typeGroups = result.Issues
            .GroupBy(i => i.IssueType)
            .OrderByDescending(g => g.Count())
            .ToList();

        var maxTypeLength = typeGroups.Any() ? typeGroups.Max(g => g.Key.Length) : 20;
        var typeWidth = Math.Max(maxTypeLength, 20);

        sb.AppendLine($"{"Issue Type".PadRight(typeWidth)} | Count | Critical | Errors | Warnings | Info");
        sb.AppendLine($"{new string('-', typeWidth)}-+-------+----------+--------+----------+------");

        foreach (var group in typeGroups)
        {
            var critical = group.Count(i => i.Severity == ValidationSeverity.Critical);
            var errors = group.Count(i => i.Severity == ValidationSeverity.Error);
            var warnings = group.Count(i => i.Severity == ValidationSeverity.Warning);
            var info = group.Count(i => i.Severity == ValidationSeverity.Info);

            sb.AppendLine($"{group.Key.PadRight(typeWidth)} | {group.Count(),5} | {critical,8} | {errors,6} | {warnings,8} | {info,4}");
        }

        sb.AppendLine();
    }

    public void WriteConsoleReport(ValidationResult result)
    {
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  ROW COUNT VALIDATION REPORT");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information($"Oracle Schema: {result.OracleSchema}");
        Log.Information($"PostgreSQL Schema: {result.PostgresSchema}");
        Log.Information($"Validation Time: {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        Log.Information("───────────────────────────────────────────────────────────");
        Log.Information($"Status: {GetStatusEmoji(result.OverallStatus)} {result.OverallStatus}");
        Log.Information($"Total Tables: {result.TotalTablesValidated}");
        Log.Information($"✅ Matching: {result.TablesWithMatchingCounts}");
        Log.Information($"❌ Mismatched: {result.TablesWithMismatchedCounts}");
        Log.Information($"⚠️  Only in Oracle: {result.TablesOnlyInOracle}");
        Log.Information($"ℹ️  Only in PostgreSQL: {result.TablesOnlyInPostgres}");
        Log.Information("───────────────────────────────────────────────────────────");
        Log.Information($"Oracle Total Rows: {result.TotalOracleRows:N0}");
        Log.Information($"PostgreSQL Total Rows: {result.TotalPostgresRows:N0}");
        Log.Information($"Difference: {Math.Abs(result.TotalPostgresRows - result.TotalOracleRows):N0}");
        Log.Information("═══════════════════════════════════════════════════════════");

        if (result.HasCriticalIssues || result.HasErrors)
        {
            Log.Error("❌ VALIDATION FAILED - Review row count mismatches");
        }
        else if (result.Warnings > 0)
        {
            Log.Warning("⚠️  VALIDATION PASSED WITH WARNINGS");
        }
        else
        {
            Log.Information("✅ VALIDATION PASSED - All row counts match!");
        }
    }

    private string GetStatusEmoji(string status) => status switch
    {
        "PASSED" => "\u2705",  // ✅
        "WARNING" => "\u26A0\uFE0F",  // ⚠️
        "FAILED" => "\u274C",  // ❌
        _ => "\u2753"  // ❓
    };
}
