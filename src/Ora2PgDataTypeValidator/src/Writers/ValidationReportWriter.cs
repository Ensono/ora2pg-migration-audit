using Ora2PgDataTypeValidator.Models;
using Serilog;
using System.Text;

namespace Ora2PgDataTypeValidator.src.Writers;

public class ValidationReportWriter
{
    public async Task WriteReportsAsync(ValidationResult result, string baseOutputPath)
    {

        var mdPath = baseOutputPath.Replace(".txt", ".md");
        var txtPath = baseOutputPath.Replace(".md", ".txt");

        await WriteMarkdownReportAsync(result, mdPath);
        await WriteTextReportAsync(result, txtPath);
    }

    public async Task WriteMarkdownReportAsync(ValidationResult result, string outputPath)
    {
        var sb = new StringBuilder();

        sb.AppendLine("# Oracle to PostgreSQL Data Type Validation Report");
        sb.AppendLine();
        sb.AppendLine("🆕 **DMS-Pattern Validation** - Validates against actual GCP Database Migration Service conversion patterns");
        sb.AppendLine();
        sb.AppendLine($"**Generated:** {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"**Oracle Schema:** {result.OracleSchema}");
        sb.AppendLine($"**PostgreSQL Schema:** {result.PostgresSchema}");
        sb.AppendLine();

        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine($"| Metric | Count |");
        sb.AppendLine("|--------|-------|");
        sb.AppendLine($"| **Overall Status** | **{GetStatusEmoji(result.OverallStatus)} {result.OverallStatus}** |");
        sb.AppendLine($"| Total Columns Validated | {result.TotalColumnsValidated} |");
        sb.AppendLine($"| Critical Issues | {result.CriticalIssues} ❌ |");
        sb.AppendLine($"| Errors | {result.Errors} 🔴 |");
        sb.AppendLine($"| Warnings | {result.Warnings} ⚠️ |");
        sb.AppendLine($"| Info Messages | {result.InfoMessages} ℹ️ |");
        sb.AppendLine();

        WriteCategorySummary(sb, result);

        if (result.Issues.Any())
        {
            WriteSeveritySection(sb, result, ValidationSeverity.Critical, "❌ Critical Issues");
            WriteSeveritySection(sb, result, ValidationSeverity.Error, "🔴 Errors");
            WriteSeveritySection(sb, result, ValidationSeverity.Warning, "⚠️ Warnings");
            WriteSeveritySection(sb, result, ValidationSeverity.Info, "ℹ️ Information");
        }

        await File.WriteAllTextAsync(outputPath, sb.ToString());
        Log.Information($"📄 Markdown report written to: {outputPath}");
    }

    public async Task WriteTextReportAsync(ValidationResult result, string outputPath)
    {
        var sb = new StringBuilder();

        sb.AppendLine("===============================================================================");
        sb.AppendLine("  ORACLE TO POSTGRESQL DATA TYPE VALIDATION REPORT");
        sb.AppendLine("  DMS-Pattern Based Validation");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();
        sb.AppendLine($"Generated:          {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"Oracle Schema:      {result.OracleSchema}");
        sb.AppendLine($"PostgreSQL Schema:  {result.PostgresSchema}");
        sb.AppendLine();

        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine("  SUMMARY");
        sb.AppendLine("-------------------------------------------------------------------------------");
        sb.AppendLine($"Overall Status:             {result.OverallStatus}");
        sb.AppendLine($"Total Columns Validated:    {result.TotalColumnsValidated}");
        sb.AppendLine($"Critical Issues:            {result.CriticalIssues}");
        sb.AppendLine($"Errors:                     {result.Errors}");
        sb.AppendLine($"Warnings:                   {result.Warnings}");
        sb.AppendLine($"Info Messages:              {result.InfoMessages}");
        sb.AppendLine();

        WriteCategorySummaryText(sb, result);

        if (result.Issues.Any())
        {
            WriteSeveritySectionText(sb, result, ValidationSeverity.Critical, "CRITICAL ISSUES");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Error, "ERRORS");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Warning, "WARNINGS");
            WriteSeveritySectionText(sb, result, ValidationSeverity.Info, "INFORMATION");
        }

        await File.WriteAllTextAsync(outputPath, sb.ToString());
        Log.Information($"📄 Text report written to: {outputPath}");
    }

    private void WriteSeveritySection(StringBuilder sb, ValidationResult result, ValidationSeverity severity, string title)
    {
        var issues = result.Issues.Where(i => i.Severity == severity).ToList();
        if (!issues.Any()) return;

        sb.AppendLine($"## {title} ({issues.Count})");
        sb.AppendLine();

        foreach (var issue in issues.OrderBy(i => i.TableName).ThenBy(i => i.ColumnName))
        {
            sb.AppendLine($"### {GetCategoryIcon(issue.Category)} {issue.TableName}.{issue.ColumnName}");
            sb.AppendLine();
            sb.AppendLine($"**Category:** {issue.Category}");
            sb.AppendLine($"**Mapping:** Oracle `{issue.OracleType}` → PostgreSQL `{issue.PostgresType}`");
            sb.AppendLine();
            sb.AppendLine($"**Issue:** {issue.Message}");
            sb.AppendLine();
            
            if (!string.IsNullOrEmpty(issue.Recommendation))
            {
                sb.AppendLine($"**Recommendation:** {issue.Recommendation}");
                sb.AppendLine();
            }

            if (issue.Metadata.Any())
            {
                sb.AppendLine("<details>");
                sb.AppendLine("<summary>Technical Details</summary>");
                sb.AppendLine();
                foreach (var kvp in issue.Metadata)
                {
                    sb.AppendLine($"- **{kvp.Key}:** {kvp.Value}");
                }
                sb.AppendLine("</details>");
                sb.AppendLine();
            }

            sb.AppendLine("---");
            sb.AppendLine();
        }
    }

    private void WriteCategorySummary(StringBuilder sb, ValidationResult result)
    {
        sb.AppendLine("## Issues by Category");
        sb.AppendLine();

        var categoryGroups = result.Issues
            .GroupBy(i => i.Category)
            .OrderByDescending(g => g.Count())
            .ToList();

        sb.AppendLine("| Category | Count | Critical | Errors | Warnings | Info |");
        sb.AppendLine("|----------|-------|----------|--------|----------|------|");

        foreach (var group in categoryGroups)
        {
            var critical = group.Count(i => i.Severity == ValidationSeverity.Critical);
            var errors = group.Count(i => i.Severity == ValidationSeverity.Error);
            var warnings = group.Count(i => i.Severity == ValidationSeverity.Warning);
            var info = group.Count(i => i.Severity == ValidationSeverity.Info);

            sb.AppendLine($"| {group.Key} | {group.Count()} | {critical} | {errors} | {warnings} | {info} |");
        }

        sb.AppendLine();
    }

    private void WriteSeveritySectionText(StringBuilder sb, ValidationResult result, ValidationSeverity severity, string title)
    {
        var issues = result.Issues.Where(i => i.Severity == severity).ToList();
        if (!issues.Any()) return;

        sb.AppendLine("===============================================================================");
        sb.AppendLine($"  {title} ({issues.Count})");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();

        foreach (var issue in issues.OrderBy(i => i.TableName).ThenBy(i => i.ColumnName))
        {
            sb.AppendLine($"Column:          {issue.TableName}.{issue.ColumnName}");
            sb.AppendLine($"Category:        {issue.Category}");
            sb.AppendLine($"Oracle Type:     {issue.OracleType}");
            sb.AppendLine($"PostgreSQL Type: {issue.PostgresType}");
            sb.AppendLine();
            sb.AppendLine($"Issue:");
            sb.AppendLine($"  {issue.Message}");
            sb.AppendLine();
            
            if (!string.IsNullOrEmpty(issue.Recommendation))
            {
                sb.AppendLine($"Recommendation:");
                sb.AppendLine($"  {issue.Recommendation}");
                sb.AppendLine();
            }

            if (issue.Metadata.Any())
            {
                sb.AppendLine("Technical Details:");
                foreach (var kvp in issue.Metadata)
                {
                    sb.AppendLine($"  - {kvp.Key}: {kvp.Value}");
                }
                sb.AppendLine();
            }

            sb.AppendLine("-------------------------------------------------------------------------------");
            sb.AppendLine();
        }
    }

    private void WriteCategorySummaryText(StringBuilder sb, ValidationResult result)
    {
        sb.AppendLine("===============================================================================");
        sb.AppendLine("  ISSUES BY CATEGORY");
        sb.AppendLine("===============================================================================");
        sb.AppendLine();

        var categoryGroups = result.Issues
            .GroupBy(i => i.Category)
            .OrderByDescending(g => g.Count())
            .ToList();

        var maxCategoryLength = categoryGroups.Any() ? categoryGroups.Max(g => g.Key.Length) : 20;
        var categoryWidth = Math.Max(maxCategoryLength, 20);

        sb.AppendLine($"{"Category".PadRight(categoryWidth)} | Count | Critical | Errors | Warnings | Info");
        sb.AppendLine($"{new string('-', categoryWidth)}-+-------+----------+--------+----------+------");

        foreach (var group in categoryGroups)
        {
            var critical = group.Count(i => i.Severity == ValidationSeverity.Critical);
            var errors = group.Count(i => i.Severity == ValidationSeverity.Error);
            var warnings = group.Count(i => i.Severity == ValidationSeverity.Warning);
            var info = group.Count(i => i.Severity == ValidationSeverity.Info);

            sb.AppendLine($"{group.Key.PadRight(categoryWidth)} | {group.Count(),5} | {critical,8} | {errors,6} | {warnings,8} | {info,4}");
        }

        sb.AppendLine();
    }

    public void WriteConsoleReport(ValidationResult result)
    {
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information("  DATA TYPE VALIDATION REPORT");
        Log.Information("═══════════════════════════════════════════════════════════");
        Log.Information($"Oracle Schema: {result.OracleSchema}");
        Log.Information($"PostgreSQL Schema: {result.PostgresSchema}");
        Log.Information($"Validation Time: {result.ValidationTime:yyyy-MM-dd HH:mm:ss}");
        Log.Information("───────────────────────────────────────────────────────────");
        Log.Information($"Status: {GetStatusEmoji(result.OverallStatus)} {result.OverallStatus}");
        Log.Information($"Columns Validated: {result.TotalColumnsValidated}");
        Log.Information($"Critical Issues: {result.CriticalIssues} ❌");
        Log.Information($"Errors: {result.Errors} 🔴");
        Log.Information($"Warnings: {result.Warnings} ⚠️");
        Log.Information($"Info: {result.InfoMessages} ℹ️");
        Log.Information("═══════════════════════════════════════════════════════════");

        if (result.HasCriticalIssues || result.HasErrors)
        {
            Log.Error("❌ VALIDATION FAILED - Review issues above");
        }
        else if (result.Warnings > 0)
        {
            Log.Warning("⚠️  VALIDATION PASSED WITH WARNINGS");
        }
        else
        {
            Log.Information("✅ VALIDATION PASSED - All type mappings look good!");
        }
    }

    private string GetStatusEmoji(string status) => status switch
    {
        "PASSED" => "✅",
        "WARNING" => "⚠️",
        "FAILED" => "❌",
        _ => "❓"
    };

    private string GetCategoryIcon(string category) => category switch
    {
        // DMS Validation
        "Type Mapping Mismatch" => "🔀",
        "Valid Mapping" => "✅",
        
        // Numeric Issues
        "Numeric Overflow Risk" => "⚠️",
        "Precision/Scale Mismatch" => "🔢",
        "Invalid Mapping" => "❌",
        "Storage Optimization" => "💾",
        
        // String Issues
        "String Type Mismatch" => "📝",
        "Text Truncation Risk" => "✂️",
        "Character Encoding" => "🔤",
        "Padding Behavior" => "↔️",
        
        // Date/Time Issues
        "Time Data Loss" => "⏰",
        "Date Mapping OK" => "📅",
        "Timestamp Type Mismatch" => "🕐",
        "Timezone Type Mismatch" => "🌍",
        "UTC Conversion" => "🌐",
        
        // Advanced Types
        "XML Type Mismatch" => "📄",
        "XML Features Lost" => "📋",
        "JSON Type Mismatch" => "🔧",
        "JSONB Recommended" => "⚡",
        "Binary Type Mismatch" => "📦",
        "Spatial Type Missing" => "🗺️",
        "Spatial Type OK" => "📍",
        
        // Legacy/Deprecated
        "Legacy LONG Type" => "⚠️",
        "Deprecated Type" => "🚫",
        "Deprecated Binary Type" => "🚫",
        
        // Critical Issues
        "Empty String Handling" => "⚠️",
        "External File Pointer" => "🔴",
        "User-Defined Type" => "🔴",
        
        // Auto-increment
        "Auto-Increment Missing" => "🔢",
        "Auto-Increment OK" => "🔢",
        
        // Float/Precision
        "Float Type Mismatch" => "🔢",
        "Rounding Errors" => "≈",
        "Float Precision" => "🎯",
        
        // Misc
        "Boolean Conversion" => "✓",
        "Unicode Support" => "🌐",
        "Binary File Validation" => "📁",
        
        _ => "•"
    };
}
