using System.Text;
using Ora2Pg.Common.Writers;
using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.src.Writers;

public class ValidationReportHtmlWriter : BaseHtmlReportWriter
{
    public void WriteHtmlReport(ValidationResult result, string oracleSchema, string postgresSchema, string outputPath)
    {
        try
        {
            var html = GenerateHtml(result, oracleSchema, postgresSchema);
            File.WriteAllText(outputPath, html);
            Log.Information($"📄 HTML report written to: {outputPath}");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to write HTML report");
            throw;
        }
    }

    private string GenerateHtml(ValidationResult result, string oracleSchema, string postgresSchema)
    {
        var sb = new StringBuilder();


        var additionalCss = @"
        .number { text-align: right; font-family: 'Courier New', monospace; }
        .pk-list { margin: 5px 0; padding: 10px; background-color: #fff3cd; border-left: 4px solid #ffc107; }
        .pk-list ul { margin: 5px 0; padding-left: 20px; }
        .pk-list li { margin: 3px 0; font-family: 'Courier New', monospace; font-size: 0.9em; }";
        
        sb.Append(GenerateHtmlHeader("Row Count Validation Report", additionalCss));

        var metadata = new Dictionary<string, string>();
        
        if (!string.IsNullOrEmpty(result.OracleDatabase))
        {
            metadata.Add("Oracle Database", result.OracleDatabase);
        }
        if (!string.IsNullOrEmpty(result.PostgresDatabase))
        {
            metadata.Add("PostgreSQL Database", result.PostgresDatabase);
        }
        
        metadata.Add("Oracle Schema", oracleSchema);
        metadata.Add("PostgreSQL Schema", postgresSchema);
        metadata.Add("Validation Time", result.ValidationTime.ToString("yyyy-MM-dd HH:mm:ss"));
        
        sb.Append(GenerateMetadataSection(metadata));

        sb.Append(GenerateStatusBadge(result.OverallStatus));

        var summaryMetrics = new List<SummaryMetric>
        {
            new("Total Tables Validated", result.TotalTablesValidated.ToString(), null, null),
            new("Matching Row Counts", result.TablesWithMatchingCounts.ToString(), "✅", "match"),
            new("Mismatched Row Counts", result.TablesWithMismatchedCounts.ToString(), 
                result.TablesWithMismatchedCounts == 0 ? "✅" : "❌", 
                result.TablesWithMismatchedCounts == 0 ? "match" : "mismatch"),
            new("Tables Only in Oracle", result.TablesOnlyInOracle.ToString(), 
                result.TablesOnlyInOracle == 0 ? "✅" : "⚠️", 
                result.TablesOnlyInOracle == 0 ? "match" : "warning"),
            new("Tables Only in PostgreSQL", result.TablesOnlyInPostgres.ToString(), 
                result.TablesOnlyInPostgres == 0 ? "✅" : "⚠️", 
                result.TablesOnlyInPostgres == 0 ? "match" : "warning"),
            new("Critical Issues (>10% difference)", result.CriticalIssues.ToString(), 
                result.CriticalIssues == 0 ? "✅" : "🔴", 
                result.CriticalIssues == 0 ? "match" : "mismatch"),
            new("Errors (1-10% difference)", result.Errors.ToString(), 
                result.Errors == 0 ? "✅" : "🔴", 
                result.Errors == 0 ? "match" : "mismatch"),
            new("Warnings (<1% difference)", result.Warnings.ToString(), 
                result.Warnings == 0 ? "✅" : "🟡", 
                result.Warnings == 0 ? "match" : "warning")
        };
        sb.Append(GenerateSummaryTable(summaryMetrics));

        if (result.Issues.Any())
        {
            sb.AppendLine("        <h2>Validation Details</h2>");
            sb.AppendLine("        <table>");
            sb.AppendLine("            <tr>");
            sb.AppendLine("                <th>Table Name</th>");
            sb.AppendLine("                <th>Severity</th>");
            sb.AppendLine("                <th>Status</th>");
            sb.AppendLine("                <th style=\"text-align: right;\">Oracle Rows</th>");
            sb.AppendLine("                <th style=\"text-align: right;\">PostgreSQL Rows</th>");
            sb.AppendLine("                <th style=\"text-align: right;\">Difference</th>");
            sb.AppendLine("                <th>Details</th>");
            sb.AppendLine("            </tr>");
            
            var issuesToShow = result.Issues.ToList();
            
            foreach (var issue in issuesToShow)
            {
                var severityIcon = issue.Severity switch
                {
                    ValidationSeverity.Critical => "\U0001F534",  // 🔴
                    ValidationSeverity.Error => "\u274C",  // ❌
                    ValidationSeverity.Warning => "\u26A0\uFE0F",  // ⚠️
                    _ => "\u2139\uFE0F"  // ℹ️
                };
                
                var rowClass = issue.Severity switch
                {
                    ValidationSeverity.Critical => "mismatch",
                    ValidationSeverity.Error => "mismatch",
                    ValidationSeverity.Warning => "warning",
                    _ => "match"
                };
                
                var diff = issue.PostgresRowCount - issue.OracleRowCount;
                var diffDisplay = diff == 0 ? "\u2705 0" : $"\u274C {diff:+#;-#;0}";  // ✅ : ❌
                
                sb.AppendLine($"            <tr class=\"{rowClass}\">");
                sb.AppendLine($"                <td><strong>{EscapeHtml(issue.TableName)}</strong></td>");
                sb.AppendLine($"                <td>{severityIcon} {issue.Severity}</td>");
                sb.AppendLine($"                <td>{EscapeHtml(issue.IssueType)}</td>");
                sb.AppendLine($"                <td class=\"number\">{issue.OracleRowCount:N0}</td>");
                sb.AppendLine($"                <td class=\"number\">{issue.PostgresRowCount:N0}</td>");
                sb.AppendLine($"                <td class=\"number\">{diffDisplay}</td>");
                sb.AppendLine("                <td>");

                if (issue.MissingInPostgres.Any())
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine("                        <strong>Missing in PostgreSQL:</strong>");
                    sb.AppendLine("                        <ul>");
                    var missingToShow = issue.MissingInPostgres.Take(10).ToList();
                    foreach (var row in missingToShow)
                    {
                        sb.AppendLine($"                            <li>{EscapeHtml(row.PrimaryKeyDisplay)}</li>");
                    }
                    if (issue.MissingInPostgres.Count > 10)
                    {
                        sb.AppendLine($"                            <li><em>... and {issue.MissingInPostgres.Count - 10} more</em></li>");
                    }
                    sb.AppendLine("                        </ul>");
                    sb.AppendLine("                    </div>");
                }

                if (issue.ExtraInPostgres.Any())
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine("                        <strong>Extra in PostgreSQL:</strong>");
                    sb.AppendLine("                        <ul>");
                    var extraToShow = issue.ExtraInPostgres.Take(10).ToList();
                    foreach (var row in extraToShow)
                    {
                        sb.AppendLine($"                            <li>{EscapeHtml(row.PrimaryKeyDisplay)}</li>");
                    }
                    if (issue.ExtraInPostgres.Count > 10)
                    {
                        sb.AppendLine($"                            <li><em>... and {issue.ExtraInPostgres.Count - 10} more</em></li>");
                    }
                    sb.AppendLine("                        </ul>");
                    sb.AppendLine("                    </div>");
                }
                
                if (!issue.MissingInPostgres.Any() && !issue.ExtraInPostgres.Any())
                {
                    if (!issue.PartitionRowCounts.Any())
                    {
                        sb.AppendLine("                    -");
                    }
                }

                if (issue.PartitionRowCounts.Any())
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine("                        <strong>Partition Breakdown:</strong>");
                    sb.AppendLine("                        <ul>");
                    foreach (var partition in issue.PartitionRowCounts.OrderBy(p => p.PartitionName))
                    {
                        sb.AppendLine($"                            <li>{EscapeHtml(partition.PartitionName)}: {partition.RowCount:N0}</li>");
                    }
                    sb.AppendLine("                        </ul>");
                    sb.AppendLine("                    </div>");
                }
                
                sb.AppendLine("                </td>");
                sb.AppendLine("            </tr>");
            }
            
            sb.AppendLine("        </table>");
        }
        
        sb.Append(GenerateHtmlFooter());
        
        return sb.ToString();
    }
}