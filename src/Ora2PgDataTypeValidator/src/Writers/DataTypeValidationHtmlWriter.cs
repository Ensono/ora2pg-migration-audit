using System.Text;
using Ora2Pg.Common.Writers;
using Ora2PgDataTypeValidator.Models;

namespace Ora2PgDataTypeValidator.src.Writers;


public class DataTypeValidationHtmlWriter : BaseHtmlReportWriter
{
    public void WriteHtmlReport(ValidationResult result, string outputPath)
    {
        var html = GenerateHtml(result);
        File.WriteAllText(outputPath, html);
    }

    private string GenerateHtml(ValidationResult result)
    {
        var sb = new StringBuilder();

        sb.Append(GenerateHtmlHeader("Oracle to PostgreSQL Data Type Validation Report"));

        var metadata = new Dictionary<string, string>
        {
            { "Oracle Schema", result.OracleSchema },
            { "PostgreSQL Schema", result.PostgresSchema },
            { "Validation Time", result.ValidationTime.ToString("yyyy-MM-dd HH:mm:ss") },
            { "Total Columns Validated", result.TotalColumnsValidated.ToString("N0") }
        };
        sb.Append(GenerateMetadataSection(metadata));

        sb.Append(GenerateStatusBadge(result.OverallStatus));

        var summaryMetrics = new List<SummaryMetric>
        {
            new("Total Columns Validated", result.TotalColumnsValidated.ToString("N0"), "📊", null),
            new("Critical Issues", result.CriticalIssues.ToString(), 
                result.CriticalIssues == 0 ? "✅" : "🔴",
                result.CriticalIssues == 0 ? "match" : "mismatch"),
            new("Errors", result.Errors.ToString(), 
                result.Errors == 0 ? "✅" : "❌",
                result.Errors == 0 ? "match" : "mismatch"),
            new("Warnings", result.Warnings.ToString(), 
                result.Warnings == 0 ? "✅" : "⚠️",
                result.Warnings == 0 ? "match" : "warning"),
            new("Info Messages", result.InfoMessages.ToString(), "ℹ️", null)
        };
        sb.Append(GenerateSummaryTable(summaryMetrics));
        

        if (result.Issues.Any())
        {
            sb.AppendLine("        <h2>Validation Details</h2>");
            sb.AppendLine("        <table>");
            sb.AppendLine("            <tr>");
            sb.AppendLine("                <th>Severity</th>");
            sb.AppendLine("                <th>Table.Column</th>");
            sb.AppendLine("                <th>Oracle Type</th>");
            sb.AppendLine("                <th>PostgreSQL Type</th>");
            sb.AppendLine("                <th>Category</th>");
            sb.AppendLine("                <th>Issue & Recommendation</th>");
            sb.AppendLine("            </tr>");
            
            var issuesToShow = result.Issues.Take(100).ToList();
            
            foreach (var issue in issuesToShow)
            {
                var severityIcon = GetSeverityIcon(issue.Severity.ToString());
                var rowClass = GetSeverityCssClass(issue.Severity.ToString());
                
                sb.AppendLine($"            <tr class=\"{rowClass}\">");
                sb.AppendLine($"                <td>{severityIcon} {issue.Severity}</td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.TableName)}.{EscapeHtml(issue.ColumnName)}</span></td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.OracleType)}</span></td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.PostgresType)}</span></td>");
                sb.AppendLine($"                <td>{EscapeHtml(issue.Category)}</td>");
                sb.AppendLine("                <td>");
                sb.AppendLine($"                    <strong>Issue:</strong> {EscapeHtml(issue.Message)}");
                
                if (!string.IsNullOrWhiteSpace(issue.Recommendation))
                {
                    sb.AppendLine("                    <div class=\"detail-box\" style=\"margin-top: 5px;\">");
                    sb.AppendLine($"                        <strong>💡 Recommendation:</strong> {EscapeHtml(issue.Recommendation)}");
                    sb.AppendLine("                    </div>");
                }
                
                if (issue.Metadata.Any())
                {
                    sb.AppendLine("                    <div style=\"margin-top: 5px; font-size: 0.9em; color: #666;\">");
                    foreach (var meta in issue.Metadata.Take(3))
                    {
                        sb.AppendLine($"                        <div><em>{EscapeHtml(meta.Key)}:</em> {EscapeHtml(meta.Value)}</div>");
                    }
                    sb.AppendLine("                    </div>");
                }
                
                sb.AppendLine("                </td>");
                sb.AppendLine("            </tr>");
            }
            
            sb.AppendLine("        </table>");
            
            if (result.Issues.Count > 100)
            {
                sb.AppendLine($"        <p><em>... and {result.Issues.Count - 100} more issues</em></p>");
            }
        }
        else
        {
            sb.AppendLine("        <div class=\"detail-box\">");
            sb.AppendLine("            <p style=\"color: #28a745; font-size: 1.1em;\">✅ <strong>Perfect!</strong> All data types are compatible and properly mapped.</p>");
            sb.AppendLine("        </div>");
        }
        

        if (result.Issues.Any())
        {
            var issuesByCategory = result.Issues
                .GroupBy(i => i.Category)
                .Select(g => new { Category = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count)
                .ToList();
            
            sb.AppendLine("        <h2>Issues by Category</h2>");
            sb.AppendLine("        <table>");
            sb.AppendLine("            <tr>");
            sb.AppendLine("                <th>Category</th>");
            sb.AppendLine("                <th style=\"text-align: right;\">Count</th>");
            sb.AppendLine("            </tr>");
            
            foreach (var category in issuesByCategory)
            {
                sb.AppendLine("            <tr>");
                sb.AppendLine($"                <td>{EscapeHtml(category.Category)}</td>");
                sb.AppendLine($"                <td class=\"number\">{category.Count}</td>");
                sb.AppendLine("            </tr>");
            }
            
            sb.AppendLine("        </table>");
        }

        sb.Append(GenerateHtmlFooter());
        
        return sb.ToString();
    }
}
