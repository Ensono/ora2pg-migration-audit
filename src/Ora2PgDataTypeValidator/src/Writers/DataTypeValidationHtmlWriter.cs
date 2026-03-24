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
        
        sb.AppendLine("        <div style=\"background-color: #e7f3ff; border-left: 4px solid #2196F3; padding: 12px; margin: 20px 0; border-radius: 4px;\">");
        sb.AppendLine("            <strong>🆕 DMS-Pattern Based Validation</strong>");
        sb.AppendLine("            <p style=\"margin: 5px 0 0 0; color: #555;\">Validates against actual GCP Database Migration Service conversion patterns extracted from production migrations.</p>");
        sb.AppendLine("        </div>");

        var metadata = new Dictionary<string, string>();
        
        if (!string.IsNullOrEmpty(result.OracleDatabase))
        {
            metadata.Add("Oracle Database", result.OracleDatabase);
        }
        if (!string.IsNullOrEmpty(result.PostgresDatabase))
        {
            metadata.Add("PostgreSQL Database", result.PostgresDatabase);
        }
        
        metadata.Add("Oracle Schema", result.OracleSchema);
        metadata.Add("PostgreSQL Schema", result.PostgresSchema);
        metadata.Add("Validation Time", result.ValidationTime.ToString("yyyy-MM-dd HH:mm:ss"));
        metadata.Add("Total Columns Validated", result.TotalColumnsValidated.ToString("N0"));
        
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
                sb.AppendLine($"                <td>{GetCategoryIcon(category.Category)} {EscapeHtml(category.Category)}</td>");
                sb.AppendLine($"                <td class=\"number\">{category.Count}</td>");
                sb.AppendLine("            </tr>");
            }
            
            sb.AppendLine("        </table>");
        }

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
            
            var sortedIssues = result.Issues
                .OrderBy(i => i.Severity switch
                {
                    ValidationSeverity.Critical => 1,
                    ValidationSeverity.Error => 2,
                    ValidationSeverity.Warning => 3,
                    ValidationSeverity.Info => 4,
                    _ => 5
                })
                .ThenBy(i => i.TableName)
                .ThenBy(i => i.ColumnName);
            
            foreach (var issue in sortedIssues)
            {
                var severityIcon = GetSeverityIcon(issue.Severity.ToString());
                var rowClass = GetSeverityCssClass(issue.Severity.ToString());
                
                sb.AppendLine($"            <tr class=\"{rowClass}\">");
                sb.AppendLine($"                <td>{severityIcon} {issue.Severity}</td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.TableName)}.{EscapeHtml(issue.ColumnName)}</span></td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.OracleType)}</span></td>");
                sb.AppendLine($"                <td><span class=\"code\">{EscapeHtml(issue.PostgresType)}</span></td>");
                sb.AppendLine($"                <td>{GetCategoryIcon(issue.Category)} {EscapeHtml(issue.Category)}</td>");
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
        }
        else
        {
            sb.AppendLine("        <div class=\"detail-box\">");
            sb.AppendLine("            <p style=\"color: #28a745; font-size: 1.1em;\">✅ <strong>Perfect!</strong> All data types are compatible and properly mapped.</p>");
            sb.AppendLine("        </div>");
        }

        sb.Append(GenerateHtmlFooter());
        
        return sb.ToString();
    }

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
