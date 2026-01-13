using System.Text;

namespace Ora2Pg.Common.Writers;




public abstract class BaseHtmlReportWriter
{
    protected const string DefaultCss = @"
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; }
        .status { font-size: 1.2em; font-weight: bold; padding: 10px; border-radius: 4px; display: inline-block; margin: 10px 0; }
        .status.passed { background-color: #d4edda; color: #155724; }
        .status.failed { background-color: #f8d7da; color: #721c24; }
        .status.warning { background-color: #fff3cd; color: #856404; }
        .metadata { background-color: #f8f9fa; padding: 15px; border-radius: 4px; margin: 20px 0; }
        .metadata p { margin: 5px 0; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; font-weight: bold; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        tr:hover { background-color: #f5f5f5; }
        .match { color: #28a745; }
        .mismatch { color: #dc3545; font-weight: bold; }
        .warning { color: #ffc107; }
        .number { text-align: right; font-family: 'Courier New', monospace; }
        .pk-list { margin: 5px 0; padding: 10px; background-color: #fff3cd; border-left: 4px solid #ffc107; }
        .pk-list ul { margin: 5px 0; padding-left: 20px; }
        .pk-list li { margin: 3px 0; font-family: 'Courier New', monospace; font-size: 0.9em; }
        .code { font-family: 'Courier New', monospace; background-color: #f4f4f4; padding: 2px 6px; border-radius: 3px; }
        .detail-box { background-color: #f8f9fa; padding: 10px; margin: 10px 0; border-radius: 4px; border-left: 4px solid #007bff; }
        .info-icon { color: #17a2b8; }
        .success-icon { color: #28a745; }
        .error-icon { color: #dc3545; }
        .warning-icon { color: #ffc107; }
";

    protected string GenerateHtmlHeader(string title, string? additionalCss = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine("<!DOCTYPE html>");
        sb.AppendLine("<html>");
        sb.AppendLine("<head>");
        sb.AppendLine("    <meta charset=\"UTF-8\">");
        sb.AppendLine("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">");
        sb.AppendLine($"    <title>{EscapeHtml(title)}</title>");
        sb.AppendLine("    <style>");
        sb.AppendLine(DefaultCss);
        
        if (!string.IsNullOrWhiteSpace(additionalCss))
        {
            sb.AppendLine(additionalCss);
        }
        
        sb.AppendLine("    </style>");
        sb.AppendLine("</head>");
        sb.AppendLine("<body>");
        sb.AppendLine("    <div class=\"container\">");
        sb.AppendLine($"        <h1>{EscapeHtml(title)}</h1>");
        
        return sb.ToString();
    }


    protected string GenerateMetadataSection(Dictionary<string, string> metadata)
    {
        var sb = new StringBuilder();
        sb.AppendLine("        <div class=\"metadata\">");
        
        foreach (var kvp in metadata)
        {
            sb.AppendLine($"            <p><strong>{EscapeHtml(kvp.Key)}:</strong> {EscapeHtml(kvp.Value)}</p>");
        }
        
        sb.AppendLine("        </div>");
        return sb.ToString();
    }


    protected string GenerateStatusBadge(string status)
    {
        var statusClass = status.ToUpper() switch
        {
            "PASSED" => "passed",
            "WARNING" => "warning",
            "FAILED" => "failed",
            _ => "warning"
        };
        
        var statusIcon = status.ToUpper() switch
        {
            "PASSED" => "✅",
            "WARNING" => "⚠️",
            "FAILED" => "❌",
            _ => "ℹ️"
        };
        
        var sb = new StringBuilder();
        sb.AppendLine($"        <div class=\"status {statusClass}\">");
        sb.AppendLine($"            {statusIcon} Status: {status}");
        sb.AppendLine("        </div>");
        
        return sb.ToString();
    }


    protected string GenerateSummaryTable(List<SummaryMetric> metrics)
    {
        var sb = new StringBuilder();
        sb.AppendLine("        <h2>Summary Statistics</h2>");
        sb.AppendLine("        <table>");
        sb.AppendLine("            <tr>");
        sb.AppendLine("                <th>Metric</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Count</th>");
        sb.AppendLine("            </tr>");
        
        foreach (var metric in metrics)
        {
            var rowClass = metric.CssClass ?? "";
            var icon = metric.Icon ?? "";
            
            sb.AppendLine($"            <tr{(string.IsNullOrWhiteSpace(rowClass) ? "" : $" class=\"{rowClass}\"")}>");
            sb.AppendLine($"                <td>{icon} {EscapeHtml(metric.Label)}</td>");
            sb.AppendLine($"                <td class=\"number\">{metric.Value}</td>");
            sb.AppendLine("            </tr>");
        }
        
        sb.AppendLine("        </table>");
        return sb.ToString();
    }


    protected string GenerateHtmlFooter()
    {
        var sb = new StringBuilder();
        sb.AppendLine("    </div>");
        sb.AppendLine("</body>");
        sb.AppendLine("</html>");
        return sb.ToString();
    }


    protected string EscapeHtml(string text)
    {
        if (string.IsNullOrEmpty(text))
            return string.Empty;

        return text
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&#39;");
    }


    protected string FormatNumber(long number) => number.ToString("N0");


    protected string GetSeverityIcon(string severity)
    {
        return severity.ToUpper() switch
        {
            "CRITICAL" => "🔴",
            "ERROR" => "❌",
            "WARNING" => "⚠️",
            "INFO" => "ℹ️",
            _ => "ℹ️"
        };
    }


    protected string GetSeverityCssClass(string severity)
    {
        return severity.ToUpper() switch
        {
            "CRITICAL" => "mismatch",
            "ERROR" => "mismatch",
            "WARNING" => "warning",
            _ => "match"
        };
    }
}


public record SummaryMetric(string Label, string Value, string? Icon = null, string? CssClass = null);
