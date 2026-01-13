using System.Text;
using Ora2Pg.Common.Writers;
using Ora2PgDataValidator.Comparison;

namespace Ora2PgDataValidator.src.Writers;

public class DataValidationHtmlWriter : BaseHtmlReportWriter
{
    public void WriteHtmlReport(List<ComparisonResult> results, string outputPath)
    {
        var html = GenerateHtml(results);
        File.WriteAllText(outputPath, html);
    }

    private string GenerateHtml(List<ComparisonResult> results)
    {
        var sb = new StringBuilder();
        

        sb.Append(GenerateHtmlHeader("Oracle to PostgreSQL Data Fingerprint Validation Report"));

        int totalTables = results.Count;
        int successfulTables = results.Count(r => !r.HasError && r.IsMatch);
        int failedTables = results.Count(r => !r.HasError && !r.IsMatch);
        int errorTables = results.Count(r => r.HasError);
        long totalSourceRows = results.Sum(r => (long)r.SourceRowCount);
        long totalTargetRows = results.Sum(r => (long)r.TargetRowCount);
        long totalMatchingRows = results.Sum(r => (long)r.MatchingRows);
        long totalMismatchedRows = results.Sum(r => (long)r.MismatchedRows);
        long totalMissingRows = results.Sum(r => (long)r.MissingInTarget);
        long totalExtraRows = results.Sum(r => (long)r.ExtraInTarget);
        
        double overallMatchPercentage = totalSourceRows > 0 
            ? (double)totalMatchingRows / totalSourceRows * 100.0 
            : 0.0;

        var metadata = new Dictionary<string, string>
        {
            { "Validation Date", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") },
            { "Total Tables Compared", totalTables.ToString("N0") },
            { "Overall Match Percentage", $"{overallMatchPercentage:F2}%" }
        };
        sb.Append(GenerateMetadataSection(metadata));

        var status = errorTables > 0 ? "FAILED" : 
                    failedTables > 0 ? "WARNING" : "PASSED";
        sb.Append(GenerateStatusBadge(status));

        var summaryMetrics = new List<SummaryMetric>
        {
            new("Total Tables", totalTables.ToString(), "📊", null),
            new("Successful Matches", successfulTables.ToString(), 
                successfulTables == totalTables ? "✅" : "🔴",
                successfulTables == totalTables ? "match" : "mismatch"),
            new("Failed Validations", failedTables.ToString(), 
                failedTables == 0 ? "✅" : "❌",
                failedTables == 0 ? "match" : "mismatch"),
            new("Errors", errorTables.ToString(), 
                errorTables == 0 ? "✅" : "🔴",
                errorTables == 0 ? "match" : "mismatch"),
            new("Total Source Rows", FormatNumber(totalSourceRows), "📈", null),
            new("Total Target Rows", FormatNumber(totalTargetRows), "📊", null),
            new("Matching Rows", FormatNumber(totalMatchingRows), "✅", "match"),
            new("Mismatched Rows", FormatNumber(totalMismatchedRows), 
                totalMismatchedRows == 0 ? "✅" : "❌",
                totalMismatchedRows == 0 ? "match" : "mismatch"),
            new("Missing in Target", FormatNumber(totalMissingRows), 
                totalMissingRows == 0 ? "✅" : "⚠️",
                totalMissingRows == 0 ? "match" : "warning"),
            new("Extra in Target", FormatNumber(totalExtraRows), 
                totalExtraRows == 0 ? "✅" : "⚠️",
                totalExtraRows == 0 ? "match" : "warning")
        };
        sb.Append(GenerateSummaryTable(summaryMetrics));

        sb.AppendLine("        <h2>Validation Details</h2>");
        sb.AppendLine("        <table>");
        sb.AppendLine("            <tr>");
        sb.AppendLine("                <th>Status</th>");
        sb.AppendLine("                <th>Table Name</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Source Rows</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Target Rows</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Matching</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Mismatched</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Missing</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Extra</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Match %</th>");
        sb.AppendLine("                <th>Details</th>");
        sb.AppendLine("            </tr>");
        
        foreach (var result in results)
        {
            var rowClass = result.HasError ? "mismatch" : 
                          result.IsMatch ? "match" : "warning";
            
            var statusIcon = result.HasError ? "🔴" : 
                            result.IsMatch ? "✅" : "⚠️";
            
            var statusText = result.HasError ? "Error" : 
                            result.IsMatch ? "Match" : "Mismatch";
            
            sb.AppendLine($"            <tr class=\"{rowClass}\">");
            sb.AppendLine($"                <td>{statusIcon} {statusText}</td>");
            sb.AppendLine($"                <td><strong>{EscapeHtml(result.SourceTable)}</strong></td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.SourceRowCount)}</td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.TargetRowCount)}</td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.MatchingRows)}</td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.MismatchedRows)}</td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.MissingInTarget)}</td>");
            sb.AppendLine($"                <td class=\"number\">{FormatNumber(result.ExtraInTarget)}</td>");
            sb.AppendLine($"                <td class=\"number\">{result.MatchPercentage:F2}%</td>");
            sb.AppendLine("                <td>");
            
            if (result.HasError)
            {
                sb.AppendLine($"                    <div class=\"detail-box\" style=\"border-left-color: #dc3545;\">");
                sb.AppendLine($"                        <strong>Error:</strong> {EscapeHtml(result.Error ?? "Unknown error")}");
                sb.AppendLine("                    </div>");
            }
            else if (!result.IsMatch)
            {
                if (result.MismatchedRows > 0)
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine($"                        <strong>Mismatched Rows:</strong> {result.MismatchedRows:N0}");
                    
                    if (result.MismatchedRowDetails.Any())
                    {
                        sb.AppendLine("                        <ul>");
                        var mismatchToShow = result.MismatchedRowDetails.Take(5);
                        foreach (var kvp in mismatchToShow)
                        {
                            sb.Append($"                            <li>Row #{kvp.Key}");
                            

                            if (result.MismatchedRowPrimaryKeys.TryGetValue(kvp.Key, out var pkPair))
                            {
                                var oraclePk = string.Join(", ", pkPair.OraclePrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
                                sb.Append($" <code style=\"font-size: 0.9em;\">[PK: {EscapeHtml(oraclePk)}]</code>");
                            }
                            
                            sb.AppendLine($": Hash mismatch</li>");
                        }
                        if (result.MismatchedRowDetails.Count > 5)
                        {
                            sb.AppendLine($"                            <li><em>... and {result.MismatchedRowDetails.Count - 5} more</em></li>");
                        }
                        sb.AppendLine("                        </ul>");
                    }
                    sb.AppendLine("                    </div>");
                }
                
                if (result.MissingInTarget > 0)
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine($"                        <strong>Missing in Target:</strong> {result.MissingInTarget:N0} rows");
                    
                    if (result.MissingRows.Any())
                    {
                        sb.AppendLine("                        <ul>");
                        var missingToShow = result.MissingRows.Take(5);
                        foreach (var kvp in missingToShow)
                        {
                            sb.Append($"                            <li>Row #{kvp.Key}");
                            

                            if (result.MissingRowPrimaryKeys.TryGetValue(kvp.Key, out var pkValues))
                            {
                                var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                                sb.Append($" <code style=\"font-size: 0.9em;\">[PK: {EscapeHtml(pk)}]</code>");
                            }
                            
                            sb.AppendLine("</li>");
                        }
                        if (result.MissingRows.Count > 5)
                        {
                            sb.AppendLine($"                            <li><em>... and {result.MissingRows.Count - 5} more</em></li>");
                        }
                        sb.AppendLine("                        </ul>");
                    }
                    sb.AppendLine("                    </div>");
                }
                
                if (result.ExtraInTarget > 0)
                {
                    sb.AppendLine("                    <div class=\"pk-list\">");
                    sb.AppendLine($"                        <strong>Extra in Target:</strong> {result.ExtraInTarget:N0} rows");
                    
                    if (result.ExtraRows.Any())
                    {
                        sb.AppendLine("                        <ul>");
                        var extraToShow = result.ExtraRows.Take(5);
                        foreach (var kvp in extraToShow)
                        {
                            sb.Append($"                            <li>Row #{kvp.Key}");
                            

                            if (result.ExtraRowPrimaryKeys.TryGetValue(kvp.Key, out var pkValues))
                            {
                                var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                                sb.Append($" <code style=\"font-size: 0.9em;\">[PK: {EscapeHtml(pk)}]</code>");
                            }
                            
                            sb.AppendLine("</li>");
                        }
                        if (result.ExtraRows.Count > 5)
                        {
                            sb.AppendLine($"                            <li><em>... and {result.ExtraRows.Count - 5} more</em></li>");
                        }
                        sb.AppendLine("                        </ul>");
                    }
                    sb.AppendLine("                    </div>");
                }
            }
            else
            {
                sb.AppendLine("                    -");
            }
            
            sb.AppendLine("                </td>");
            sb.AppendLine("            </tr>");
        }
        
        sb.AppendLine("        </table>");
        

        if (errorTables == 0 && failedTables == 0)
        {
            sb.AppendLine("        <div class=\"detail-box\">");
            sb.AppendLine("            <p style=\"color: #28a745; font-size: 1.1em;\">✅ <strong>Perfect Match!</strong> All tables have matching data fingerprints.</p>");
            sb.AppendLine("        </div>");
        }
        else if (errorTables > 0)
        {
            sb.AppendLine("        <div class=\"detail-box\" style=\"border-left-color: #dc3545;\">");
            sb.AppendLine($"            <p style=\"color: #dc3545; font-size: 1.1em;\">🔴 <strong>Errors Detected!</strong> {errorTables} table(s) encountered errors during validation.</p>");
            sb.AppendLine("        </div>");
        }
        else
        {
            sb.AppendLine("        <div class=\"detail-box\" style=\"border-left-color: #ffc107;\">");
            sb.AppendLine($"            <p style=\"color: #856404; font-size: 1.1em;\">⚠️ <strong>Data Mismatches Found!</strong> {failedTables} table(s) have data integrity issues.</p>");
            sb.AppendLine("        </div>");
        }

        sb.Append(GenerateHtmlFooter());
        
        return sb.ToString();
    }
}
