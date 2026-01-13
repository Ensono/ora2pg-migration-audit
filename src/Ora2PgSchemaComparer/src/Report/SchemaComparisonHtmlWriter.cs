using System.Text;
using Ora2Pg.Common.Reports;
using Ora2PgSchemaComparer.Comparison;

namespace Ora2PgSchemaComparer.Report;


public class SchemaComparisonHtmlWriter : BaseHtmlReportWriter
{
    public void WriteHtmlReport(ComparisonResult result, string outputPath)
    {
        var html = GenerateHtml(result);
        File.WriteAllText(outputPath, html);
    }

    private string GenerateHtml(ComparisonResult result)
    {
        var sb = new StringBuilder();

        sb.Append(GenerateHtmlHeader("Oracle to PostgreSQL Schema Comparison Report"));

        var metadata = new Dictionary<string, string>
        {
            { "Oracle Schema", result.OracleSchema.SchemaName },
            { "PostgreSQL Schema", result.PostgresSchema.SchemaName },
            { "Comparison Date", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") },
            { "Overall Grade", result.OverallGrade }
        };
        sb.Append(GenerateMetadataSection(metadata));

        var status = result.TotalIssues == 0 ? "PASSED" : 
                    result.HasCriticalIssues ? "FAILED" : "WARNING";
        sb.Append(GenerateStatusBadge(status));
        

        var summaryMetrics = new List<SummaryMetric>
        {
            new("Total Issues", result.TotalIssues.ToString(), 
                result.TotalIssues == 0 ? "✅" : "❌",
                result.TotalIssues == 0 ? "match" : "mismatch"),
            new("Critical Issues", (result.HasCriticalIssues ? "Yes" : "No"), 
                result.HasCriticalIssues ? "🔴" : "✅",
                result.HasCriticalIssues ? "mismatch" : "match"),
            new("Table Issues", result.TableIssues.Count.ToString(), 
                result.TableIssues.Count == 0 ? "✅" : "⚠️",
                result.TableIssues.Count == 0 ? "match" : "warning"),
            new("Constraint Issues", result.ConstraintIssues.Count.ToString(), 
                result.ConstraintIssues.Count == 0 ? "✅" : "⚠️",
                result.ConstraintIssues.Count == 0 ? "match" : "warning"),
            new("Index Issues", result.IndexIssues.Count.ToString(), 
                result.IndexIssues.Count == 0 ? "✅" : "⚠️",
                result.IndexIssues.Count == 0 ? "match" : "warning"),
            new("Code Object Issues", result.CodeObjectIssues.Count.ToString(), 
                result.CodeObjectIssues.Count == 0 ? "✅" : "⚠️",
                result.CodeObjectIssues.Count == 0 ? "match" : "warning")
        };
        sb.Append(GenerateSummaryTable(summaryMetrics));

        sb.AppendLine("        <h2>Schema Overview</h2>");
        sb.AppendLine("        <table>");
        sb.AppendLine("            <tr>");
        sb.AppendLine("                <th>Component</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Oracle</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">PostgreSQL</th>");
        sb.AppendLine("                <th style=\"text-align: right;\">Difference</th>");
        sb.AppendLine("            </tr>");
        
        AddComparisonRow(sb, "Tables", result.OracleSchema.TableCount, result.PostgresSchema.TableCount);
        AddComparisonRow(sb, "Columns", result.OracleSchema.ColumnCount, result.PostgresSchema.ColumnCount);
        AddComparisonRow(sb, "Primary Keys", result.OracleSchema.PrimaryKeyCount, result.PostgresSchema.PrimaryKeyCount);
        AddComparisonRow(sb, "Foreign Keys", result.OracleSchema.ForeignKeyCount, result.PostgresSchema.ForeignKeyCount);
        AddComparisonRow(sb, "Unique Constraints", result.OracleSchema.UniqueConstraintCount, result.PostgresSchema.UniqueConstraintCount);
        AddComparisonRow(sb, "Check Constraints", result.OracleSchema.CheckConstraintCount, result.PostgresSchema.CheckConstraintCount);
        AddComparisonRow(sb, "Indexes", result.OracleSchema.IndexCount, result.PostgresSchema.IndexCount);
        
        sb.AppendLine("        </table>");

        if (result.TotalIssues > 0)
        {
            sb.AppendLine("        <h2>Detailed Issues</h2>");

            if (result.TableIssues.Any())
            {
                sb.AppendLine("        <h3>Table & Structure Issues</h3>");
                GenerateIssuesTable(sb, result.TableIssues);
            }

            if (result.ConstraintIssues.Any())
            {
                sb.AppendLine("        <h3>Constraint Issues</h3>");
                GenerateIssuesTable(sb, result.ConstraintIssues);
            }
            
            if (result.IndexIssues.Any())
            {
                sb.AppendLine("        <h3>Index Issues</h3>");
                GenerateIssuesTable(sb, result.IndexIssues);
            }
            
            if (result.CodeObjectIssues.Any())
            {
                sb.AppendLine("        <h3>Code Object Issues</h3>");
                GenerateIssuesTable(sb, result.CodeObjectIssues);
            }
        }
        else
        {
            sb.AppendLine("        <div class=\"detail-box\">");
            sb.AppendLine("            <p style=\"color: #28a745; font-size: 1.1em;\">✅ <strong>Perfect Match!</strong> All schema components match between Oracle and PostgreSQL.</p>");
            sb.AppendLine("        </div>");
        }

        sb.Append(GenerateHtmlFooter());
        
        return sb.ToString();
    }

    private void AddComparisonRow(StringBuilder sb, string component, int oracleCount, int postgresCount)
    {
        var diff = postgresCount - oracleCount;
        var diffDisplay = diff == 0 ? "✅ 0" : $"❌ {diff:+#;-#;0}";
        var rowClass = diff == 0 ? "match" : "mismatch";
        
        sb.AppendLine($"            <tr class=\"{rowClass}\">");
        sb.AppendLine($"                <td>{EscapeHtml(component)}</td>");
        sb.AppendLine($"                <td class=\"number\">{FormatNumber(oracleCount)}</td>");
        sb.AppendLine($"                <td class=\"number\">{FormatNumber(postgresCount)}</td>");
        sb.AppendLine($"                <td class=\"number\">{diffDisplay}</td>");
        sb.AppendLine("            </tr>");
    }

    private void GenerateIssuesTable(StringBuilder sb, List<string> issues)
    {
        sb.AppendLine("        <table>");
        sb.AppendLine("            <tr>");
        sb.AppendLine("                <th>Severity</th>");
        sb.AppendLine("                <th>Issue Description</th>");
        sb.AppendLine("            </tr>");
        
        foreach (var issue in issues)
        {
            var isCritical = issue.Contains("❌");
            var isWarning = issue.Contains("⚠️");
            var isInfo = issue.Contains("ℹ️");
            
            var severity = isCritical ? "Critical" : isWarning ? "Warning" : "Info";
            var icon = GetSeverityIcon(severity);
            var rowClass = GetSeverityCssClass(severity);
            
            sb.AppendLine($"            <tr class=\"{rowClass}\">");
            sb.AppendLine($"                <td>{icon} {severity}</td>");
            sb.AppendLine($"                <td>{EscapeHtml(issue)}</td>");
            sb.AppendLine("            </tr>");
        }
        
        sb.AppendLine("        </table>");
    }
}
