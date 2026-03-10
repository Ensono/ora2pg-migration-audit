using System.Text;
using Ora2Pg.Common.Writers;
using Ora2PgSchemaComparer.Comparison;

namespace Ora2PgSchemaComparer.src.Writers;


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
                result.TotalIssues == 0 ? "\u2705" : "\u274C",  // ✅ : ❌
                result.TotalIssues == 0 ? "match" : "mismatch"),
            new("Critical Issues", (result.HasCriticalIssues ? "Yes" : "No"), 
                result.HasCriticalIssues ? "\U0001F534" : "\u2705",  // 🔴 : ✅
                result.HasCriticalIssues ? "mismatch" : "match"),
            new("Table Issues", result.TableIssues.Count.ToString(), 
                result.TableIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F",  // ✅ : ⚠️
                result.TableIssues.Count == 0 ? "match" : "warning"),
            new("Constraint Issues", result.ConstraintIssues.Count.ToString(), 
                result.ConstraintIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F",  // ✅ : ⚠️
                result.ConstraintIssues.Count == 0 ? "match" : "warning"),
            new("Index Issues", result.IndexIssues.Count.ToString(), 
                result.IndexIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F",  // ✅ : ⚠️
                result.IndexIssues.Count == 0 ? "match" : "warning"),
            new("Code Object Issues", result.CodeObjectIssues.Count.ToString(), 
                result.CodeObjectIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F",  // ✅ : ⚠️
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
        
        AddComparisonRow(sb, "Tables", result.OracleLogicalTableCount, result.PostgresLogicalTableCount);
        AddComparisonRow(sb, "Columns", result.OracleLogicalColumnCount, result.PostgresLogicalColumnCount);
        AddComparisonRow(sb, "Primary Keys", result.OracleLogicalPrimaryKeyCount, result.PostgresLogicalPrimaryKeyCount);
        
        if (result.SyntheticPrimaryKeyCount > 0)
        {
            sb.AppendLine("            <tr class=\"warning\">");
            sb.AppendLine("                <td>Synthetic PKs (DMS rowid)</td>");
            sb.AppendLine("                <td class=\"number\">0</td>");
            sb.AppendLine($"                <td class=\"number\">{result.SyntheticPrimaryKeyCount}</td>");
            sb.AppendLine($"                <td class=\"number\">+{result.SyntheticPrimaryKeyCount}</td>");
            sb.AppendLine("            </tr>");
        }
        
        AddComparisonRow(sb, "Foreign Keys", result.OracleLogicalForeignKeyCount, result.PostgresLogicalForeignKeyCount);
        AddComparisonRow(sb, "Unique Constraints", result.OracleLogicalUniqueConstraintCount, result.PostgresLogicalUniqueConstraintCount);
        AddComparisonRow(sb, "Check Constraints", result.OracleLogicalCheckConstraintCount, result.PostgresLogicalCheckConstraintCount);
        AddComparisonRow(sb, "Indexes", result.OracleLogicalIndexCount, result.PostgresLogicalIndexCount);
        
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
            sb.AppendLine("            <p style=\"color: #28a745; font-size: 1.1em;\">\u2705 <strong>Perfect Match!</strong> All schema components match between Oracle and PostgreSQL.</p>");
            sb.AppendLine("        </div>");
        }

        bool hasZeroCounts = result.OracleSchema.SequenceCount == 0 || result.OracleSchema.TriggerCount == 0 ||
                             result.OracleSchema.ProcedureCount == 0 || result.OracleSchema.FunctionCount == 0;
        
        if (result.HasOracleExtractionErrors)
        {
            sb.AppendLine("        <div class=\"detail-box\" style=\"border-left-color: #dc3545; margin-top: 30px;\">");
            sb.AppendLine("            <h3 style=\"color: #721c24; margin-top: 0;\">\u26A0\uFE0F Warning: Oracle Code Objects Extraction Errors</h3>");
            sb.AppendLine("            <p>The following errors occurred while extracting Oracle code objects:</p>");
            sb.AppendLine("            <ul>");
            foreach (var error in result.OracleExtractionErrors)
            {
                sb.AppendLine($"                <li><code>{System.Web.HttpUtility.HtmlEncode(error)}</code></li>");
            }
            sb.AppendLine("            </ul>");
            sb.AppendLine("            <p>These errors may occur due to:</p>");
            sb.AppendLine("            <ul>");
            sb.AppendLine("                <li><strong>User Permissions:</strong> The database user may lack SELECT privileges on system views (<code>all_sequences</code>, <code>all_triggers</code>, <code>all_objects</code>).</li>");
            sb.AppendLine("                <li><strong>Schema Scope:</strong> Objects may exist in different schemas or be owned by system accounts.</li>");
            sb.AppendLine("                <li><strong>Database Configuration:</strong> Certain Oracle configurations or versions may restrict metadata access.</li>");
            sb.AppendLine("            </ul>");
            sb.AppendLine("            <p><em>Check application logs for detailed information and verify database permissions.</em></p>");
            sb.AppendLine("        </div>");
        }
        else if (hasZeroCounts)
        {
            sb.AppendLine("        <div class=\"detail-box\" style=\"border-left-color: #17a2b8; margin-top: 30px;\">");
            sb.AppendLine("            <h3 style=\"color: #17a2b8; margin-top: 0;\">\u2139\uFE0F Note: Oracle Code Objects Count</h3>");
            sb.AppendLine("            <p>Oracle shows 0 for some code objects (sequences, triggers, procedures/functions). This may be expected if:</p>");
            sb.AppendLine("            <ul>");
            sb.AppendLine("                <li><strong>No Objects Exist:</strong> The schema genuinely has no sequences, triggers, or stored procedures.</li>");
            sb.AppendLine("                <li><strong>User Permissions:</strong> The database user may lack SELECT privileges on system views (<code>all_sequences</code>, <code>all_triggers</code>, <code>all_objects</code>), causing queries to return 0 rows instead of failing.</li>");
            sb.AppendLine("                <li><strong>Schema Scope:</strong> Objects may exist in different schemas or be owned by system accounts.</li>");
            sb.AppendLine("                <li><strong>Database Configuration:</strong> Certain Oracle configurations or versions may restrict metadata access.</li>");
            sb.AppendLine("            </ul>");
            sb.AppendLine("            <p><em>If this is unexpected, check application logs and verify database permissions.</em></p>");
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
