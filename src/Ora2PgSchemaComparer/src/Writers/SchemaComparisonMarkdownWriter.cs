using System.Text;
using Ora2PgSchemaComparer.Comparison;

namespace Ora2PgSchemaComparer.src.Writers;


public class SchemaComparisonMarkdownWriter
{
    public void WriteMarkdownReport(ComparisonResult result, string outputPath)
    {
        var markdown = GenerateMarkdown(result);
        File.WriteAllText(outputPath, markdown);
    }

    private string GenerateMarkdown(ComparisonResult result)
    {
        var sb = new StringBuilder();

        sb.AppendLine("# Oracle to PostgreSQL Schema Comparison Report");
        sb.AppendLine();
        sb.AppendLine("## Metadata");
        sb.AppendLine();
        sb.AppendLine($"- **Source (Oracle):** {result.OracleSchema.SchemaName} schema");
        sb.AppendLine($"- **Target (PostgreSQL):** {result.PostgresSchema.SchemaName} schema");
        sb.AppendLine($"- **Comparison Date:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"- **Overall Grade:** {result.OverallGrade}");
        sb.AppendLine();

        sb.AppendLine("## Executive Summary");
        sb.AppendLine();
        
        var statusIcon = result.HasCriticalIssues ? "❌" : "✅";
        var statusText = result.HasCriticalIssues ? "FAILED" : "PASSED";
        sb.AppendLine($"**Status:** {statusIcon} {statusText}");
        sb.AppendLine();

        sb.AppendLine("### Schema Overview");
        sb.AppendLine();
        sb.AppendLine("| Object Type | Oracle | PostgreSQL | Match |");
        sb.AppendLine("|------------|--------|------------|-------|");
        
        var tablesMatch = result.OracleLogicalTableCount == result.PostgresLogicalTableCount ? "✅" : "❌";
        var columnsMatch = result.OracleLogicalColumnCount == result.PostgresLogicalColumnCount ? "✅" : "❌";
        var pksMatch = result.OracleLogicalPrimaryKeyCount == result.PostgresLogicalPrimaryKeyCount ? "✅" : "❌";
        var fksMatch = result.OracleLogicalForeignKeyCount == result.PostgresLogicalForeignKeyCount ? "✅" : "❌";
        
        sb.AppendLine($"| Tables | {result.OracleLogicalTableCount} | {result.PostgresLogicalTableCount} | {tablesMatch} |");
        sb.AppendLine($"| Columns | {result.OracleLogicalColumnCount} | {result.PostgresLogicalColumnCount} | {columnsMatch} |");
        sb.AppendLine($"| Primary Keys | {result.OracleLogicalPrimaryKeyCount} | {result.PostgresLogicalPrimaryKeyCount} | {pksMatch} |");
        sb.AppendLine($"| Foreign Keys | {result.OracleLogicalForeignKeyCount} | {result.PostgresLogicalForeignKeyCount} | {fksMatch} |");
        sb.AppendLine();

        sb.AppendLine("### Issues Summary");
        sb.AppendLine();
        sb.AppendLine("| Category | Count |");
        sb.AppendLine("|----------|-------|");
        sb.AppendLine($"| Table Issues | {result.TableIssues.Count} |");
        sb.AppendLine($"| Constraint Issues | {result.ConstraintIssues.Count} |");
        sb.AppendLine($"| Index Issues | {result.IndexIssues.Count} |");
        sb.AppendLine($"| Code Object Issues | {result.CodeObjectIssues.Count} |");
        sb.AppendLine($"| **Total Issues** | **{result.TotalIssues}** |");
        sb.AppendLine();

        if (result.TotalIssues > 0)
        {
            sb.AppendLine("## Detailed Issues");
            sb.AppendLine();

            if (result.TableIssues.Any())
            {
                sb.AppendLine("### Table Issues");
                sb.AppendLine();
                foreach (var issue in result.TableIssues)
                {
                    sb.AppendLine($"- {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.ConstraintIssues.Any())
            {
                sb.AppendLine("### Constraint Issues");
                sb.AppendLine();
                foreach (var issue in result.ConstraintIssues)
                {
                    sb.AppendLine($"- {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.IndexIssues.Any())
            {
                sb.AppendLine("### Index Issues");
                sb.AppendLine();
                foreach (var issue in result.IndexIssues)
                {
                    sb.AppendLine($"- {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.CodeObjectIssues.Any())
            {
                sb.AppendLine("### Code Object Issues");
                sb.AppendLine();
                foreach (var issue in result.CodeObjectIssues)
                {
                    sb.AppendLine($"- {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }
        }
        else
        {
            sb.AppendLine("## ✅ No Issues Found");
            sb.AppendLine();
            sb.AppendLine("All schema objects have been successfully migrated from Oracle to PostgreSQL with no discrepancies.");
            sb.AppendLine();
        }

        sb.AppendLine("---");
        sb.AppendLine();
        sb.AppendLine($"*Report generated on {DateTime.Now:yyyy-MM-dd HH:mm:ss}*");

        return sb.ToString();
    }

    private string EscapeMarkdown(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        return text.Replace("|", "\\|");
    }
}
