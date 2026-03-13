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
        
        var statusIcon = result.HasCriticalIssues ? "\u274C" : "\u2705";  // ❌ : ✅
        var statusText = result.HasCriticalIssues ? "FAILED" : "PASSED";
        sb.AppendLine($"**Status:** {statusIcon} {statusText}");
        sb.AppendLine();

        sb.AppendLine("### Schema Overview");
        sb.AppendLine();
        sb.AppendLine("| Object Type | Oracle | PostgreSQL | Match |");
        sb.AppendLine("|------------|--------|------------|-------|");
        
        var tablesMatch = result.OracleLogicalTableCount == result.PostgresLogicalTableCount ? "\u2705" : "\u274C";  // ✅ : ❌
        var columnsMatch = result.OracleLogicalColumnCount == result.PostgresLogicalColumnCount ? "\u2705" : "\u274C";  // ✅ : ❌
        var pksMatch = result.OracleLogicalPrimaryKeyCount == result.PostgresLogicalPrimaryKeyCount ? "\u2705" : "\u274C";  // ✅ : ❌
        var fksMatch = result.OracleLogicalForeignKeyCount == result.PostgresLogicalForeignKeyCount ? "\u2705" : "\u274C";  // ✅ : ❌
        
        sb.AppendLine($"| Tables | {result.OracleLogicalTableCount} | {result.PostgresLogicalTableCount} | {tablesMatch} |");
        sb.AppendLine($"| Columns | {result.OracleLogicalColumnCount} | {result.PostgresLogicalColumnCount} | {columnsMatch} |");
        sb.AppendLine($"| Primary Keys | {result.OracleLogicalPrimaryKeyCount} | {result.PostgresLogicalPrimaryKeyCount} | {pksMatch} |");
        
        if (result.SyntheticPrimaryKeyCount > 0)
        {
            sb.AppendLine($"| Synthetic PKs (DMS rowid) | 0 | {result.SyntheticPrimaryKeyCount} | \u26A0\uFE0F |");  // ⚠️
        }
        
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
                    var severity = GetIssueSeverity(issue);
                    var badge = severity switch
                    {
                        "Critical" => "🔴 **CRITICAL**",
                        "Warning" => "⚠️ **WARNING**",
                        "Info" => "ℹ️ **INFO**",
                        _ => ""
                    };
                    sb.AppendLine($"- {badge} {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.ConstraintIssues.Any())
            {
                sb.AppendLine("### Constraint Issues");
                sb.AppendLine();
                foreach (var issue in result.ConstraintIssues)
                {
                    var severity = GetIssueSeverity(issue);
                    var badge = severity switch
                    {
                        "Critical" => "🔴 **CRITICAL**",
                        "Warning" => "⚠️ **WARNING**",
                        "Info" => "ℹ️ **INFO**",
                        _ => ""
                    };
                    sb.AppendLine($"- {badge} {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.IndexIssues.Any())
            {
                sb.AppendLine("### Index Issues");
                sb.AppendLine();
                foreach (var issue in result.IndexIssues)
                {
                    var severity = GetIssueSeverity(issue);
                    var badge = severity switch
                    {
                        "Critical" => "🔴 **CRITICAL**",
                        "Warning" => "⚠️ **WARNING**",
                        "Info" => "ℹ️ **INFO**",
                        _ => ""
                    };
                    sb.AppendLine($"- {badge} {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }

            if (result.CodeObjectIssues.Any())
            {
                sb.AppendLine("### Code Object Issues");
                sb.AppendLine();
                foreach (var issue in result.CodeObjectIssues)
                {
                    var severity = GetIssueSeverity(issue);
                    var badge = severity switch
                    {
                        "Critical" => "🔴 **CRITICAL**",
                        "Warning" => "⚠️ **WARNING**",
                        "Info" => "ℹ️ **INFO**",
                        _ => ""
                    };
                    sb.AppendLine($"- {badge} {EscapeMarkdown(issue)}");
                }
                sb.AppendLine();
            }
        }
        else
        {
            sb.AppendLine("## \u2705 No Issues Found");
            sb.AppendLine();
            sb.AppendLine("All schema objects have been successfully migrated from Oracle to PostgreSQL with no discrepancies.");
            sb.AppendLine();
        }

        bool hasZeroCounts = result.OracleSchema.SequenceCount == 0 || result.OracleSchema.TriggerCount == 0 ||
                             result.OracleSchema.ProcedureCount == 0 || result.OracleSchema.FunctionCount == 0;
        
        if (result.HasOracleExtractionErrors)
        {
            sb.AppendLine("---");
            sb.AppendLine();
            sb.AppendLine("## \u26A0\uFE0F Warning: Oracle Code Objects Extraction Errors");
            sb.AppendLine();
            sb.AppendLine("The following errors occurred while extracting Oracle code objects:");
            sb.AppendLine();
            foreach (var error in result.OracleExtractionErrors)
            {
                sb.AppendLine($"- `{error}`");
            }
            sb.AppendLine();
            sb.AppendLine("These errors may occur due to:");
            sb.AppendLine();
            sb.AppendLine("- **User Permissions:** The database user may lack SELECT privileges on system views (`all_sequences`, `all_triggers`, `all_objects`).");
            sb.AppendLine("- **Schema Scope:** Objects may exist in different schemas or be owned by system accounts.");
            sb.AppendLine("- **Database Configuration:** Certain Oracle configurations or versions may restrict metadata access.");
            sb.AppendLine();
            sb.AppendLine("*Check application logs for detailed information and verify database permissions.*");
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

    private string GetIssueSeverity(string issue)
    {
        if (issue.Contains("❌")) return "Critical";
        if (issue.Contains("⚠️")) return "Warning";
        if (issue.Contains("ℹ️") || issue.Contains("ℹ")) return "Info";
        return "Info";
    }
}
