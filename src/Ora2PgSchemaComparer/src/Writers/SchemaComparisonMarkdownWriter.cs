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
        
        var status = result.TotalIssues == 0 ? "PASSED" : 
                    result.HasCriticalIssues ? "FAILED" : "WARNING";
        var statusIcon = status == "PASSED" ? "\u2705" : status == "FAILED" ? "\u274C" : "\u26A0\uFE0F";  // ✅ : ❌ : ⚠️
        sb.AppendLine($"**Status:** {statusIcon} **{status}**");
        sb.AppendLine();

        // Summary Metrics Table
        sb.AppendLine("### Summary Metrics");
        sb.AppendLine();
        sb.AppendLine("| Metric | Value | Status |");
        sb.AppendLine("|--------|-------|--------|");
        sb.AppendLine($"| Total Issues | {result.TotalIssues} | {(result.TotalIssues == 0 ? "\u2705" : "\u274C")} |");
        sb.AppendLine($"| Critical Issues | {(result.HasCriticalIssues ? "Yes" : "No")} | {(result.HasCriticalIssues ? "\U0001F534" : "\u2705")} |");
        sb.AppendLine($"| Table Issues | {result.TableIssues.Count} | {(result.TableIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F")} |");
        sb.AppendLine($"| Constraint Issues | {result.ConstraintIssues.Count} | {(result.ConstraintIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F")} |");
        sb.AppendLine($"| Index Issues | {result.IndexIssues.Count} | {(result.IndexIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F")} |");
        sb.AppendLine($"| Code Object Issues | {result.CodeObjectIssues.Count} | {(result.CodeObjectIssues.Count == 0 ? "\u2705" : "\u26A0\uFE0F")} |");
        sb.AppendLine();

        sb.AppendLine("### Schema Overview");
        sb.AppendLine();
        sb.AppendLine("| Component | Oracle | PostgreSQL | Difference |");
        sb.AppendLine("|-----------|--------|------------|------------|");
        
        AddMarkdownComparisonRow(sb, "Tables", result.OracleLogicalTableCount, result.PostgresLogicalTableCount);
        AddMarkdownComparisonRow(sb, "Columns", result.OracleLogicalColumnCount, result.PostgresLogicalColumnCount);
        AddMarkdownComparisonRow(sb, "Primary Keys", result.OracleLogicalPrimaryKeyCount, result.PostgresLogicalPrimaryKeyCount);
        
        if (result.SyntheticPrimaryKeyCount > 0)
        {
            sb.AppendLine($"| Synthetic PKs (DMS rowid) | 0 | {result.SyntheticPrimaryKeyCount} | \u26A0\uFE0F +{result.SyntheticPrimaryKeyCount} |");
        }
        
        AddMarkdownComparisonRow(sb, "Foreign Keys", result.OracleLogicalForeignKeyCount, result.PostgresLogicalForeignKeyCount);
        AddMarkdownComparisonRow(sb, "Unique Constraints", result.OracleLogicalUniqueConstraintCount, result.PostgresLogicalUniqueConstraintCount);
        AddMarkdownComparisonRow(sb, "Check Constraints", result.OracleLogicalCheckConstraintCount, result.PostgresLogicalCheckConstraintCount);
        AddMarkdownComparisonRow(sb, "Indexes", result.OracleLogicalIndexCount, result.PostgresLogicalIndexCount);
        sb.AppendLine();

        if (result.TotalIssues > 0)
        {
            sb.AppendLine("## Detailed Issues");
            sb.AppendLine();

            if (result.TableIssues.Any())
            {
                sb.AppendLine("### Table & Structure Issues");
                sb.AppendLine();
                GenerateIssuesMarkdownTable(sb, result.TableIssues);
                sb.AppendLine();
            }

            if (result.ConstraintIssues.Any())
            {
                sb.AppendLine("### Constraint Issues");
                sb.AppendLine();
                GenerateIssuesMarkdownTable(sb, result.ConstraintIssues);
                sb.AppendLine();
            }

            if (result.IndexIssues.Any())
            {
                sb.AppendLine("### Index Issues");
                sb.AppendLine();
                GenerateIssuesMarkdownTable(sb, result.IndexIssues);
                sb.AppendLine();
            }

            if (result.CodeObjectIssues.Any())
            {
                sb.AppendLine("### Code Object Issues");
                sb.AppendLine();
                GenerateCodeObjectIssuesMarkdownTable(sb, result.CodeObjectIssues);
                sb.AppendLine();
            }
        }
        else
        {
            sb.AppendLine("## \u2705 Perfect Match!");
            sb.AppendLine();
            sb.AppendLine("All schema components match between Oracle and PostgreSQL. No discrepancies found.");
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
            sb.AppendLine("**Possible Causes:**");
            sb.AppendLine();
            sb.AppendLine("- **User Permissions:** The database user may lack SELECT privileges on system views (`all_sequences`, `all_triggers`, `all_objects`).");
            sb.AppendLine("- **Schema Scope:** Objects may exist in different schemas or be owned by system accounts.");
            sb.AppendLine("- **Database Configuration:** Certain Oracle configurations or versions may restrict metadata access.");
            sb.AppendLine();
            sb.AppendLine("*Check application logs for detailed information and verify database permissions.*");
            sb.AppendLine();
        }

        // Add Grade Explanation
        sb.AppendLine("---");
        sb.AppendLine();
        sb.AppendLine("## Grade Explanation");
        sb.AppendLine();
        sb.AppendLine("| Grade | Criteria | Meaning |");
        sb.AppendLine("|-------|----------|---------|");
        sb.AppendLine("| **A** | ≤5 issues, no critical | Excellent - Ready for production |");
        sb.AppendLine("| **B** | ≤15 issues, no critical | Good - Minor adjustments needed |");
        sb.AppendLine("| **C** | ≤30 issues, no critical | Fair - Moderate fixes required |");
        sb.AppendLine("| **D** | >30 issues OR has critical | Poor - Significant issues |");
        sb.AppendLine("| **F** | Has critical issues | Failed - Critical problems |");
        sb.AppendLine();

        sb.AppendLine("---");
        sb.AppendLine();
        sb.AppendLine($"*Report generated on {DateTime.Now:yyyy-MM-dd HH:mm:ss}*");

        return sb.ToString();
    }

    private void AddMarkdownComparisonRow(StringBuilder sb, string component, int oracleCount, int postgresCount)
    {
        var diff = postgresCount - oracleCount;
        var diffDisplay = diff == 0 ? "\u2705 0" : $"\u274C {diff:+#;-#;0}";
        
        sb.AppendLine($"| {component} | {oracleCount} | {postgresCount} | {diffDisplay} |");
    }

    private void GenerateIssuesMarkdownTable(StringBuilder sb, List<string> issues)
    {
        sb.AppendLine("| Severity | Issue Description |");
        sb.AppendLine("|----------|-------------------|");
        
        foreach (var issue in issues)
        {
            var severity = GetIssueSeverity(issue);
            var icon = severity switch
            {
                "Critical" => "🔴",
                "Warning" => "⚠️",
                "Info" => "ℹ️",
                _ => ""
            };
            
            sb.AppendLine($"| {icon} **{severity}** | {EscapeMarkdown(issue)} |");
        }
    }

    private void GenerateCodeObjectIssuesMarkdownTable(StringBuilder sb, List<string> issues)
    {
        sb.AppendLine("| Severity | Object Type | Oracle | PostgreSQL | Missing/Extra Objects |");
        sb.AppendLine("|----------|-------------|--------|------------|-----------------------|");
        
        foreach (var issue in issues)
        {
            var severity = GetIssueSeverity(issue);
            var icon = severity switch
            {
                "Critical" => "🔴",
                "Warning" => "⚠️",
                "Info" => "ℹ️",
                _ => ""
            };
            
            // Parse the issue message
            var objectType = "";
            var oracleCount = "";
            var postgresCount = "";
            var details = "";
            
            if (issue.Contains("Sequence count mismatch"))
            {
                objectType = "Sequences";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("View count mismatch"))
            {
                objectType = "Views";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("Materialized view count mismatch"))
            {
                objectType = "Materialized Views";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("Trigger count mismatch"))
            {
                objectType = "Triggers";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("Procedure count mismatch"))
            {
                objectType = "Procedures";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("Functions (includes triggers) count mismatch"))
            {
                objectType = "Functions (includes triggers)";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else if (issue.Contains("Function count mismatch"))
            {
                objectType = "Functions";
                ExtractCountsAndDetails(issue, ref oracleCount, ref postgresCount, ref details);
            }
            else
            {
                // Fallback for other issues
                sb.AppendLine($"| {icon} **{severity}** | (Other) | - | - | {EscapeMarkdown(issue)} |");
                continue;
            }
            
            sb.AppendLine($"| {icon} **{severity}** | {objectType} | {oracleCount} | {postgresCount} | {EscapeMarkdown(details)} |");
        }
    }

    private void ExtractCountsAndDetails(string issue, ref string oracleCount, ref string postgresCount, ref string details)
    {
        // Extract Oracle count
        var oracleMatch = System.Text.RegularExpressions.Regex.Match(issue, @"Oracle=(\d+)");
        if (oracleMatch.Success)
        {
            oracleCount = oracleMatch.Groups[1].Value;
        }
        
        // Extract PostgreSQL count
        var postgresMatch = System.Text.RegularExpressions.Regex.Match(issue, @"PostgreSQL=(\d+)");
        if (postgresMatch.Success)
        {
            postgresCount = postgresMatch.Groups[1].Value;
        }
        
        // Extract details after the pipe
        var pipeIndex = issue.IndexOf('|');
        if (pipeIndex >= 0)
        {
            details = issue.Substring(pipeIndex + 1).Trim();
            // Remove the icon if present in details
            details = details.Replace("ℹ️", "").Replace("ℹ", "").Trim();
        }
        else
        {
            details = "-";
        }
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
