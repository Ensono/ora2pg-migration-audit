using Ora2Pg.Common.Config;
using Ora2PgDataTypeValidator.Models;
using Serilog;

namespace Ora2PgDataTypeValidator.src.Writers;

public class MultiSchemaSummaryWriter
{
    public void WriteSummaryReport(
        List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults,
        string reportsDir,
        string timestamp)
    {
        if (allResults == null || allResults.Count == 0)
        {
            Log.Warning("No results to write in multi-schema summary report");
            return;
        }
        
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "").ToLower();
        var dbPrefix = !string.IsNullOrWhiteSpace(dbName) ? $"{dbName}-" : "";
        
        var summaryPath = Path.Combine(reportsDir, $"{dbPrefix}summary-datatype-validation-{timestamp}.md");
        
        Directory.CreateDirectory(reportsDir);
        
        using var writer = new StreamWriter(summaryPath);
        
        WriteHeader(writer, allResults, timestamp);
        WriteOverallStatus(writer, allResults);
        WriteSchemaBreakdown(writer, allResults);
        WriteTopIssues(writer, allResults);
        WriteQuickLinks(writer, allResults, timestamp);
        
        Log.Information("📝 Multi-schema summary report generated: {Path}", summaryPath);
    }

    private void WriteHeader(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults, string timestamp)
    {
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "");
        
        writer.WriteLine("# Multi-Schema Data Type Validation Summary");
        writer.WriteLine();
        writer.WriteLine($"**Database:** {dbName}");
        writer.WriteLine($"**Validation Date:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        writer.WriteLine($"**Total Schema Pairs:** {allResults.Count}");
        writer.WriteLine();
        
        writer.WriteLine("## Schemas Tested");
        writer.WriteLine();
        foreach (var (oracleSchema, postgresSchema, _) in allResults)
        {
            writer.WriteLine($"- Oracle: `{oracleSchema}` → PostgreSQL: `{postgresSchema}`");
        }
        writer.WriteLine();
    }

    private void WriteOverallStatus(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults)
    {
        var schemasWithErrors = allResults.Count(r => r.Result.HasErrors || r.Result.HasCriticalIssues);
        var schemasWithWarnings = allResults.Count(r => r.Result.Warnings > 0 && !r.Result.HasErrors && !r.Result.HasCriticalIssues);
        var schemasClean = allResults.Count(r => r.Result.Warnings == 0 && !r.Result.HasErrors && !r.Result.HasCriticalIssues);
        
        var totalColumns = allResults.Sum(r => r.Result.TotalColumnsValidated);
        var totalCritical = allResults.Sum(r => r.Result.CriticalIssues);
        var totalErrors = allResults.Sum(r => r.Result.Errors);
        var totalWarnings = allResults.Sum(r => r.Result.Warnings);
        var totalInfo = allResults.Sum(r => r.Result.InfoMessages);
        
        writer.WriteLine("## Overall Status");
        writer.WriteLine();
        writer.WriteLine($"| Metric | Count |");
        writer.WriteLine($"|--------|-------|");
        writer.WriteLine($"| ❌ Schemas with Errors | {schemasWithErrors} |");
        writer.WriteLine($"| ⚠️ Schemas with Warnings | {schemasWithWarnings} |");
        writer.WriteLine($"| ✅ Clean Schemas | {schemasClean} |");
        writer.WriteLine($"| **Total Columns Validated** | **{totalColumns:N0}** |");
        writer.WriteLine($"| Critical Issues | {totalCritical:N0} |");
        writer.WriteLine($"| Errors | {totalErrors:N0} |");
        writer.WriteLine($"| Warnings | {totalWarnings:N0} |");
        writer.WriteLine($"| Info Messages | {totalInfo:N0} |");
        writer.WriteLine();
    }

    private void WriteSchemaBreakdown(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults)
    {
        writer.WriteLine("## Schema-by-Schema Breakdown");
        writer.WriteLine();
        writer.WriteLine("| Oracle Schema | PostgreSQL Schema | Columns | Critical | Errors | Warnings | Info | Status |");
        writer.WriteLine("|---------------|-------------------|---------|----------|--------|----------|------|--------|");
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults)
        {
            var statusIcon = result.HasCriticalIssues || result.HasErrors ? "❌" : 
                           result.Warnings > 0 ? "⚠️" : "✅";
            
            writer.WriteLine($"| {oracleSchema} | {postgresSchema} | {result.TotalColumnsValidated:N0} | " +
                           $"{result.CriticalIssues} | {result.Errors} | {result.Warnings} | {result.InfoMessages} | {statusIcon} {result.OverallStatus} |");
        }
        writer.WriteLine();
    }

    private void WriteTopIssues(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults)
    {
        writer.WriteLine("## Top Data Type Mapping Issues (Across All Schemas)");
        writer.WriteLine();
        
        var issuesByType = new Dictionary<string, HashSet<string>>();
        var issueSeverity = new Dictionary<string, ValidationSeverity>();
        var issueMessage = new Dictionary<string, string>();
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults)
        {
            foreach (var issue in result.Issues)
            {
                var issueKey = $"{issue.OracleType} → {issue.PostgresType}";
                
                if (!issuesByType.ContainsKey(issueKey))
                {
                    issuesByType[issueKey] = new HashSet<string>();
                    issueSeverity[issueKey] = issue.Severity;
                    issueMessage[issueKey] = issue.Message;
                }
                
                issuesByType[issueKey].Add(oracleSchema);
            }
        }
        
        var topIssues = issuesByType
            .OrderByDescending(kv => kv.Value.Count)
            .ThenBy(kv => issueSeverity[kv.Key])
            .Take(10);
        
        if (topIssues.Any())
        {
            writer.WriteLine("| Type Mapping | Severity | Affected Schemas | Message |");
            writer.WriteLine("|--------------|----------|------------------|---------|");
            
            foreach (var (typeMapping, affectedSchemas) in topIssues)
            {
                var severity = issueSeverity[typeMapping];
                var severityIcon = severity == ValidationSeverity.Critical ? "🔴" :
                                 severity == ValidationSeverity.Error ? "❌" :
                                 severity == ValidationSeverity.Warning ? "⚠️" : "ℹ️";
                var message = issueMessage[typeMapping];
                var schemas = string.Join(", ", affectedSchemas.OrderBy(s => s));
                
                writer.WriteLine($"| `{typeMapping}` | {severityIcon} {severity} | {affectedSchemas.Count} ({schemas}) | {message} |");
            }
        }
        else
        {
            writer.WriteLine("No issues found - all data type mappings are valid! ✅");
        }
        
        writer.WriteLine();
    }

    private void WriteQuickLinks(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults, string timestamp)
    {
        writer.WriteLine("## Quick Links to Individual Reports");
        writer.WriteLine();
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults)
        {
            var statusIcon = result.HasCriticalIssues || result.HasErrors ? "❌" : 
                           result.Warnings > 0 ? "⚠️" : "✅";
            var reportFile = $"{oracleSchema.ToLower()}-datatype-validation-{timestamp}.md";
            
            writer.WriteLine($"- {statusIcon} [{oracleSchema} → {postgresSchema}]({reportFile})");
        }
        writer.WriteLine();
    }
}
