using Ora2Pg.Common.Config;
using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.Writers;

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
        
        var summaryPath = Path.Combine(reportsDir, $"{dbPrefix}summary-rowcount-validation-{timestamp}.md");
        
        Directory.CreateDirectory(reportsDir);
        
        using var writer = new StreamWriter(summaryPath);
        
        WriteHeader(writer, allResults, timestamp);
        WriteOverallStatus(writer, allResults);
        WriteSchemaBreakdown(writer, allResults);
        WriteTopMismatches(writer, allResults);
        WriteQuickLinks(writer, allResults, timestamp);
        
        Log.Information("📝 Multi-schema summary report generated: {Path}", summaryPath);
    }

    private void WriteHeader(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults, string timestamp)
    {
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "");
        
        writer.WriteLine("# Multi-Schema Row Count Validation Summary");
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
        
        var totalTables = allResults.Sum(r => r.Result.TotalTablesValidated);
        var totalMatching = allResults.Sum(r => r.Result.TablesWithMatchingCounts);
        var totalMismatched = allResults.Sum(r => r.Result.TablesWithMismatchedCounts);
        var totalOracleOnly = allResults.Sum(r => r.Result.TablesOnlyInOracle);
        var totalPostgresOnly = allResults.Sum(r => r.Result.TablesOnlyInPostgres);
        var totalOracleRows = allResults.Sum(r => r.Result.TotalOracleRows);
        var totalPostgresRows = allResults.Sum(r => r.Result.TotalPostgresRows);
        
        writer.WriteLine("## Overall Status");
        writer.WriteLine();
        writer.WriteLine($"| Metric | Count |");
        writer.WriteLine($"|--------|-------|");
        writer.WriteLine($"| ❌ Schemas with Errors | {schemasWithErrors} |");
        writer.WriteLine($"| ⚠️ Schemas with Warnings | {schemasWithWarnings} |");
        writer.WriteLine($"| ✅ Clean Schemas | {schemasClean} |");
        writer.WriteLine($"| **Total Tables Validated** | **{totalTables:N0}** |");
        writer.WriteLine($"| ✅ Tables with Matching Counts | {totalMatching:N0} |");
        writer.WriteLine($"| ❌ Tables with Mismatched Counts | {totalMismatched:N0} |");
        writer.WriteLine($"| ⚠️ Tables Only in Oracle | {totalOracleOnly:N0} |");
        writer.WriteLine($"| ⚠️ Tables Only in PostgreSQL | {totalPostgresOnly:N0} |");
        writer.WriteLine($"| **Total Oracle Rows** | **{totalOracleRows:N0}** |");
        writer.WriteLine($"| **Total PostgreSQL Rows** | **{totalPostgresRows:N0}** |");
        var rowDiff = totalOracleRows - totalPostgresRows;
        writer.WriteLine($"| **Row Difference** | **{rowDiff:N0}** |");
        writer.WriteLine();

        // Aggregate severity counts across all schemas
        var totalCritical = allResults.Sum(r => r.Result.CriticalIssues);
        var totalErrors = allResults.Sum(r => r.Result.Errors);
        var totalWarnings = allResults.Sum(r => r.Result.Warnings);
        var totalInfo = allResults.Sum(r => r.Result.InfoMessages);

        writer.WriteLine("### Severity Breakdown (All Schemas)");
        writer.WriteLine();
        writer.WriteLine("| Severity | Count | Description |");
        writer.WriteLine("|----------|-------|-------------|");
        writer.WriteLine($"| ❌ Critical Issues | {totalCritical} | >10% row count difference |");
        writer.WriteLine($"| 🔴 Errors | {totalErrors} | 1-10% row count difference |");
        writer.WriteLine($"| ⚠️ Warnings | {totalWarnings} | <1% row count difference |");
        writer.WriteLine($"| ℹ️ Info | {totalInfo} | Matching counts |");
        writer.WriteLine();
    }

    private void WriteSchemaBreakdown(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults)
    {
        writer.WriteLine("## Schema-by-Schema Breakdown");
        writer.WriteLine();
        writer.WriteLine("| Oracle Schema | PostgreSQL Schema | Tables | Matches | Mismatches | Oracle Rows | PostgreSQL Rows | Status |");
        writer.WriteLine("|---------------|-------------------|--------|---------|------------|-------------|-----------------|--------|");
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults)
        {
            var statusIcon = result.HasCriticalIssues || result.HasErrors ? "❌" : 
                           result.Warnings > 0 ? "⚠️" : "✅";
            
            writer.WriteLine($"| {oracleSchema} | {postgresSchema} | {result.TotalTablesValidated:N0} | " +
                           $"{result.TablesWithMatchingCounts:N0} | {result.TablesWithMismatchedCounts:N0} | " +
                           $"{result.TotalOracleRows:N0} | {result.TotalPostgresRows:N0} | " +
                           $"{statusIcon} {result.OverallStatus} |");
        }
        writer.WriteLine();
    }

    private void WriteTopMismatches(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ValidationResult Result)> allResults)
    {
        writer.WriteLine("## Top Row Count Mismatches (Across All Schemas)");
        writer.WriteLine();
        
        var allMismatches = new List<(string Schema, string Table, long OracleRows, long PostgresRows, long Difference)>();
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults)
        {
            foreach (var issue in result.Issues)
            {
                if ((issue.Severity == ValidationSeverity.Error || issue.Severity == ValidationSeverity.Critical) 
                    && issue.OracleRowCount.HasValue && issue.PostgresRowCount.HasValue)
                {
                    var diff = Math.Abs(issue.OracleRowCount.Value - issue.PostgresRowCount.Value);
                    allMismatches.Add((oracleSchema, issue.TableName, issue.OracleRowCount.Value, issue.PostgresRowCount.Value, diff));
                }
            }
        }
        
        var topMismatches = allMismatches
            .OrderByDescending(m => m.Difference)
            .Take(10);
        
        if (topMismatches.Any())
        {
            writer.WriteLine("| Schema | Table | Oracle Rows | PostgreSQL Rows | Difference | % Diff |");
            writer.WriteLine("|--------|-------|-------------|-----------------|------------|--------|");
            
            foreach (var (schema, table, oracleRows, postgresRows, diff) in topMismatches)
            {
                var percentDiff = oracleRows > 0 ? (double)diff / oracleRows * 100 : 0;
                writer.WriteLine($"| {schema} | {table} | {oracleRows:N0} | {postgresRows:N0} | {diff:N0} | {percentDiff:F2}% |");
            }
        }
        else
        {
            writer.WriteLine("No row count mismatches found - all tables have matching row counts! ✅");
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
            var reportFile = $"{oracleSchema.ToLower()}-rowcount-validation-{timestamp}.md";
            
            writer.WriteLine($"- {statusIcon} [{oracleSchema} → {postgresSchema}]({reportFile})");
        }
        writer.WriteLine();
    }
}
