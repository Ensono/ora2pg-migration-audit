using Ora2Pg.Common.Config;
using Ora2PgDataValidator.Comparison;
using Serilog;

namespace Ora2PgDataValidator.Writers;

public class DataValidatorSummary
{
    public string OracleSchema { get; set; } = "";
    public string PostgresSchema { get; set; } = "";
    public int TotalTables { get; set; }
    public int SuccessfulValidations { get; set; }
    public int FailedValidations { get; set; }
    public List<ComparisonResult> Results { get; set; } = new();
    
    public bool HasErrors => FailedValidations > 0;
    public string OverallStatus => HasErrors ? "FAILED" : "PASSED";
}

public class MultiSchemaSummaryWriter
{
    public void WriteSummaryReport(
        List<DataValidatorSummary> allResults,
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
        
        var summaryPath = Path.Combine(reportsDir, $"{dbPrefix}summary-data-fingerprint-validation-{timestamp}.md");
        
        Directory.CreateDirectory(reportsDir);
        
        using var writer = new StreamWriter(summaryPath);
        
        WriteHeader(writer, allResults, timestamp);
        WriteOverallStatus(writer, allResults);
        WriteSchemaBreakdown(writer, allResults);
        WriteTopMismatches(writer, allResults);
        WriteQuickLinks(writer, allResults, timestamp);
        
        Log.Information("📝 Multi-schema summary report generated: {Path}", summaryPath);
    }

    private void WriteHeader(StreamWriter writer, List<DataValidatorSummary> allResults, string timestamp)
    {
        var props = ApplicationProperties.Instance;
        var dbName = props.Get("POSTGRES_DB", "");
        
        writer.WriteLine("# Multi-Schema Data Fingerprint Validation Summary");
        writer.WriteLine();
        writer.WriteLine($"**Database:** {dbName}");
        writer.WriteLine($"**Validation Date:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        writer.WriteLine($"**Total Schema Pairs:** {allResults.Count}");
        writer.WriteLine();
        
        writer.WriteLine("## Schemas Tested");
        writer.WriteLine();
        foreach (var result in allResults)
        {
            writer.WriteLine($"- Oracle: `{result.OracleSchema}` → PostgreSQL: `{result.PostgresSchema}`");
        }
        writer.WriteLine();
    }

    private void WriteOverallStatus(StreamWriter writer, List<DataValidatorSummary> allResults)
    {
        var schemasWithErrors = allResults.Count(r => r.HasErrors);
        var schemasClean = allResults.Count(r => !r.HasErrors);
        
        var totalTables = allResults.Sum(r => r.TotalTables);
        var totalSuccess = allResults.Sum(r => r.SuccessfulValidations);
        var totalFailed = allResults.Sum(r => r.FailedValidations);
        
        var totalMissingRows = allResults.Sum(r => r.Results.Sum(cr => cr.MissingInTarget));
        var totalExtraRows = allResults.Sum(r => r.Results.Sum(cr => cr.ExtraInTarget));
        var totalMismatchedRows = allResults.Sum(r => r.Results.Sum(cr => cr.MismatchedRows));
        
        writer.WriteLine("## Overall Status");
        writer.WriteLine();
        writer.WriteLine($"| Metric | Count |");
        writer.WriteLine($"|--------|-------|");
        writer.WriteLine($"| ❌ Schemas with Errors | {schemasWithErrors} |");
        writer.WriteLine($"| ✅ Clean Schemas | {schemasClean} |");
        writer.WriteLine($"| **Total Tables Validated** | **{totalTables:N0}** |");
        writer.WriteLine($"| ✅ Successful Validations | {totalSuccess:N0} |");
        writer.WriteLine($"| ❌ Failed Validations | {totalFailed:N0} |");
        writer.WriteLine($"| **Total Missing Rows** | **{totalMissingRows:N0}** |");
        writer.WriteLine($"| **Total Extra Rows** | **{totalExtraRows:N0}** |");
        writer.WriteLine($"| **Total Mismatched Rows** | **{totalMismatchedRows:N0}** |");
        writer.WriteLine();
    }

    private void WriteSchemaBreakdown(StreamWriter writer, List<DataValidatorSummary> allResults)
    {
        writer.WriteLine("## Schema-by-Schema Breakdown");
        writer.WriteLine();
        writer.WriteLine("| Oracle Schema | PostgreSQL Schema | Tables | Success | Failed | Missing Rows | Extra Rows | Mismatched Rows | Status |");
        writer.WriteLine("|---------------|-------------------|--------|---------|--------|--------------|------------|-----------------|--------|");
        
        foreach (var result in allResults)
        {
            var statusIcon = result.HasErrors ? "❌" : "✅";
            var missingRows = result.Results.Sum(cr => cr.MissingInTarget);
            var extraRows = result.Results.Sum(cr => cr.ExtraInTarget);
            var mismatchedRows = result.Results.Sum(cr => cr.MismatchedRows);
            
            writer.WriteLine($"| {result.OracleSchema} | {result.PostgresSchema} | {result.TotalTables:N0} | " +
                           $"{result.SuccessfulValidations:N0} | {result.FailedValidations:N0} | " +
                           $"{missingRows:N0} | {extraRows:N0} | {mismatchedRows:N0} | " +
                           $"{statusIcon} {result.OverallStatus} |");
        }
        writer.WriteLine();
    }

    private void WriteTopMismatches(StreamWriter writer, List<DataValidatorSummary> allResults)
    {
        writer.WriteLine("## Top Tables with Data Differences (Across All Schemas)");
        writer.WriteLine();
        
        var allFailures = new List<(string Schema, ComparisonResult Result)>();
        
        foreach (var summary in allResults)
        {
            foreach (var result in summary.Results.Where(r => !r.IsMatch && string.IsNullOrEmpty(r.Error)))
            {
                allFailures.Add((summary.OracleSchema, result));
            }
        }
        
        var topFailures = allFailures
            .OrderByDescending(f => f.Result.MissingInTarget + f.Result.ExtraInTarget + f.Result.MismatchedRows)
            .Take(10);
        
        if (topFailures.Any())
        {
            writer.WriteLine("| Schema | Table | Missing in PG | Extra in PG | Mismatched | Total Rows (Oracle) |");
            writer.WriteLine("|--------|-------|---------------|-------------|------------|---------------------|");
            
            foreach (var (schema, result) in topFailures)
            {
                writer.WriteLine($"| {schema} | {result.SourceTable} | {result.MissingInTarget:N0} | " +
                               $"{result.ExtraInTarget:N0} | {result.MismatchedRows:N0} | {result.SourceRowCount:N0} |");
            }
        }
        else
        {
            writer.WriteLine("No data differences found - all tables have matching data! ✅");
        }
        
        writer.WriteLine();
    }

    private void WriteQuickLinks(StreamWriter writer, List<DataValidatorSummary> allResults, string timestamp)
    {
        writer.WriteLine("## Quick Links to Individual Reports");
        writer.WriteLine();
        
        foreach (var result in allResults)
        {
            var statusIcon = result.HasErrors ? "❌" : "✅";
            var reportFile = $"{result.OracleSchema.ToLower()}-data-fingerprint-validation-{timestamp}.md";
            
            writer.WriteLine($"- {statusIcon} [{result.OracleSchema} → {result.PostgresSchema}]({reportFile})");
        }
        writer.WriteLine();
    }
}
