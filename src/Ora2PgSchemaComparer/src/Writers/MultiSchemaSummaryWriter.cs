using Ora2PgSchemaComparer.Comparison;
using Ora2Pg.Common.Config;
using Serilog;

namespace Ora2PgSchemaComparer.src.Writers;

public class MultiSchemaSummaryWriter
{
    public void WriteSummaryReport(
        List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults,
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
        
        var summaryPath = Path.Combine(reportsDir, $"{dbPrefix}summary-schema-comparison-{timestamp}.md");
        
        using var writer = new StreamWriter(summaryPath);
        
        WriteHeader(writer, allResults);
        WriteOverallStatus(writer, allResults);
        WriteSchemaBreakdown(writer, allResults);
        WriteTopIssues(writer, allResults);
        WriteQuickLinks(writer, allResults, timestamp);
        
        Log.Information("📊 Multi-schema summary report: {Path}", summaryPath);
    }

    private void WriteHeader(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults)
    {
        writer.WriteLine("# Multi-Schema Validation Summary");
        writer.WriteLine();
        writer.WriteLine($"**Report Generated:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        writer.WriteLine($"**Schemas Tested:** {allResults.Count}");
        writer.WriteLine($"**Validator:** Schema Comparer");
        writer.WriteLine();
    }

    private void WriteOverallStatus(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults)
    {
        writer.WriteLine("## Overall Status");
        writer.WriteLine();
        
        var gradeA = allResults.Count(r => r.Result.OverallGrade.StartsWith("A"));
        var gradeB = allResults.Count(r => r.Result.OverallGrade.StartsWith("B"));
        var gradeC = allResults.Count(r => r.Result.OverallGrade == "C");
        var gradeD = allResults.Count(r => r.Result.OverallGrade == "D");
        var gradeF = allResults.Count(r => r.Result.OverallGrade == "F");
        
        if (gradeF > 0)
        {
            writer.WriteLine($"❌ **{gradeF}** schema(s) with critical issues (Grade F)");
        }
        if (gradeD > 0)
        {
            writer.WriteLine($"⚠️ **{gradeD}** schema(s) need attention (Grade D)");
        }
        if (gradeC > 0)
        {
            writer.WriteLine($"⚠️ **{gradeC}** schema(s) have moderate issues (Grade C)");
        }
        if (gradeB > 0)
        {
            writer.WriteLine($"✅ **{gradeB}** schema(s) in good shape (Grade B)");
        }
        if (gradeA > 0)
        {
            writer.WriteLine($"✅ **{gradeA}** schema(s) excellent (Grade A)");
        }
        
        writer.WriteLine();
        
        // Totals
        var totalTables = allResults.Sum(r => r.Result.OracleLogicalTableCount);
        var totalColumns = allResults.Sum(r => r.Result.OracleLogicalColumnCount);
        var totalIssues = allResults.Sum(r => r.Result.TotalIssues);
        
        writer.WriteLine($"- **Total Tables Compared:** {totalTables:N0}");
        writer.WriteLine($"- **Total Columns Compared:** {totalColumns:N0}");
        writer.WriteLine($"- **Total Issues Found:** {totalIssues:N0}");
        writer.WriteLine();
    }

    private void WriteSchemaBreakdown(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults)
    {
        writer.WriteLine("## Schema Breakdown");
        writer.WriteLine();
        writer.WriteLine("| Schema | Grade | Tables | Columns | Issues | Status |");
        writer.WriteLine("|--------|-------|--------|---------|--------|--------|");
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults.OrderBy(r => r.OracleSchema))
        {
            var grade = result.OverallGrade;
            var gradeIcon = grade.StartsWith("A") ? "✅" :
                           grade.StartsWith("B") ? "✅" :
                           grade == "C" ? "⚠️" :
                           grade == "D" ? "⚠️" :
                           grade == "F" ? "❌" : "❓";
            
            var tables = result.OracleLogicalTableCount;
            var columns = result.OracleLogicalColumnCount;
            var issues = result.TotalIssues;
            
            var status = grade.StartsWith("A") ? "Excellent" :
                        grade.StartsWith("B") ? "Good" :
                        grade == "C" ? "Moderate Issues" :
                        grade == "D" ? "Needs Attention" :
                        grade == "F" ? "Critical Issues" : "Unknown";
            
            writer.WriteLine($"| {oracleSchema} → {postgresSchema.ToLower()} | {gradeIcon} {grade} | {tables} | {columns} | {issues} | {status} |");
        }
        
        writer.WriteLine();
    }

    private void WriteTopIssues(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults)
    {
        writer.WriteLine("## Top Issues Across All Schemas");
        writer.WriteLine();
        
        var issuesByMessage = new Dictionary<string, List<string>>();
        
        foreach (var (oracleSchema, _, result) in allResults)
        {
            var allIssues = new List<string>();
            allIssues.AddRange(result.TableIssues);
            allIssues.AddRange(result.ConstraintIssues);
            allIssues.AddRange(result.IndexIssues);
            allIssues.AddRange(result.CodeObjectIssues);
            
            foreach (var issue in allIssues)
            {
                var normalizedIssue = issue.Replace("❌", "").Replace("⚠️", "").Replace("ℹ", "").Trim();
                
                if (!issuesByMessage.ContainsKey(normalizedIssue))
                {
                    issuesByMessage[normalizedIssue] = new List<string>();
                }
                if (!issuesByMessage[normalizedIssue].Contains(oracleSchema))
                {
                    issuesByMessage[normalizedIssue].Add(oracleSchema);
                }
            }
        }
        
        var topIssues = issuesByMessage
            .OrderByDescending(kv => kv.Value.Count)
            .ThenBy(kv => kv.Key)
            .Take(10)
            .ToList();
        
        if (topIssues.Any())
        {
            var counter = 1;
            foreach (var (issue, schemas) in topIssues)
            {
                var severity = issue.Contains("count mismatch") || issue.Contains("missing") ? "⚠️" : "ℹ️";
                writer.WriteLine($"{counter}. {severity} **{issue}**");
                writer.WriteLine($"   - Affects {schemas.Count}/{allResults.Count} schema(s): {string.Join(", ", schemas.OrderBy(s => s))}");
                writer.WriteLine();
                counter++;
            }
        }
        else
        {
            writer.WriteLine("✅ No issues found across any schemas!");
            writer.WriteLine();
        }
    }

    private void WriteQuickLinks(StreamWriter writer, List<(string OracleSchema, string PostgresSchema, ComparisonResult Result)> allResults, string timestamp)
    {
        writer.WriteLine("## Detailed Reports");
        writer.WriteLine();
        writer.WriteLine("For detailed analysis of each schema, see the individual reports:");
        writer.WriteLine();
        
        foreach (var (oracleSchema, postgresSchema, result) in allResults.OrderBy(r => r.OracleSchema))
        {
            var schemaLower = oracleSchema.ToLower();
            var grade = result.OverallGrade;
            var gradeIcon = grade.StartsWith("A") ? "✅" :
                           grade.StartsWith("B") ? "✅" :
                           grade == "C" ? "⚠️" :
                           grade == "D" ? "⚠️" :
                           grade == "F" ? "❌" : "❓";
            
            writer.WriteLine($"### {gradeIcon} {oracleSchema} → {postgresSchema.ToLower()} (Grade {grade})");
            writer.WriteLine();
            writer.WriteLine($"- [Markdown Report]({schemaLower}-schema-comparison-{timestamp}.md)");
            writer.WriteLine($"- [Text Report]({schemaLower}-schema-comparison-{timestamp}.txt)");
            writer.WriteLine($"- [HTML Report]({schemaLower}-schema-comparison-{timestamp}.html)");
            writer.WriteLine();
        }
    }
}
