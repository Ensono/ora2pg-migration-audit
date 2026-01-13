using System.Text;
using Ora2PgDataValidator.Comparison;

namespace Ora2PgDataValidator.Report;

public class DataValidationMarkdownWriter
{
    public void WriteMarkdownReport(List<ComparisonResult> results, string outputPath)
    {
        var markdown = GenerateMarkdown(results);
        File.WriteAllText(outputPath, markdown);
    }

    private string GenerateMarkdown(List<ComparisonResult> results)
    {
        var sb = new StringBuilder();

        var totalTables = results.Count;
        var successfulMatches = results.Count(r => !r.HasError && r.IsMatch);
        var failedMatches = results.Count(r => !r.HasError && !r.IsMatch);
        var errors = results.Count(r => r.HasError);
        
        var totalSourceRows = results.Sum(r => (long)r.SourceRowCount);
        var totalTargetRows = results.Sum(r => (long)r.TargetRowCount);
        var totalMatchingRows = results.Sum(r => (long)r.MatchingRows);
        
        var overallMatchPercentage = totalSourceRows > 0 
            ? (double)totalMatchingRows / totalSourceRows * 100.0 
            : 0.0;

        var status = failedMatches == 0 && errors == 0 ? "PASSED" : "FAILED";
        var statusIcon = status == "PASSED" ? "✅" : "❌";

        sb.AppendLine("# Oracle to PostgreSQL Data Fingerprint Validation Report");
        sb.AppendLine();
        sb.AppendLine("## Metadata");
        sb.AppendLine();
        sb.AppendLine($"- **Validation Date:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"- **Total Tables Compared:** {totalTables}");
        sb.AppendLine($"- **Overall Match Percentage:** {overallMatchPercentage:F2}%");
        sb.AppendLine($"- **Status:** {statusIcon} {status}");
        sb.AppendLine();

        sb.AppendLine("## Executive Summary");
        sb.AppendLine();
        sb.AppendLine("| Metric | Value |");
        sb.AppendLine("|--------|-------|");
        sb.AppendLine($"| 📊 Total Tables | {totalTables} |");
        sb.AppendLine($"| ✅ Successful Matches | {successfulMatches} |");
        sb.AppendLine($"| ⚠️ Mismatches | {failedMatches} |");
        sb.AppendLine($"| 🔴 Errors | {errors} |");
        sb.AppendLine($"| 📈 Total Source Rows | {totalSourceRows:N0} |");
        sb.AppendLine($"| 📉 Total Target Rows | {totalTargetRows:N0} |");
        sb.AppendLine($"| ✔️ Matching Rows | {totalMatchingRows:N0} |");
        sb.AppendLine();

        sb.AppendLine("## Detailed Comparison Results");
        sb.AppendLine();
        sb.AppendLine("| Status | Table | Source Rows | Target Rows | Matching | Mismatched | Missing | Extra | Match % |");
        sb.AppendLine("|--------|-------|-------------|-------------|----------|------------|---------|-------|---------|");

        foreach (var result in results.OrderBy(r => r.IsMatch ? 0 : 1).ThenBy(r => r.SourceTable))
        {
            var tableStatusIcon = result.HasError ? "🔴" : result.IsMatch ? "✅" : "⚠️";

            var matchPercentage = result.MatchPercentage;

            var tableName = !string.IsNullOrEmpty(result.TargetTable) ? result.TargetTable : result.SourceTable;
            sb.AppendLine($"| {tableStatusIcon} | {EscapeMarkdown(tableName)} | {result.SourceRowCount:N0} | {result.TargetRowCount:N0} | {result.MatchingRows:N0} | {result.MismatchedRows:N0} | {result.MissingInTarget:N0} | {result.ExtraInTarget:N0} | {matchPercentage:F2}% |");
        }

        sb.AppendLine();

        var tablesWithMismatches = results.Where(r => !r.IsMatch && !r.HasError).ToList();
        if (tablesWithMismatches.Any())
        {
            sb.AppendLine("## ⚠️ Detailed Mismatch Information");
            sb.AppendLine();

            foreach (var result in tablesWithMismatches)
            {
                var tableName = !string.IsNullOrEmpty(result.TargetTable) ? result.TargetTable : result.SourceTable;
                sb.AppendLine($"### {tableName}");
                sb.AppendLine();
                sb.AppendLine($"- **Source Rows:** {result.SourceRowCount:N0}");
                sb.AppendLine($"- **Target Rows:** {result.TargetRowCount:N0}");
                sb.AppendLine($"- **Match Percentage:** {result.MatchPercentage:F2}%");
                sb.AppendLine();

                if (result.MismatchedRowDetails.Any())
                {
                    sb.AppendLine("**Mismatched Rows (showing first 5):**");
                    sb.AppendLine();
                    sb.AppendLine("| Row Index | Primary Key | Source Hash | Target Hash |");
                    sb.AppendLine("|-----------|-------------|-------------|-------------|");

                    var mismatchesToShow = result.MismatchedRowDetails.Take(5);
                    foreach (var mismatch in mismatchesToShow)
                    {
                        var sourceHash = mismatch.Value.OracleHash.Length > 8 ? mismatch.Value.OracleHash[..8] : mismatch.Value.OracleHash;
                        var targetHash = mismatch.Value.PostgresHash.Length > 8 ? mismatch.Value.PostgresHash[..8] : mismatch.Value.PostgresHash;

                        string pkDisplay = "N/A";
                        if (result.MismatchedRowPrimaryKeys.TryGetValue(mismatch.Key, out var pkPair))
                        {
                            var oraclePk = string.Join(", ", pkPair.OraclePrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
                            pkDisplay = oraclePk;
                        }
                        
                        sb.AppendLine($"| {mismatch.Key} | `{EscapeMarkdown(pkDisplay)}` | `{sourceHash}...` | `{targetHash}...` |");
                    }

                    if (result.MismatchedRowDetails.Count > 5)
                    {
                        sb.AppendLine();
                        sb.AppendLine($"*... and {result.MismatchedRowDetails.Count - 5} more mismatched rows*");
                    }
                    sb.AppendLine();
                }

                if (result.MissingRows.Any())
                {
                    sb.AppendLine($"**Missing Rows in PostgreSQL:** {result.MissingRows.Count}");
                    sb.AppendLine();
                    var missingToShow = result.MissingRows.Take(5);
                    foreach (var missing in missingToShow)
                    {
                        var hash = missing.Value.Length > 16 ? missing.Value[..16] : missing.Value;
                        

                        string pkDisplay = "";
                        if (result.MissingRowPrimaryKeys.TryGetValue(missing.Key, out var pkValues))
                        {
                            var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                            pkDisplay = $" [PK: {pk}]";
                        }
                        
                        sb.AppendLine($"- Row {missing.Key}{pkDisplay}: `{hash}...`");
                    }
                    if (result.MissingRows.Count > 5)
                    {
                        sb.AppendLine($"- *... and {result.MissingRows.Count - 5} more*");
                    }
                    sb.AppendLine();
                }

                if (result.ExtraRows.Any())
                {
                    sb.AppendLine($"**Extra Rows in PostgreSQL:** {result.ExtraRows.Count}");
                    sb.AppendLine();
                    var extraToShow = result.ExtraRows.Take(5);
                    foreach (var extra in extraToShow)
                    {
                        var hash = extra.Value.Length > 16 ? extra.Value[..16] : extra.Value;
                        

                        string pkDisplay = "";
                        if (result.ExtraRowPrimaryKeys.TryGetValue(extra.Key, out var pkValues))
                        {
                            var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                            pkDisplay = $" [PK: {pk}]";
                        }
                        
                        sb.AppendLine($"- Row {extra.Key}{pkDisplay}: `{hash}...`");
                    }
                    if (result.ExtraRows.Count > 5)
                    {
                        sb.AppendLine($"- *... and {result.ExtraRows.Count - 5} more*");
                    }
                    sb.AppendLine();
                }
            }
        }

        var tablesWithErrors = results.Where(r => r.HasError).ToList();
        if (tablesWithErrors.Any())
        {
            sb.AppendLine("## 🔴 Errors Encountered");
            sb.AppendLine();

            foreach (var result in tablesWithErrors)
            {
                var tableName = !string.IsNullOrEmpty(result.TargetTable) ? result.TargetTable : result.SourceTable;
                sb.AppendLine($"### {tableName}");
                sb.AppendLine();
                sb.AppendLine($"**Error Message:** {EscapeMarkdown(result.Error ?? "Unknown error")}");
                sb.AppendLine();
            }
        }

        if (status == "PASSED")
        {
            sb.AppendLine("## ✅ Validation Successful");
            sb.AppendLine();
            sb.AppendLine($"All {totalTables} tables have been successfully validated. Data fingerprints match between Oracle and PostgreSQL databases.");
            sb.AppendLine();
        }

        sb.AppendLine("---");
        sb.AppendLine();
        sb.AppendLine($"*Report generated on {DateTime.Now:yyyy-MM-dd HH:mm:ss}*");
        sb.AppendLine();
        sb.AppendLine("**Note:** This report uses SHA256 hash fingerprinting to compare row-by-row data integrity between source and target databases.");

        return sb.ToString();
    }

    private string EscapeMarkdown(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;


        return text.Replace("|", "\\|");
    }
}
