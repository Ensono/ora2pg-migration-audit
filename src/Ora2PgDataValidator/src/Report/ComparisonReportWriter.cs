using Serilog;
using Ora2PgDataValidator.Comparison;

namespace Ora2PgDataValidator.Report;

public class ComparisonReportWriter
{
    private const string ReportsDir = "./reports";
    private const int ReportLineWidth = 100;
    
    public string GenerateDetailedReport(List<ComparisonResult> results)
    {
        if (!Directory.Exists(ReportsDir))
        {
            Directory.CreateDirectory(ReportsDir);
        }

        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        string reportFilename = $"migration_comparison_report_{timestamp}.txt";
        string reportPath = Path.Combine(ReportsDir, reportFilename);

        using var writer = new StreamWriter(reportPath);
        
        WriteReportHeader(writer);
        WriteExecutiveSummary(writer, results);
        WriteDetailedResults(writer, results);
        WriteReportFooter(writer);

        return reportPath;
    }

    private void WriteReportHeader(StreamWriter writer)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("ORACLE TO POSTGRESQL MIGRATION VALIDATION REPORT");
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine();
        writer.WriteLine($"Report Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        writer.WriteLine();
    }

    private void WriteExecutiveSummary(StreamWriter writer, List<ComparisonResult> results)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("EXECUTIVE SUMMARY");
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine();

        int totalTables = results.Count;
        int successfulTables = 0;
        int failedTables = 0;
        int errorTables = 0;
        long totalSourceRows = 0;
        long totalTargetRows = 0;
        long totalMatchingRows = 0;

        foreach (var result in results)
        {
            if (result.HasError)
            {
                errorTables++;
            }
            else if (result.IsMatch)
            {
                successfulTables++;
            }
            else
            {
                failedTables++;
            }

            totalSourceRows += result.SourceRowCount;
            totalTargetRows += result.TargetRowCount;
            totalMatchingRows += result.MatchingRows;
        }

        writer.WriteLine($"Total Tables Compared:           {totalTables}");
        writer.WriteLine($"  ✓ Successfully Validated:      {successfulTables}");
        writer.WriteLine($"  ✗ Validation Failed:           {failedTables}");
        writer.WriteLine($"  ⚠ Errors During Comparison:    {errorTables}");
        writer.WriteLine();
        writer.WriteLine($"Total Rows in Oracle:            {totalSourceRows:N0}");
        writer.WriteLine($"Total Rows in PostgreSQL:        {totalTargetRows:N0}");
        writer.WriteLine($"Total Matching Rows:             {totalMatchingRows:N0}");

        if (totalSourceRows > 0)
        {
            double matchPercentage = (double)totalMatchingRows / totalSourceRows * 100.0;
            writer.WriteLine($"Overall Match Percentage:        {matchPercentage:F2}%");
        }

        writer.WriteLine();
    }

    private void WriteDetailedResults(StreamWriter writer, List<ComparisonResult> results)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("DETAILED RESULTS BY TABLE");
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine();

        int tableNumber = 1;
        foreach (var result in results)
        {
            writer.WriteLine(new string('-', ReportLineWidth));
            writer.WriteLine($"Table {tableNumber}: {result.SourceTable} → {result.TargetTable}");
            writer.WriteLine(new string('-', ReportLineWidth));

            if (result.HasError)
            {
                writer.WriteLine($"❌ ERROR: {result.Error}");
            }
            else
            {
                writer.WriteLine($"Oracle Rows:        {result.SourceRowCount:N0}");
                writer.WriteLine($"PostgreSQL Rows:    {result.TargetRowCount:N0}");
                writer.WriteLine($"Matching Rows:      {result.MatchingRows:N0}");
                writer.WriteLine($"Match Percentage:   {result.MatchPercentage:F2}%");
                writer.WriteLine();

                if (result.IsMatch)
                {
                    writer.WriteLine("✓ VALIDATION: PASSED");
                }
                else
                {
                    writer.WriteLine("✗ VALIDATION: FAILED");
                    writer.WriteLine();
                    writer.WriteLine($"  Missing in PostgreSQL:  {result.MissingInTarget:N0} rows");
                    writer.WriteLine($"  Extra in PostgreSQL:    {result.ExtraInTarget:N0} rows");
                    writer.WriteLine($"  Mismatched rows:        {result.MismatchedRows:N0} rows");

                    if (result.MismatchedRowDetails.Count > 0)
                    {
                        writer.WriteLine();
                        writer.WriteLine("  Sample Mismatched Rows (first 5):");
                        int count = 0;
                        foreach (var mismatch in result.MismatchedRowDetails.Take(5))
                        {
                            writer.WriteLine($"    Row {mismatch.Key}:");

                            if (result.MismatchedRowPrimaryKeys.TryGetValue(mismatch.Key, out var pkPair))
                            {
                                var oraclePk = string.Join(", ", pkPair.OraclePrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
                                var postgresPk = string.Join(", ", pkPair.PostgresPrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
                                writer.WriteLine($"      Oracle PK:     [{oraclePk}]");
                                writer.WriteLine($"      PostgreSQL PK: [{postgresPk}]");
                            }
                            
                            writer.WriteLine($"      Hash Mismatch: {mismatch.Value}");
                            count++;
                        }
                        if (result.MismatchedRowDetails.Count > 5)
                        {
                            writer.WriteLine($"    ... and {result.MismatchedRowDetails.Count - 5} more");
                        }
                    }

                    if (result.MissingInTarget > 0 && result.MissingRows.Count > 0)
                    {
                        writer.WriteLine();
                        writer.WriteLine("  Sample Missing Rows in PostgreSQL (first 5):");
                        foreach (var missing in result.MissingRows.Take(5))
                        {
                            writer.Write($"    Row {missing.Key}");

                            if (result.MissingRowPrimaryKeys.TryGetValue(missing.Key, out var pkValues))
                            {
                                var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                                writer.Write($" [PK: {pk}]");
                            }
                            
                            writer.WriteLine($" - Hash: {missing.Value}");
                        }
                        if (result.MissingRows.Count > 5)
                        {
                            writer.WriteLine($"    ... and {result.MissingRows.Count - 5} more");
                        }
                    }

                    if (result.ExtraInTarget > 0 && result.ExtraRows.Count > 0)
                    {
                        writer.WriteLine();
                        writer.WriteLine("  Sample Extra Rows in PostgreSQL (first 5):");
                        foreach (var extra in result.ExtraRows.Take(5))
                        {
                            writer.Write($"    Row {extra.Key}");

                            if (result.ExtraRowPrimaryKeys.TryGetValue(extra.Key, out var pkValues))
                            {
                                var pk = string.Join(", ", pkValues.Select(kv => $"{kv.Key}={kv.Value}"));
                                writer.Write($" [PK: {pk}]");
                            }
                            
                            writer.WriteLine($" - Hash: {extra.Value}");
                        }
                        if (result.ExtraRows.Count > 5)
                        {
                            writer.WriteLine($"    ... and {result.ExtraRows.Count - 5} more");
                        }
                    }
                }
            }

            writer.WriteLine();
            tableNumber++;
        }
    }

    private void WriteReportFooter(StreamWriter writer)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("END OF REPORT");
        writer.WriteLine(new string('=', ReportLineWidth));
    }
}
