using Ora2PgDataValidator.Comparison;
using Ora2Pg.Common.Config;
using Ora2PgDataValidator.src;

namespace Ora2PgDataValidator.src.Writers;

public class ComparisonReportWriter
{
    private readonly string _reportsDir;
    private const int ReportLineWidth = 100;
    private string _oracleDatabase = string.Empty;
    private string _postgresDatabase = string.Empty;
    private string _oracleSchema = string.Empty;
    private string _postgresSchema = string.Empty;

    public ComparisonReportWriter()
    {
        var props = ApplicationProperties.Instance;
        _reportsDir = props.GetReportsDirectory("Ora2PgDataValidator");
    }
    
    public string GenerateDetailedReport(List<ComparisonResult> results, string schemaPrefix = "")
    {
        return GenerateDetailedReport(results, schemaPrefix, string.Empty, string.Empty, string.Empty, string.Empty);
    }
    
    public string GenerateDetailedReport(List<ComparisonResult> results, string schemaPrefix, string oracleDatabase, string postgresDatabase)
    {
        return GenerateDetailedReport(results, schemaPrefix, oracleDatabase, postgresDatabase, string.Empty, string.Empty);
    }

    public string GenerateDetailedReport(List<ComparisonResult> results, string schemaPrefix, string oracleDatabase, string postgresDatabase, string oracleSchema, string postgresSchema)
    {
        _oracleDatabase = oracleDatabase;
        _postgresDatabase = postgresDatabase;
        _oracleSchema = oracleSchema;
        _postgresSchema = postgresSchema;
        
        // Try to extract schema from first result if not provided
        if (string.IsNullOrEmpty(_oracleSchema) && results.Count > 0)
        {
            var parts = results[0].SourceTable.Split('.');
            if (parts.Length > 1) _oracleSchema = parts[0];
        }
        if (string.IsNullOrEmpty(_postgresSchema) && results.Count > 0)
        {
            var parts = results[0].TargetTable.Split('.');
            if (parts.Length > 1) _postgresSchema = parts[0];
        }
        
        if (!Directory.Exists(_reportsDir))
        {
            Directory.CreateDirectory(_reportsDir);
        }

        string timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss");
        string reportFilename = $"{schemaPrefix}data-fingerprint-validation-{timestamp}.txt";
        string reportPath = Path.Combine(_reportsDir, reportFilename);

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
        
        if (!string.IsNullOrEmpty(_oracleDatabase))
        {
            writer.WriteLine($"Oracle Database:     {_oracleDatabase}");
        }
        if (!string.IsNullOrEmpty(_postgresDatabase))
        {
            writer.WriteLine($"PostgreSQL Database: {_postgresDatabase}");
        }
        if (!string.IsNullOrEmpty(_oracleSchema))
        {
            writer.WriteLine($"Oracle Schema:       {_oracleSchema}");
        }
        if (!string.IsNullOrEmpty(_postgresSchema))
        {
            writer.WriteLine($"PostgreSQL Schema:   {_postgresSchema}");
        }
        
        writer.WriteLine();
    }

    private void WriteExecutiveSummary(StreamWriter writer, List<ComparisonResult> results)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("EXECUTIVE SUMMARY");
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine();

        int totalObjects = results.Count;
        int totalTables = results.Count(r => r.ObjectType == DatabaseObjectType.Table);
        int totalViews = results.Count(r => r.ObjectType == DatabaseObjectType.View);
        int successfulObjects = 0;
        int failedObjects = 0;
        int errorObjects = 0;
        long totalSourceRows = 0;
        long totalTargetRows = 0;
        long totalMatchingRows = 0;

        foreach (var result in results)
        {
            if (result.HasError)
            {
                errorObjects++;
            }
            else if (result.IsMatch)
            {
                successfulObjects++;
            }
            else
            {
                failedObjects++;
            }

            totalSourceRows += result.SourceRowCount;
            totalTargetRows += result.TargetRowCount;
            totalMatchingRows += result.MatchingRows;
        }

        writer.WriteLine($"Total Objects Compared:          {totalObjects} ({totalTables} tables, {totalViews} views)");
        writer.WriteLine($"  ✓ Successfully Validated:      {successfulObjects}");
        writer.WriteLine($"  ✗ Validation Failed:           {failedObjects}");
        writer.WriteLine($"  ⚠ Errors During Comparison:    {errorObjects}");
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
        writer.WriteLine("DETAILED RESULTS BY OBJECT");
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine();

        int objectNumber = 1;
        foreach (var result in results)
        {
            string objectType = result.ObjectType == DatabaseObjectType.View ? "View" : "Table";
            
            writer.WriteLine(new string('-', ReportLineWidth));
            writer.WriteLine($"{objectType} {objectNumber}: {result.SourceTable} → {result.TargetTable}");
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
            objectNumber++;
        }
    }

    private void WriteReportFooter(StreamWriter writer)
    {
        writer.WriteLine(new string('=', ReportLineWidth));
        writer.WriteLine("END OF REPORT");
        writer.WriteLine(new string('=', ReportLineWidth));
    }
}
