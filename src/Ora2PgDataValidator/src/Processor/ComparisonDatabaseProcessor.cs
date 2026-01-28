using System.Data;
using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataValidator.Extractor;
using Ora2PgDataValidator.Hasher;
using Ora2PgDataValidator.src.Writers;
using Ora2PgDataValidator.Comparison;

namespace Ora2PgDataValidator.Processor;

public class ComparisonDatabaseProcessor
{
    private readonly DatabaseConnectionManager _connectionManager;
    private readonly ComparisonReportWriter _reportWriter;
    private readonly CsvHashWriter _csvWriter;
    private readonly string _hashAlgorithm;
    private readonly int _batchSize;
    private readonly int _fetchSize;

    public ComparisonDatabaseProcessor(DatabaseConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
        _reportWriter = new ComparisonReportWriter();
        _csvWriter = new CsvHashWriter();

        var props = ApplicationProperties.Instance;
        _hashAlgorithm = props.Get("HASH_ALGORITHM", props.Get("hash.algorithm", "SHA256"));
        _batchSize = props.GetInt("BATCH_SIZE", props.GetInt("batch.size", 5000));
        _fetchSize = props.GetInt("FETCH_SIZE", props.GetInt("fetch.size", 1000));
    }


    public void ProcessAndCompareTables(Dictionary<string, string> tableMapping)
    {
        Log.Information("");
        Log.Information(new string('=', 80));
        Log.Information("DUAL DATABASE EXTRACTION AND COMPARISON");
        Log.Information(new string('=', 80));

        if (tableMapping == null || tableMapping.Count == 0)
        {
            Log.Error("✗ No tables specified for comparison");
            Log.Error("  Set TABLES_TO_COMPARE in .env file");
            Log.Error("  Format: ORACLE_SCHEMA.TABLE=postgres_schema.table");
            return;
        }

        Log.Information("Tables to compare: {Count}", tableMapping.Count);

        var allResults = new List<ComparisonResult>();
        int successCount = 0;
        int failCount = 0;

        foreach (var entry in tableMapping)
        {
            string oracleTable = entry.Key;
            string postgresTable = entry.Value;

            Log.Information("");
            Log.Information(new string('-', 80));
            Log.Information("Comparing: {OracleTable} (Oracle) ↔ {PostgresTable} (PostgreSQL)",
                           oracleTable, postgresTable);
            Log.Information(new string('-', 80));

            try
            {
                var result = ProcessTablePair(oracleTable, postgresTable);
                allResults.Add(result);

                if (result.IsMatch)
                {
                    successCount++;
                    Log.Information("✓ Tables match - migration validated successfully");
                }
                else
                {
                    failCount++;
                    Log.Warning("✗ Tables differ - migration validation failed");
                    Log.Warning("  Missing in PostgreSQL: {Count} rows", result.MissingInTarget);
                    Log.Warning("  Extra in PostgreSQL: {Count} rows", result.ExtraInTarget);
                    Log.Warning("  Mismatched rows: {Count}", result.MismatchedRows);
                }
            }
            catch (Exception ex)
            {
                failCount++;
                Log.Error(ex, "✗ Failed to compare tables: {OracleTable} ↔ {PostgresTable}",
                         oracleTable, postgresTable);
                var errorResult = new ComparisonResult(oracleTable, postgresTable)
                {
                    Error = ex.Message
                };
                allResults.Add(errorResult);
            }
        }


        Log.Information("");
        Log.Information(new string('=', 80));
        Log.Information("MIGRATION VALIDATION SUMMARY");
        Log.Information(new string('=', 80));
        Log.Information("Total tables compared: {Count}", tableMapping.Count);
        Log.Information("✓ Successful validations: {Count}", successCount);
        Log.Information("✗ Failed validations: {Count}", failCount);

        try
        {
            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var props = ApplicationProperties.Instance;
            var reportsDir = props.GetReportsDirectory("Ora2PgDataValidator");

            var markdownWriter = new DataValidationMarkdownWriter();
            var markdownReportPath = Path.Combine(reportsDir, $"data_fingerprint_validation_{timestamp}.md");
            markdownWriter.WriteMarkdownReport(allResults, markdownReportPath);
            Log.Information("📄 Markdown report saved to: {ReportPath}", markdownReportPath);

            string textReportPath = _reportWriter.GenerateDetailedReport(allResults);
            Log.Information("📄 Text report saved to: {ReportPath}", textReportPath);

            var htmlWriter = new DataValidationHtmlWriter();
            var htmlReportPath = Path.Combine(reportsDir, $"data_fingerprint_validation_{timestamp}.html");
            htmlWriter.WriteHtmlReport(allResults, htmlReportPath);
            Log.Information("📄 HTML report saved to: {ReportPath}", htmlReportPath);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "✗ Failed to generate comparison report");
        }

        Log.Information("");
        Log.Information(new string('=', 80));
        Log.Information("CSV hash files are available in the reports/ folder for manual review");
        Log.Information(new string('=', 80));
    }


    private ComparisonResult ProcessTablePair(string oracleTable, string postgresTable)
    {
        var result = new ComparisonResult(oracleTable, postgresTable);

        try
        {
            Log.Information("  Extracting Oracle data from {Table}...", oracleTable);
            var (oracleHashes, oracleRows, oracleMetadata) = ExtractAndHashTable(DatabaseType.Oracle, oracleTable);
            result.SourceRowCount = oracleHashes.Count;
            Log.Information("  ✓ Oracle: {Count} rows extracted", oracleHashes.Count);

            _csvWriter.WriteTableHashes(oracleTable, "Oracle", oracleHashes);

            Log.Information("  Extracting PostgreSQL data from {Table}...", postgresTable);
            var (postgresHashes, postgresRows, postgresMetadata) = ExtractAndHashTable(DatabaseType.PostgreSQL, postgresTable);
            result.TargetRowCount = postgresHashes.Count;
            Log.Information("  ✓ PostgreSQL: {Count} rows extracted", postgresHashes.Count);

            _csvWriter.WriteTableHashes(postgresTable, "PostgreSQL", postgresHashes);

            Log.Information("  Comparing hash values...");
            CompareHashes(oracleHashes, postgresHashes, oracleRows, postgresRows,
                         oracleMetadata, postgresMetadata, result);

            result.IsMatch = result.MismatchedRows == 0 &&
                           result.MissingInTarget == 0 &&
                           result.ExtraInTarget == 0;
        }
        catch (Exception ex)
        {
            result.Error = ex.Message;
            Log.Error(ex, "  ✗ Error processing table pair");
        }

        return result;
    }


    private (Dictionary<string, string> hashes,
             Dictionary<string, Dictionary<string, object?>> rowData,
             TableMetadata metadata) ExtractAndHashTable(DatabaseType dbType, string tableRef)
    {
        var hashes = new Dictionary<string, string>();
        var rowData = new Dictionary<string, Dictionary<string, object?>>();

        using var connection = _connectionManager.GetConnection(dbType);
        connection.Open();

        var extractor = new DataExtractor(connection, dbType);
        var metadata = extractor.GetTableMetadata(tableRef);

        int rowNumber = 0;
        extractor.ExtractTableDataInBatches(tableRef, _batchSize, batch =>
        {
            foreach (var row in batch)
            {
                rowNumber++;

                var rowDict = new Dictionary<string, object?>();
                for (int i = 0; i < metadata.Columns.Count && i < row.Length; i++)
                {
                    rowDict[metadata.Columns[i].Name] = row[i];
                }

                string hash = HashGenerator.GenerateHash(rowDict, _hashAlgorithm);
                hashes[rowNumber.ToString()] = hash;
                rowData[rowNumber.ToString()] = rowDict;
            }
        });

        return (hashes, rowData, metadata);
    }

    
    private void CompareHashes(Dictionary<string, string> oracleHashes,
                              Dictionary<string, string> postgresHashes,
                              Dictionary<string, Dictionary<string, object?>> oracleRows,
                              Dictionary<string, Dictionary<string, object?>> postgresRows,
                              TableMetadata oracleMetadata,
                              TableMetadata postgresMetadata,
                              ComparisonResult result)
    {
        int matching = 0;
        int mismatched = 0;
        int missing = 0;
        int extra = 0;

        foreach (var oracleEntry in oracleHashes)
        {
            int rowId = int.Parse(oracleEntry.Key);
            string oracleHash = oracleEntry.Value;

            if (postgresHashes.TryGetValue(oracleEntry.Key, out string? postgresHash))
            {
                if (oracleHash == postgresHash)
                {
                    matching++;
                }
                else
                {
                    mismatched++;

                    var oraclePkValues = ExtractPrimaryKeyValues(oracleRows[oracleEntry.Key], oracleMetadata.PrimaryKeyColumns);
                    var postgresPkValues = ExtractPrimaryKeyValues(postgresRows[oracleEntry.Key], postgresMetadata.PrimaryKeyColumns);
                    
                    result.AddMismatchedRow(rowId, oracleHash, postgresHash, oraclePkValues, postgresPkValues);
                }
            }
            else
            {
                missing++;

                var oraclePkValues = ExtractPrimaryKeyValues(oracleRows[oracleEntry.Key], oracleMetadata.PrimaryKeyColumns);
                result.AddMissingRow(rowId, oracleHash, oraclePkValues);
            }
        }

        foreach (var postgresEntry in postgresHashes)
        {
            if (!oracleHashes.ContainsKey(postgresEntry.Key))
            {
                extra++;
                int rowId = int.Parse(postgresEntry.Key);

                var postgresPkValues = ExtractPrimaryKeyValues(postgresRows[postgresEntry.Key], postgresMetadata.PrimaryKeyColumns);
                result.AddExtraRow(rowId, postgresEntry.Value, postgresPkValues);
            }
        }

        result.MatchingRows = matching;
        result.MismatchedRows = mismatched;
        result.MissingInTarget = missing;
        result.ExtraInTarget = extra;

        Log.Information("  Comparison: {Matching} matching, {Mismatched} mismatched, {Missing} missing, {Extra} extra",
                       matching, mismatched, missing, extra);
    }
    
    private Dictionary<string, object?> ExtractPrimaryKeyValues(Dictionary<string, object?> row, List<string> primaryKeyColumns)
    {
        var pkValues = new Dictionary<string, object?>();
        foreach (var pkColumn in primaryKeyColumns)
        {

            var matchingKey = row.Keys.FirstOrDefault(k => k.Equals(pkColumn, StringComparison.OrdinalIgnoreCase));
            if (matchingKey != null)
            {
                pkValues[pkColumn] = row[matchingKey];
            }
        }
        return pkValues;
    }
}
