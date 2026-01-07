using Serilog;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Connection;
using Ora2PgDataValidator.Extractor;
using Ora2PgDataValidator.Hasher;
using Ora2PgDataValidator.Report;

namespace Ora2PgDataValidator.Processor;


public class SingleDatabaseProcessor
{
    private readonly DatabaseConnectionManager _connectionManager;
    private readonly CsvHashWriter _csvWriter;
    private readonly string _hashAlgorithm;
    private readonly int _batchSize;

    public SingleDatabaseProcessor(DatabaseConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
        _csvWriter = new CsvHashWriter();

        var props = ApplicationProperties.Instance;
        _hashAlgorithm = props.Get("HASH_ALGORITHM", props.Get("hash.algorithm", "SHA256"));
        _batchSize = props.GetInt("BATCH_SIZE", props.GetInt("batch.size", 5000));
    }
    
    public void ProcessTables(DatabaseType dbType, List<string> tables)
    {
        Log.Information("");
        Log.Information(new string('=', 80));
        Log.Information("SINGLE DATABASE EXTRACTION - {DbType}", dbType);
        Log.Information(new string('=', 80));

        if (tables == null || tables.Count == 0)
        {
            Log.Error("✗ No tables specified for extraction");
            return;
        }

        Log.Information("Tables to process: {Count}", tables.Count);

        int successCount = 0;
        int failCount = 0;

        foreach (var table in tables)
        {
            Log.Information("");
            Log.Information(new string('-', 80));
            Log.Information("Processing table: {Table}", table);
            Log.Information(new string('-', 80));

            try
            {
                var hashes = ExtractAndHashTable(dbType, table);

                _csvWriter.WriteTableHashes(table, dbType.ToString(), hashes);
                
                successCount++;
                Log.Information("✓ Successfully processed {Table}: {Count} rows",
                               table, hashes.Count);
            }
            catch (Exception ex)
            {
                failCount++;
                Log.Error(ex, "✗ Failed to process table: {Table}", table);
            }
        }

        Log.Information("");
        Log.Information(new string('=', 80));
        Log.Information("EXTRACTION SUMMARY");
        Log.Information(new string('=', 80));
        Log.Information("Total tables processed: {Count}", tables.Count);
        Log.Information("✓ Successful: {Count}", successCount);
        Log.Information("✗ Failed: {Count}", failCount);
        Log.Information("");
        Log.Information("CSV hash files are available in the reports/ folder");
    }

    
    private Dictionary<string, string> ExtractAndHashTable(DatabaseType dbType, string tableRef)
    {
        var hashes = new Dictionary<string, string>();

        using var connection = _connectionManager.GetConnection(dbType);
        connection.Open();

        var extractor = new DataExtractor(connection, dbType);
        var metadata = extractor.GetTableMetadata(tableRef);

        Log.Information("  Extracting data from {Table}...", tableRef);

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
            }
        });

        Log.Information("  ✓ Extracted {Count} rows", hashes.Count);

        return hashes;
    }
}
