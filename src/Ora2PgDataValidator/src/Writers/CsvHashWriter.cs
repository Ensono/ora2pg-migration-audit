using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Serilog;
using Ora2Pg.Common.Config;

namespace Ora2PgDataValidator.src.Writers;


public class CsvHashWriter
{
    private readonly bool _saveHashesToCsv;
    private readonly string _outputDir;

    public CsvHashWriter()
    {
        var props = ApplicationProperties.Instance;
        _saveHashesToCsv = props.GetBool("SAVE_HASHES_TO_CSV",
            props.GetBool("save.hashes.to.csv", true));
        _outputDir = "./reports";

        if (_saveHashesToCsv)
        {
            EnsureOutputDirectoryExists();
            Log.Information("CSV hash output is ENABLED. Files will be saved to: {OutputDir}", _outputDir);
        }
        else
        {
            Log.Information("CSV hash output is DISABLED. No hash files will be created.");
        }
    }

    public bool IsSaveHashesEnabled => _saveHashesToCsv;
    
    public void WriteTableHashes(string tableName, string databaseType, Dictionary<string, string> hashes)
    {
        if (!_saveHashesToCsv)
        {
            Log.Debug("Hash saving disabled, skipping CSV write for table: {TableName}", tableName);
            return;
        }

        if (hashes == null || hashes.Count == 0)
        {
            Log.Warning("No hashes to write for table: {TableName} ({DatabaseType})", tableName, databaseType);
            return;
        }

        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        string fileName = $"{tableName}_{databaseType.ToLower()}_{timestamp}_hashes.csv";
        string filePath = Path.Combine(_outputDir, fileName);

        try
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true
            };

            using var writer = new StreamWriter(filePath);
            using var csv = new CsvWriter(writer, config);

            csv.WriteField("Row_ID");
            csv.WriteField("Hash_Value");
            csv.WriteField("Hash_Length");
            csv.NextRecord();

            foreach (var entry in hashes)
            {
                csv.WriteField(entry.Key);
                csv.WriteField(entry.Value);
                csv.WriteField(entry.Value?.Length ?? 0);
                csv.NextRecord();
            }

            Log.Information("✓ Saved {Count} hash values to: {FilePath}", hashes.Count, filePath);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "✗ Failed to write hash CSV file: {FilePath}", filePath);
        }
    }

    private void EnsureOutputDirectoryExists()
    {
        if (!Directory.Exists(_outputDir))
        {
            Directory.CreateDirectory(_outputDir);
            Log.Debug("Created reports directory: {OutputDir}", _outputDir);
        }
    }
}
