using Serilog;

namespace Ora2PgPerformanceValidator.Loaders;

public class QueryLoader
{
    private readonly ILogger _logger = Log.ForContext<QueryLoader>();

    public Dictionary<string, (string oracleQuery, string postgresQuery)> LoadQueryPairs(
        string oracleQueriesPath,
        string postgresQueriesPath,
        Dictionary<string, string>? parameters = null)
    {
        var queryPairs = new Dictionary<string, (string, string)>();

        if (!Directory.Exists(oracleQueriesPath))
        {
            _logger.Warning("Oracle queries directory not found: {Path}", oracleQueriesPath);
            return queryPairs;
        }

        if (!Directory.Exists(postgresQueriesPath))
        {
            _logger.Warning("PostgreSQL queries directory not found: {Path}", postgresQueriesPath);
            return queryPairs;
        }

        var oracleFiles = Directory.GetFiles(oracleQueriesPath, "*.sql");
        _logger.Information("Found {Count} Oracle query files", oracleFiles.Length);

        foreach (var oracleFile in oracleFiles)
        {
            var fileName = Path.GetFileNameWithoutExtension(oracleFile);
            var postgresFile = Path.Combine(postgresQueriesPath, $"{fileName}.sql");

            if (!File.Exists(postgresFile))
            {
                _logger.Warning("No matching PostgreSQL query found for: {FileName}", fileName);
                continue;
            }

            try
            {
                var oracleQuery = File.ReadAllText(oracleFile);
                var postgresQuery = File.ReadAllText(postgresFile);

                if (string.IsNullOrWhiteSpace(oracleQuery) || string.IsNullOrWhiteSpace(postgresQuery))
                {
                    _logger.Warning("Empty query file: {FileName}", fileName);
                    continue;
                }

                if (parameters != null)
                {
                    foreach (var (key, value) in parameters)
                    {
                        oracleQuery = oracleQuery.Replace($"{{{key}}}", value);
                        postgresQuery = postgresQuery.Replace($"{{{key}}}", value);
                    }
                }

                queryPairs[fileName] = (oracleQuery, postgresQuery);
                _logger.Information("Loaded query pair: {FileName}", fileName);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed to load query pair: {FileName}", fileName);
            }
        }

        return queryPairs;
    }
}
