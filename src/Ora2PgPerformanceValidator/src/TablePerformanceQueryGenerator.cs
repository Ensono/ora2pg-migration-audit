using Ora2PgPerformanceValidator.Models;
using Serilog;

namespace Ora2PgPerformanceValidator.Generators;

public class TablePerformanceQueryGenerator
{
    private readonly string _oracleSchema;
    private readonly string _postgresSchema;
    private readonly ILogger _logger = Log.ForContext<TablePerformanceQueryGenerator>();

    public TablePerformanceQueryGenerator(string oracleSchema, string postgresSchema)
    {
        _oracleSchema = oracleSchema;
        _postgresSchema = postgresSchema;
    }

    public List<(string name, string oracleQuery, string postgresQuery, string category)> GenerateQueries(
        List<TableInfo> tables,
        bool enableCountTests = true,
        bool enableSampleTests = true,
        bool enablePkLookupTests = true,
        bool enableOrderedScanTests = true,
        int sampleRowLimit = 100)
    {
        var queries = new List<(string, string, string, string)>();

        _logger.Information("Generating performance queries for {Count} tables", tables.Count);

        foreach (var table in tables)
        {
            // Use lowercase table name for consistency in logs and reports
            var tableNameLower = table.Name.ToLower();
            
            // 1. Count query (aggregate - tests full table scan)
            if (enableCountTests)
            {
                queries.Add((
                    $"table_count_{tableNameLower}",
                    $"SELECT COUNT(*) as row_count FROM {_oracleSchema}.{table.Name}",
                    $"SELECT COUNT(*) as row_count FROM {_postgresSchema.ToLower()}.{tableNameLower}",
                    "table_count"
                ));
            }

            // 2. Sample query (tests data retrieval with LIMIT)
            if (enableSampleTests)
            {
                queries.Add((
                    $"table_sample_{tableNameLower}",
                    $"SELECT * FROM {_oracleSchema}.{table.Name} WHERE ROWNUM <= {sampleRowLimit}",
                    $"SELECT * FROM {_postgresSchema.ToLower()}.{tableNameLower} LIMIT {sampleRowLimit}",
                    "table_sample"
                ));
            }

            // 3. PK lookup (if PK exists - tests index usage)
            if (enablePkLookupTests && table.PrimaryKey != null)
            {
                // Note: Actual value will be substituted later
                queries.Add((
                    $"table_pk_lookup_{tableNameLower}",
                    $"SELECT * FROM {_oracleSchema}.{table.Name} WHERE {table.PrimaryKey.Column} = {{PK_VALUE}}",
                    $"SELECT * FROM {_postgresSchema.ToLower()}.{tableNameLower} WHERE {table.PrimaryKey.Column.ToLower()} = {{PK_VALUE}}",
                    "pk_lookup"
                ));
            }

            // 4. Ordered scan (if PK exists - tests sort performance)
            if (enableOrderedScanTests && table.PrimaryKey != null)
            {
                queries.Add((
                    $"table_ordered_{tableNameLower}",
                    $"SELECT * FROM {_oracleSchema}.{table.Name} ORDER BY {table.PrimaryKey.Column} FETCH FIRST {sampleRowLimit} ROWS ONLY",
                    $"SELECT * FROM {_postgresSchema.ToLower()}.{tableNameLower} ORDER BY {table.PrimaryKey.Column.ToLower()} LIMIT {sampleRowLimit}",
                    "ordered_scan"
                ));
            }
        }

        _logger.Information("Generated {Count} performance test queries", queries.Count);
        
        var breakdown = queries.GroupBy(q => q.Item4)
            .Select(g => $"{g.Key}: {g.Count()}")
            .ToList();
        _logger.Information("Query breakdown: {Breakdown}", string.Join(", ", breakdown));

        return queries;
    }

    public (string oracle, string postgres) SubstitutePkValue(
        string oracleQuery, 
        string postgresQuery, 
        string pkValue)
    {
        return (
            oracleQuery.Replace("{PK_VALUE}", pkValue),
            postgresQuery.Replace("{PK_VALUE}", pkValue)
        );
    }
}
