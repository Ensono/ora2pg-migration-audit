using Ora2PgRowCountValidator.Models;
using Ora2PgRowCountValidator.Extractors;
using Serilog;

namespace Ora2PgRowCountValidator.Comparison;

public class DetailedRowComparer
{
    private const int MaxRowsToDisplay = 10; // Limit rows shown in reports
    private const int MaxRowsToCheck = 100;   // Limit rows to check for existence

    private readonly string _oracleSchema;
    private readonly string _postgresSchema;
    private readonly OraclePrimaryKeyExtractor _oraclePkExtractor;
    private readonly PostgresPrimaryKeyExtractor _postgresPkExtractor;

    public DetailedRowComparer(
        string oracleConnectionString,
        string postgresConnectionString,
        string oracleSchema,
        string postgresSchema)
    {
        _oracleSchema = oracleSchema;
        _postgresSchema = postgresSchema;
        _oraclePkExtractor = new OraclePrimaryKeyExtractor(oracleConnectionString);
        _postgresPkExtractor = new PostgresPrimaryKeyExtractor(postgresConnectionString);
    }


    public async Task<RowCountIssue> PerformDetailedComparisonAsync(
        RowCountIssue issue,
        long oracleCount,
        long postgresCount)
    {

        if (issue.IssueType == "Match")
        {
            issue.HasDetailedComparison = false;
            issue.DetailedComparisonSkippedReason = "Row counts match";
            return issue;
        }

        try
        {
            Log.Information($"🔍 Performing detailed row comparison for {issue.TableName}...");

            var oraclePkTask = _oraclePkExtractor.GetPrimaryKeyAsync(_oracleSchema, issue.TableName);
            var postgresPkTask = _postgresPkExtractor.GetPrimaryKeyAsync(_postgresSchema, issue.TableName);

            await Task.WhenAll(oraclePkTask, postgresPkTask);

            var oraclePk = await oraclePkTask;
            var postgresPk = await postgresPkTask;

            if (!oraclePk.HasPrimaryKey)
            {
                issue.HasDetailedComparison = false;
                issue.DetailedComparisonSkippedReason = $"No primary key in Oracle table {issue.TableName}";
                Log.Warning($"⚠️  Cannot perform detailed comparison - no PK in Oracle {issue.TableName}");
                return issue;
            }

            if (!postgresPk.HasPrimaryKey)
            {
                issue.HasDetailedComparison = false;
                issue.DetailedComparisonSkippedReason = $"No primary key in PostgreSQL table {issue.TableName}";
                Log.Warning($"⚠️  Cannot perform detailed comparison - no PK in PostgreSQL {issue.TableName}");
                return issue;
            }

            if (!PKsMatch(oraclePk, postgresPk))
            {
                issue.HasDetailedComparison = false;
                issue.DetailedComparisonSkippedReason = 
                    $"Primary key mismatch (Oracle: {oraclePk.PrimaryKeyColumnsString}, PostgreSQL: {postgresPk.PrimaryKeyColumnsString})";
                Log.Warning($"⚠️  PK columns don't match between databases for {issue.TableName}");
                return issue;
            }

            if (oracleCount > postgresCount)
            {
                issue.MissingInPostgres = await FindMissingInPostgresAsync(
                    issue.TableName, 
                    oraclePk, 
                    MaxRowsToCheck);
            }
            else if (postgresCount > oracleCount)
            {
                issue.ExtraInPostgres = await FindExtraInPostgresAsync(
                    issue.TableName, 
                    postgresPk, 
                    MaxRowsToCheck);
            }

            issue.HasDetailedComparison = true;
            
            var missingCount = issue.MissingInPostgres.Count;
            var extraCount = issue.ExtraInPostgres.Count;
            
            if (missingCount > 0)
            {
                Log.Information($"  ❌ Found {missingCount} missing rows in PostgreSQL (showing first {MaxRowsToDisplay})");
            }
            if (extraCount > 0)
            {
                Log.Information($"  ➕ Found {extraCount} extra rows in PostgreSQL (showing first {MaxRowsToDisplay})");
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"Failed to perform detailed row comparison for {issue.TableName}");
            issue.HasDetailedComparison = false;
            issue.DetailedComparisonSkippedReason = $"Error during comparison: {ex.Message}";
        }

        return issue;
    }




    private async Task<List<MissingRowInfo>> FindMissingInPostgresAsync(
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        int maxRowsToCheck)
    {
        var oracleRows = await _oraclePkExtractor.FindMissingRowsAsync(
            _oracleSchema, 
            tableName, 
            pkInfo, 
            maxRowsToCheck);

        if (oracleRows.Count == 0) return new List<MissingRowInfo>();

        var existingInPostgres = await _postgresPkExtractor.BulkRowExistsAsync(
            _postgresSchema,
            tableName,
            pkInfo,
            oracleRows.Select(r => r.PrimaryKeyValues).ToList());

        return oracleRows
            .Where(r => !existingInPostgres.Contains(BuildPkKey(r.PrimaryKeyValues, pkInfo)))
            .Take(MaxRowsToDisplay)
            .ToList();
    }

    private async Task<List<MissingRowInfo>> FindExtraInPostgresAsync(
        string tableName, 
        PrimaryKeyInfo pkInfo, 
        int maxRowsToCheck)
    {
        var postgresRows = await _postgresPkExtractor.FindExtraRowsAsync(
            _postgresSchema, 
            tableName, 
            pkInfo, 
            maxRowsToCheck);

        if (postgresRows.Count == 0) return new List<MissingRowInfo>();

        var existingInOracle = await _oraclePkExtractor.BulkRowExistsAsync(
            _oracleSchema,
            tableName,
            pkInfo,
            postgresRows.Select(r => r.PrimaryKeyValues).ToList());

        return postgresRows
            .Where(r => !existingInOracle.Contains(BuildPkKey(r.PrimaryKeyValues, pkInfo)))
            .Take(MaxRowsToDisplay)
            .ToList();
    }

    private static string BuildPkKey(Dictionary<string, object?> pkValues, PrimaryKeyInfo pkInfo)
    {
        return string.Join("|", pkInfo.PrimaryKeyColumns.Select(col => pkValues.GetValueOrDefault(col)?.ToString() ?? "NULL"));
    }


    private bool PKsMatch(PrimaryKeyInfo oraclePk, PrimaryKeyInfo postgresPk)
    {
        if (oraclePk.PrimaryKeyColumns.Count != postgresPk.PrimaryKeyColumns.Count)
            return false;

        for (int i = 0; i < oraclePk.PrimaryKeyColumns.Count; i++)
        {
            if (!oraclePk.PrimaryKeyColumns[i].Equals(
                postgresPk.PrimaryKeyColumns[i], 
                StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }
}
