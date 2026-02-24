using Ora2PgRowCountValidator.Models;
using Serilog;

namespace Ora2PgRowCountValidator.Comparison;


public class RowCountComparer
{
    private readonly string? _oracleConnectionString;
    private readonly string? _postgresConnectionString;
    private readonly bool _enableDetailedComparison;

    public RowCountComparer(
        string? oracleConnectionString = null, 
        string? postgresConnectionString = null,
        bool enableDetailedComparison = true)
    {
        _oracleConnectionString = oracleConnectionString;
        _postgresConnectionString = postgresConnectionString;
        _enableDetailedComparison = enableDetailedComparison;
    }


    public async Task<ValidationResult> CompareAsync(
        List<TableRowCount> oracleCounts,
        List<TableRowCount> postgresCounts,
        string oracleSchema,
        string postgresSchema)
    {
        var result = new ValidationResult
        {
            OracleSchema = oracleSchema,
            PostgresSchema = postgresSchema
        };

        Log.Information("🔍 Comparing row counts...");


        var oracleDict = oracleCounts.ToDictionary(
            t => t.TableName.ToUpper(),
            t => t);

        var postgresDict = postgresCounts.ToDictionary(
            t => t.TableName.ToUpper(),
            t => t);

        var allTableNames = oracleDict.Keys.Union(postgresDict.Keys).ToList();
        result.TotalTablesValidated = allTableNames.Count;

        DetailedRowComparer? detailedComparer = null;
        if (_enableDetailedComparison && 
            !string.IsNullOrEmpty(_oracleConnectionString) && 
            !string.IsNullOrEmpty(_postgresConnectionString))
        {
            detailedComparer = new DetailedRowComparer(
                _oracleConnectionString,
                _postgresConnectionString,
                oracleSchema,
                postgresSchema);
        }

        foreach (var tableName in allTableNames.OrderBy(t => t))
        {
            var hasOracle = oracleDict.TryGetValue(tableName, out var oracleTable);
            var hasPostgres = postgresDict.TryGetValue(tableName, out var postgresTable);

            if (hasOracle && hasPostgres)
            {

                var issue = CompareTableRowCounts(oracleTable!, postgresTable!, result);

                if (issue != null && 
                    issue.IssueType != "Match" && 
                    detailedComparer != null)
                {
                    issue = await detailedComparer.PerformDetailedComparisonAsync(
                        issue, 
                        oracleTable!.RowCount, 
                        postgresTable!.RowCount);
                }
                
                if (oracleTable!.RowCount >= 0)
                    result.TotalOracleRows += oracleTable.RowCount;
                if (postgresTable!.RowCount >= 0)
                    result.TotalPostgresRows += postgresTable.RowCount;
            }
            else if (hasOracle && !hasPostgres)
            {

                result.TablesOnlyInOracle++;
                result.Issues.Add(new RowCountIssue
                {
                    TableName = tableName,
                    Severity = ValidationSeverity.Critical,
                    IssueType = "Missing Table",
                    OracleRowCount = oracleTable!.RowCount,
                    PostgresRowCount = null,
                    Message = $"Table exists in Oracle ({oracleTable.RowCount:N0} rows) but NOT in PostgreSQL.",
                    Recommendation = "Verify table was included in migration. Check table name case sensitivity."
                });
                
                if (oracleTable.RowCount >= 0)
                    result.TotalOracleRows += oracleTable.RowCount;
            }
            else if (!hasOracle && hasPostgres)
            {

                result.TablesOnlyInPostgres++;
                result.Issues.Add(new RowCountIssue
                {
                    TableName = tableName,
                    Severity = ValidationSeverity.Warning,
                    IssueType = "Extra Table",
                    OracleRowCount = null,
                    PostgresRowCount = postgresTable!.RowCount,
                    Message = $"Table exists in PostgreSQL ({postgresTable.RowCount:N0} rows) but NOT in Oracle.",
                    Recommendation = "Verify this is intentional (e.g., PostgreSQL-specific audit tables)."
                });
                
                if (postgresTable.RowCount >= 0)
                    result.TotalPostgresRows += postgresTable.RowCount;
            }
        }

        result.TablesWithMatchingCounts = result.Issues.Count(i => i.IssueType == "Match");
        result.TablesWithMismatchedCounts = result.Issues.Count(i => i.IssueType == "Row Count Mismatch");

        Log.Information($"✅ Comparison complete: {result.TablesWithMatchingCounts} matches, {result.TablesWithMismatchedCounts} mismatches");

        return result;
    }

    private RowCountIssue? CompareTableRowCounts(
        TableRowCount oracle,
        TableRowCount postgres,
        ValidationResult result)
    {
        RowCountIssue? issue = null;

        if (oracle.RowCount < 0 && postgres.RowCount < 0)
        {
            issue = new RowCountIssue
            {
                TableName = oracle.TableName,
                Severity = ValidationSeverity.Error,
                IssueType = "Query Error",
                OracleRowCount = oracle.RowCount,
                PostgresRowCount = postgres.RowCount,
                Message = "Failed to query row counts from both databases.",
                Recommendation = "Check table permissions and database connectivity."
            };
            result.Issues.Add(issue);
            return issue;
        }

        if (oracle.RowCount < 0)
        {
            issue = new RowCountIssue
            {
                TableName = oracle.TableName,
                Severity = ValidationSeverity.Error,
                IssueType = "Oracle Query Error",
                OracleRowCount = oracle.RowCount,
                PostgresRowCount = postgres.RowCount,
                Message = $"Failed to query row count from Oracle. PostgreSQL has {postgres.RowCount:N0} rows.",
                Recommendation = "Check Oracle table permissions and query syntax."
            };
            result.Issues.Add(issue);
            return issue;
        }

        if (postgres.RowCount < 0)
        {
            issue = new RowCountIssue
            {
                TableName = oracle.TableName,
                Severity = ValidationSeverity.Error,
                IssueType = "PostgreSQL Query Error",
                OracleRowCount = oracle.RowCount,
                PostgresRowCount = postgres.RowCount,
                Message = $"Failed to query row count from PostgreSQL. Oracle has {oracle.RowCount:N0} rows.",
                Recommendation = "Check PostgreSQL table permissions and query syntax."
            };
            result.Issues.Add(issue);
            return issue;
        }


        if (oracle.RowCount == postgres.RowCount)
        {
            issue = new RowCountIssue
            {
                TableName = oracle.TableName,
                Severity = ValidationSeverity.Info,
                IssueType = "Match",
                OracleRowCount = oracle.RowCount,
                PostgresRowCount = postgres.RowCount,
                Difference = 0,
                PercentageDifference = 0,
                Message = $"✅ Row counts match: {oracle.RowCount:N0} rows",
                PartitionRowCounts = postgres.PartitionRowCounts.ToList()
            };
            result.Issues.Add(issue);
        }
        else
        {
            var difference = Math.Abs(postgres.RowCount - oracle.RowCount);
            var percentageDiff = oracle.RowCount > 0
                ? (double)difference / oracle.RowCount * 100
                : 100;

            var severity = percentageDiff switch
            {
                > 10 => ValidationSeverity.Critical,
                > 1 => ValidationSeverity.Error,
                _ => ValidationSeverity.Warning
            };

            issue = new RowCountIssue
            {
                TableName = oracle.TableName,
                Severity = severity,
                IssueType = "Row Count Mismatch",
                OracleRowCount = oracle.RowCount,
                PostgresRowCount = postgres.RowCount,
                Difference = difference,
                PercentageDifference = percentageDiff,
                Message = $"Row count mismatch: Oracle {oracle.RowCount:N0} vs PostgreSQL {postgres.RowCount:N0} ({difference:N0} difference, {percentageDiff:F2}%)",
                Recommendation = severity == ValidationSeverity.Critical
                    ? "CRITICAL: Large difference detected. Review migration logs and data integrity."
                    : "Review data migration process. Check for data filtering, transformation issues, or ongoing transactions.",
                PartitionRowCounts = postgres.PartitionRowCounts.ToList()
            };
            result.Issues.Add(issue);
        }

        return issue;
    }
}
