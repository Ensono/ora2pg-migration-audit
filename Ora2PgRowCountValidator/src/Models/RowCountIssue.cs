namespace Ora2PgRowCountValidator.Models;


public enum ValidationSeverity
{
    Info,
    Warning,
    Error,
    Critical
}


public class RowCountIssue
{
    public required string TableName { get; set; }
    public ValidationSeverity Severity { get; set; }
    public required string IssueType { get; set; }
    public long? OracleRowCount { get; set; }
    public long? PostgresRowCount { get; set; }
    public long? Difference { get; set; }
    public double? PercentageDifference { get; set; }
    public required string Message { get; set; }
    public string? Recommendation { get; set; }
    

    public List<MissingRowInfo> MissingInPostgres { get; set; } = new();
    

    public List<MissingRowInfo> ExtraInPostgres { get; set; } = new();
    

    public bool HasDetailedComparison { get; set; }
    

    public string? DetailedComparisonSkippedReason { get; set; }
}
