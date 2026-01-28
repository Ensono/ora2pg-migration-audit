namespace Ora2PgPerformanceValidator.Models;

public class QueryPerformanceResult
{
    public string QueryName { get; set; } = string.Empty;
    public string QueryPath { get; set; } = string.Empty;
    public bool OracleExecuted { get; set; }
    public bool PostgresExecuted { get; set; }
    public double OracleExecutionTimeMs { get; set; }
    public double PostgresExecutionTimeMs { get; set; }
    public long OracleRowsAffected { get; set; }
    public long PostgresRowsAffected { get; set; }
    public string? OracleError { get; set; }
    public string? PostgresError { get; set; }
    public double PerformanceDifferencePercent { get; set; }
    public PerformanceStatus Status { get; set; }
    public string? Notes { get; set; }
}

public enum PerformanceStatus
{
    Passed,
    Warning,
    Failed,
    RowCountMismatch
}

public class PerformanceTestSummary
{
    public DateTime TestStartTime { get; set; }
    public DateTime TestEndTime { get; set; }
    public int TotalQueries { get; set; }
    public int PassedQueries { get; set; }
    public int WarningQueries { get; set; }
    public int FailedQueries { get; set; }
    public int RowCountMismatchQueries { get; set; }
    public double AverageOracleExecutionTimeMs { get; set; }
    public double AveragePostgresExecutionTimeMs { get; set; }
    public double TotalTestDurationMs { get; set; }
    public int ThresholdPercent { get; set; } = 50;
    public List<QueryPerformanceResult> Results { get; set; } = new();
}
