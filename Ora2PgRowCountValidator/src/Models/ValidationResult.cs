namespace Ora2PgRowCountValidator.Models;

public class ValidationResult
{
    public required string OracleSchema { get; set; }
    public required string PostgresSchema { get; set; }
    public DateTime ValidationTime { get; set; } = DateTime.Now;
    public List<RowCountIssue> Issues { get; set; } = new();
    public int TotalTablesValidated { get; set; }
    public int TablesWithMatchingCounts { get; set; }
    public int TablesWithMismatchedCounts { get; set; }
    public int TablesOnlyInOracle { get; set; }
    public int TablesOnlyInPostgres { get; set; }
    public long TotalOracleRows { get; set; }
    public long TotalPostgresRows { get; set; }


    public string OverallStatus
    {
        get
        {
            if (CriticalIssues > 0 || Errors > 0)
                return "FAILED";
            if (Warnings > 0)
                return "WARNING";
            return "PASSED";
        }
    }


    public int CriticalIssues => Issues.Count(i => i.Severity == ValidationSeverity.Critical);


    public int Errors => Issues.Count(i => i.Severity == ValidationSeverity.Error);


    public int Warnings => Issues.Count(i => i.Severity == ValidationSeverity.Warning);


    public int InfoMessages => Issues.Count(i => i.Severity == ValidationSeverity.Info);


    public bool HasCriticalIssues => CriticalIssues > 0;


    public bool HasErrors => Errors > 0;
}
