namespace Ora2PgDataTypeValidator.Models;

public class ValidationResult
{
    public DateTime ValidationTime { get; init; } = DateTime.Now;
    public required string OracleSchema { get; init; }
    public required string PostgresSchema { get; init; }
    public List<ValidationIssue> Issues { get; init; } = new();
    public int TotalColumnsValidated { get; set; }
    public int CriticalIssues => Issues.Count(i => i.Severity == ValidationSeverity.Critical);
    public int Warnings => Issues.Count(i => i.Severity == ValidationSeverity.Warning);
    public int Errors => Issues.Count(i => i.Severity == ValidationSeverity.Error);
    public int InfoMessages => Issues.Count(i => i.Severity == ValidationSeverity.Info);
    
    public bool HasCriticalIssues => CriticalIssues > 0;
    public bool HasErrors => Errors > 0;
    public string OverallStatus => HasCriticalIssues || HasErrors ? "FAILED" : Warnings > 0 ? "WARNING" : "PASSED";
}
