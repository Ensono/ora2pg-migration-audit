namespace Ora2PgDataTypeValidator.Models;

public enum ValidationSeverity
{
    Info,
    Warning,
    Critical,
    Error
}


public class ValidationIssue
{
    public required string SchemaName { get; init; }
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public required string OracleType { get; init; }
    public required string PostgresType { get; init; }
    public required ValidationSeverity Severity { get; init; }
    public required string Category { get; init; }
    public required string Message { get; init; }
    public string? Recommendation { get; init; }
    public Dictionary<string, string> Metadata { get; init; } = new();
    
    public string FullColumnName => $"{SchemaName}.{TableName}.{ColumnName}";
}
