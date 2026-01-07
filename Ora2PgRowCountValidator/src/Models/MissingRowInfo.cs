namespace Ora2PgRowCountValidator.Models;


public class MissingRowInfo
{
    public required string TableName { get; set; }


    public Dictionary<string, object?> PrimaryKeyValues { get; set; } = new();


    public string PrimaryKeyDisplay => string.Join(", ", 
        PrimaryKeyValues.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}"));
}
