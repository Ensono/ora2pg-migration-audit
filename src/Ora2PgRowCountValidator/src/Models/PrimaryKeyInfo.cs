namespace Ora2PgRowCountValidator.Models;


public class PrimaryKeyInfo
{
    public required string TableName { get; set; }
    public List<string> PrimaryKeyColumns { get; set; } = new();
    public bool HasPrimaryKey => PrimaryKeyColumns.Any();


    public string PrimaryKeyColumnsString => string.Join(", ", PrimaryKeyColumns);
}
