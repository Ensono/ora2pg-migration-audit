namespace Ora2PgRowCountValidator.Models;


public class TableRowCount
{
    public required string SchemaName { get; set; }
    public required string TableName { get; set; }
    public long RowCount { get; set; }
    

    public string FullTableName => $"{SchemaName}.{TableName}";
}
