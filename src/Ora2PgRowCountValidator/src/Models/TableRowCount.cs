namespace Ora2PgRowCountValidator.Models;


public class TableRowCount
{
    public required string SchemaName { get; set; }
    public required string TableName { get; set; }
    public long RowCount { get; set; }
    public bool IsPartitioned { get; set; }
    public List<PartitionRowCount> PartitionRowCounts { get; set; } = new();
    

    public string FullTableName => $"{SchemaName}.{TableName}";
}
