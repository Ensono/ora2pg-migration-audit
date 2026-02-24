namespace Ora2PgRowCountValidator.Models;

public class PartitionRowCount
{
    public required string PartitionName { get; set; }
    public long RowCount { get; set; }
}
