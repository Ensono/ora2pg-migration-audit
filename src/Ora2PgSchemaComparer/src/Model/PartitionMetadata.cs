namespace Ora2PgSchemaComparer.Model;

public enum PartitionStrategy
{
    None,
    Range,
    List
}

public class PartitionMetadata
{
    public string ParentTableName { get; set; } = string.Empty;
    public PartitionStrategy Strategy { get; set; } = PartitionStrategy.None;
    public List<string> PartitionColumns { get; set; } = new();
    public List<PartitionDefinition> Partitions { get; set; } = new();
}

public class PartitionDefinition
{
    public string PartitionName { get; set; } = string.Empty;
    public string? BoundaryDefinition { get; set; }
}
