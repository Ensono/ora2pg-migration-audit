namespace Ora2PgSchemaComparer.Model;

public class TableDefinition
{
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string FullName => $"{SchemaName}.{TableName}";
    public List<ColumnDefinition> Columns { get; set; } = new();
    public string? TableComment { get; set; }
    public bool IsPartitioned { get; set; }
    public PartitionStrategy PartitionStrategy { get; set; } = PartitionStrategy.None;
    public List<string> PartitionColumns { get; set; } = new();
    public List<PartitionDefinition> Partitions { get; set; } = new();
    
    public override string ToString() => FullName;
}

public class ColumnDefinition
{
    public string ColumnName { get; set; } = string.Empty;
    public int ColumnPosition { get; set; }
    public string DataType { get; set; } = string.Empty;
    public int? DataLength { get; set; }
    public int? DataPrecision { get; set; }
    public int? DataScale { get; set; }
    public bool IsNullable { get; set; }
    public string? DefaultValue { get; set; }
    public string? ColumnComment { get; set; }
    
    public string DisplayType
    {
        get
        {
            if (DataPrecision.HasValue && DataScale.HasValue)
                return $"{DataType}({DataPrecision},{DataScale})";
            if (DataPrecision.HasValue)
                return $"{DataType}({DataPrecision})";
            if (DataLength.HasValue && DataType.Contains("CHAR"))
                return $"{DataType}({DataLength})";
            return DataType;
        }
    }
    
    public override string ToString() => $"{ColumnName} {DisplayType} {(IsNullable ? "NULL" : "NOT NULL")}";
}
