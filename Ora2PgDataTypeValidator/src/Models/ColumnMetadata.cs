namespace Ora2PgDataTypeValidator.Models;

public class ColumnMetadata
{
    public required string SchemaName { get; init; }
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public required string DataType { get; init; }
    public int? DataLength { get; init; }
    public int? DataPrecision { get; init; }
    public int? DataScale { get; init; }
    public bool IsNullable { get; init; }
    public string? DefaultValue { get; init; }
    public int? CharLength { get; init; }  // For VARCHAR2(n) - character semantics
    
    public string FullColumnName => $"{SchemaName}.{TableName}.{ColumnName}";
}
