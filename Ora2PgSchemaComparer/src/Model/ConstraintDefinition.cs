namespace Ora2PgSchemaComparer.Model;

public class ConstraintDefinition
{
    public string ConstraintName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public ConstraintType Type { get; set; }
    public List<string> Columns { get; set; } = new();

    public string? ReferencedSchemaName { get; set; }
    public string? ReferencedTableName { get; set; }
    public List<string>? ReferencedColumns { get; set; }
    public string? OnDeleteRule { get; set; }
    public string? OnUpdateRule { get; set; }
    public bool? IsDeferrable { get; set; }
    public bool? IsInitiallyDeferred { get; set; }

    public string? CheckCondition { get; set; }
    
    public string FullTableName => $"{SchemaName}.{TableName}";
    public string ColumnsDisplay => string.Join(", ", Columns);
    
    public override string ToString()
    {
        var result = $"{ConstraintName} ({Type}) on {FullTableName}({ColumnsDisplay})";
        if (Type == ConstraintType.ForeignKey && ReferencedTableName != null)
        {
            result += $" -> {ReferencedSchemaName}.{ReferencedTableName}";
            result += $" ON DELETE {OnDeleteRule ?? "NO ACTION"}";
        }
        return result;
    }
}

public enum ConstraintType
{
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
    NotNull
}
