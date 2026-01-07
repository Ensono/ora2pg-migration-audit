namespace Ora2PgSchemaComparer.Model;

public class IndexDefinition
{
    public string IndexName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public IndexType Type { get; set; }
    public bool IsUnique { get; set; }
    public List<IndexColumnDefinition> Columns { get; set; } = new();
    public string? FilterCondition { get; set; } // For partial/function-based indexes
    
    public string FullTableName => $"{SchemaName}.{TableName}";
    public string ColumnsDisplay => string.Join(", ", Columns.Select(c => c.ToString()));
    
    public override string ToString()
    {
        var uniqueStr = IsUnique ? "UNIQUE " : "";
        return $"{uniqueStr}{Type} INDEX {IndexName} ON {FullTableName}({ColumnsDisplay})";
    }
}

public class IndexColumnDefinition
{
    public string ColumnName { get; set; } = string.Empty;
    public int ColumnPosition { get; set; }
    public string? SortOrder { get; set; } // ASC/DESC
    public string? Expression { get; set; } // For function-based indexes
    
    public override string ToString()
    {
        if (!string.IsNullOrEmpty(Expression))
            return Expression;
        return $"{ColumnName}{(SortOrder == "DESC" ? " DESC" : "")}";
    }
}

public enum IndexType
{
    BTree,      // Standard B-Tree (PostgreSQL/Oracle)
    Bitmap,     // Oracle bitmap index (converts to GIN in PostgreSQL)
    GIN,        // PostgreSQL Generalized Inverted Index
    GiST,       // PostgreSQL Generalized Search Tree
    Hash,       // Hash index
    FunctionBased // Function-based index
}
