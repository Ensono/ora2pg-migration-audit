namespace Ora2PgDataValidator.Extractor;

public class TableMetadata
{
    public string TableName { get; }
    public List<ColumnMetadata> Columns { get; }
    public List<string> PrimaryKeyColumns { get; }

    
    public string? PrimaryKeyColumn => PrimaryKeyColumns.Count > 0 ? PrimaryKeyColumns[0] : null;

    public TableMetadata(string tableName, List<ColumnMetadata> columns, List<string> primaryKeyColumns)
    {
        TableName = tableName;
        Columns = columns;
        PrimaryKeyColumns = primaryKeyColumns;
    }

    
    public class ColumnMetadata
    {
        public string Name { get; }
        public string Type { get; }
        public int Position { get; }

        public ColumnMetadata(string name, string type, int position)
        {
            Name = name;
            Type = type;
            Position = position;
        }

        public override string ToString() => $"{Name} ({Type}) [Position {Position}]";
    }

    public override string ToString() => 
        $"Table: {TableName}, Columns: {Columns.Count}, PK: {(PrimaryKeyColumns.Count > 0 ? string.Join(", ", PrimaryKeyColumns) : "None")}";
}
