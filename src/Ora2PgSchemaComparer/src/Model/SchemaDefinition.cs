namespace Ora2PgSchemaComparer.Model;

public class SchemaDefinition
{
    public string SchemaName { get; set; } = string.Empty;
    public string DatabaseType { get; set; } = string.Empty; // "Oracle" or "PostgreSQL"

    public List<TableDefinition> Tables { get; set; } = new();
    public List<ConstraintDefinition> Constraints { get; set; } = new();
    public List<IndexDefinition> Indexes { get; set; } = new();
    public List<SequenceDefinition> Sequences { get; set; } = new();
    public List<ViewDefinition> Views { get; set; } = new();
    public List<TriggerDefinition> Triggers { get; set; } = new();
    public List<ProcedureDefinition> Procedures { get; set; } = new();

    public int TableCount => Tables.Count;
    public int ColumnCount => Tables.Sum(t => t.Columns.Count);
    public int PrimaryKeyCount => Constraints.Count(c => c.Type == ConstraintType.PrimaryKey);
    public int ForeignKeyCount => Constraints.Count(c => c.Type == ConstraintType.ForeignKey);
    public int UniqueConstraintCount => Constraints.Count(c => c.Type == ConstraintType.Unique);
    public int CheckConstraintCount => Constraints.Count(c => c.Type == ConstraintType.Check);
    public int IndexCount => Indexes.Count;
    public int SequenceCount => Sequences.Count;
    public int ViewCount => Views.Count(v => !v.IsMaterialized);
    public int MaterializedViewCount => Views.Count(v => v.IsMaterialized);
    public int TriggerCount => Triggers.Count;
    public int ProcedureCount => Procedures.Count(p => p.Type == ProcedureType.Procedure);
    public int FunctionCount => Procedures.Count(p => p.Type == ProcedureType.Function);
    
    public override string ToString() => $"{DatabaseType} Schema: {SchemaName} ({TableCount} tables)";
}
