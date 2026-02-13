namespace Ora2PgSchemaComparer.Model;


public class SequenceDefinition
{
    public string SequenceName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public decimal? CurrentValue { get; set; }
    public long? IncrementBy { get; set; }
    public long? MinValue { get; set; }
    public decimal? MaxValue { get; set; }
    public bool? IsCycle { get; set; }
    public int? CacheSize { get; set; }
    
    public string FullName => $"{SchemaName}.{SequenceName}";
    
    public override string ToString() => $"{FullName} (current: {CurrentValue}, increment: {IncrementBy})";
}

public class ViewDefinition
{
    public string ViewName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string? ViewDefinitionText { get; set; }
    public bool IsMaterialized { get; set; }
    public string? RefreshMethod { get; set; } // For materialized views
    
    public string FullName => $"{SchemaName}.{ViewName}";
    public string ViewType => IsMaterialized ? "MATERIALIZED VIEW" : "VIEW";
    
    public override string ToString() => $"{ViewType} {FullName}";
}

public class TriggerDefinition
{
    public string TriggerName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string? TriggerEvent { get; set; } // INSERT, UPDATE, DELETE
    public string? TriggerTiming { get; set; } // BEFORE, AFTER
    public string? TriggerBody { get; set; }
    public bool IsEnabled { get; set; } = true;
    
    public string FullTableName => $"{SchemaName}.{TableName}";
    
    public override string ToString() => $"{TriggerName} {TriggerTiming} {TriggerEvent} ON {FullTableName}";
}

public class ProcedureDefinition
{
    public string ProcedureName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public ProcedureType Type { get; set; }
    public string? SourceCode { get; set; }
    public List<ParameterDefinition> Parameters { get; set; } = new();
    public string? ReturnType { get; set; } // For functions
    
    public string FullName => $"{SchemaName}.{ProcedureName}";
    
    public override string ToString() => $"{Type} {FullName}({Parameters.Count} params)";
}

public class ParameterDefinition
{
    public string ParameterName { get; set; } = string.Empty;
    public int Position { get; set; }
    public string DataType { get; set; } = string.Empty;
    public string? Mode { get; set; } // IN, OUT, INOUT
    
    public override string ToString() => $"{Mode} {ParameterName} {DataType}";
}

public enum ProcedureType
{
    Procedure,
    Function,
    Package  // Oracle package
}
