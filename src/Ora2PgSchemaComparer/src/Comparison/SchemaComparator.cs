using Serilog;
using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Comparison;


public class SchemaComparator
{
    private readonly ILogger _logger = Log.ForContext<SchemaComparator>();
    
    public ComparisonResult Compare(SchemaDefinition oracleSchema, SchemaDefinition postgresSchema)
    {
        _logger.Information("Comparing schemas: {OracleSchema} vs {PostgresSchema}", 
            oracleSchema.SchemaName, postgresSchema.SchemaName);
        
        var result = new ComparisonResult
        {
            OracleSchema = oracleSchema,
            PostgresSchema = postgresSchema
        };

        var postgresSummary = PartitionNormalization.BuildPostgresSummary(result.PostgresSchema);

        CompareTables(result, postgresSummary);
        CompareConstraints(result, postgresSummary.LogicalTables.Keys);
        CompareIndexes(result, postgresSummary.LogicalTables.Keys);
        CompareCodeObjects(result);
        
        _logger.Information("✓ Schema comparison complete: {TotalIssues} issues found", result.TotalIssues);
        
        return result;
    }
    
    private void CompareTables(ComparisonResult result, LogicalSchemaSummary postgresSummary)
    {
        var oracleTables = result.OracleSchema.Tables.ToDictionary(t => t.TableName.ToUpper());
        var postgresTables = postgresSummary.LogicalTables;

        result.OracleLogicalTableCount = oracleTables.Count;
        result.OracleLogicalColumnCount = result.OracleSchema.ColumnCount;
        result.PostgresLogicalTableCount = postgresSummary.LogicalTableCount;
        result.PostgresLogicalColumnCount = postgresSummary.LogicalColumnCount;
        result.PostgresPhysicalTableCount = postgresSummary.PhysicalTableCount;
        result.PostgresPartitionedTableCount = postgresSummary.PartitionedTableCount;
        result.PostgresPartitionCount = postgresSummary.PartitionCount;
        result.PartitionDetails = postgresSummary.PartitionDetails;

        if (oracleTables.Count != postgresTables.Count)
        {
            result.TableIssues.Add($"Table count mismatch: Oracle={oracleTables.Count}, PostgreSQL={postgresTables.Count}");
        }

        foreach (var oracleTable in oracleTables.Values)
        {
            var tableName = oracleTable.TableName.ToUpper();
            if (!postgresTables.ContainsKey(tableName))
            {
                result.TableIssues.Add($"❌ Table missing in PostgreSQL: {tableName}");
                continue;
            }
            
            var postgresTable = postgresTables[tableName];

            if (oracleTable.Columns.Count != postgresTable.Columns.Count)
            {
                result.TableIssues.Add($"⚠️ Column count mismatch in {tableName}: Oracle={oracleTable.Columns.Count}, PostgreSQL={postgresTable.Columns.Count}");
            }

            var oracleColumns = oracleTable.Columns.OrderBy(c => c.ColumnPosition).Select(c => c.ColumnName.ToUpper()).ToList();
            var postgresColumns = postgresTable.Columns.OrderBy(c => c.ColumnPosition).Select(c => c.ColumnName.ToUpper()).ToList();
            
            for (int i = 0; i < Math.Min(oracleColumns.Count, postgresColumns.Count); i++)
            {
                if (oracleColumns[i] != postgresColumns[i])
                {
                    result.TableIssues.Add($"⚠️ Column order mismatch in {tableName} at position {i+1}: Oracle={oracleColumns[i]}, PostgreSQL={postgresColumns[i]}");
                }
            }
        }

        foreach (var postgresTable in postgresTables.Values)
        {
            if (!oracleTables.ContainsKey(postgresTable.TableName.ToUpper()))
            {
                result.TableIssues.Add($"ℹ️ Extra table in PostgreSQL: {postgresTable.TableName}");
            }
        }
    }
    
    private void CompareConstraints(ComparisonResult result, IEnumerable<string> postgresLogicalTableNames)
    {
        var logicalTableSet = new HashSet<string>(postgresLogicalTableNames, StringComparer.OrdinalIgnoreCase);

        var oraclePKs = result.OracleSchema.Constraints.Where(c => c.Type == ConstraintType.PrimaryKey).ToList();
        var postgresPKs = result.PostgresSchema.Constraints
            .Where(c => c.Type == ConstraintType.PrimaryKey)
            .Where(c => logicalTableSet.Contains(c.TableName.ToUpperInvariant()))
            .ToList();

        result.OracleLogicalPrimaryKeyCount = oraclePKs.Count;
        result.PostgresLogicalPrimaryKeyCount = postgresPKs.Count;
        
        if (oraclePKs.Count != postgresPKs.Count)
        {
            result.ConstraintIssues.Add($"Primary key count mismatch: Oracle={oraclePKs.Count}, PostgreSQL={postgresPKs.Count}");
        }

        var oracleFKs = result.OracleSchema.Constraints.Where(c => c.Type == ConstraintType.ForeignKey).ToList();
        var postgresFKs = result.PostgresSchema.Constraints
            .Where(c => c.Type == ConstraintType.ForeignKey)
            .Where(c => logicalTableSet.Contains(c.TableName.ToUpperInvariant()))
            .ToList();

        result.OracleLogicalForeignKeyCount = oracleFKs.Count;
        result.PostgresLogicalForeignKeyCount = postgresFKs.Count;
        
        if (oracleFKs.Count != postgresFKs.Count)
        {
            result.ConstraintIssues.Add($"Foreign key count mismatch: Oracle={oracleFKs.Count}, PostgreSQL={postgresFKs.Count}");
        }

        foreach (var oracleFK in oracleFKs)
        {
            var matchingFK = postgresFKs.FirstOrDefault(fk => 
                fk.TableName.Equals(oracleFK.TableName, StringComparison.OrdinalIgnoreCase) &&
                fk.ReferencedTableName != null &&
                fk.ReferencedTableName.Equals(oracleFK.ReferencedTableName, StringComparison.OrdinalIgnoreCase));
            
            if (matchingFK != null && matchingFK.OnDeleteRule != oracleFK.OnDeleteRule)
            {
                result.ConstraintIssues.Add($"⚠️ ON DELETE rule mismatch for FK on {oracleFK.TableName}: Oracle={oracleFK.OnDeleteRule}, PostgreSQL={matchingFK.OnDeleteRule}");
            }
        }

        var oracleUniques = result.OracleSchema.Constraints.Where(c => c.Type == ConstraintType.Unique).ToList();
        var postgresUniques = result.PostgresSchema.Constraints
            .Where(c => c.Type == ConstraintType.Unique)
            .Where(c => logicalTableSet.Contains(c.TableName.ToUpperInvariant()))
            .ToList();

        result.OracleLogicalUniqueConstraintCount = oracleUniques.Count;
        result.PostgresLogicalUniqueConstraintCount = postgresUniques.Count;
        
        if (oracleUniques.Count != postgresUniques.Count)
        {
            result.ConstraintIssues.Add($"Unique constraint count mismatch: Oracle={oracleUniques.Count}, PostgreSQL={postgresUniques.Count}");
        }

        var oracleChecks = result.OracleSchema.Constraints.Where(c => c.Type == ConstraintType.Check).ToList();
        var postgresChecks = result.PostgresSchema.Constraints
            .Where(c => c.Type == ConstraintType.Check)
            .Where(c => logicalTableSet.Contains(c.TableName.ToUpperInvariant()))
            .ToList();

        result.OracleLogicalCheckConstraintCount = oracleChecks.Count;
        result.PostgresLogicalCheckConstraintCount = postgresChecks.Count;
    }
    
    private void CompareIndexes(ComparisonResult result, IEnumerable<string> postgresLogicalTableNames)
    {
        var logicalTableSet = new HashSet<string>(postgresLogicalTableNames, StringComparer.OrdinalIgnoreCase);

        var oracleIndexes = result.OracleSchema.Indexes.ToList();
        var postgresIndexes = result.PostgresSchema.Indexes
            .Where(i => logicalTableSet.Contains(i.TableName.ToUpperInvariant()))
            .ToList();

        result.OracleLogicalIndexCount = oracleIndexes.Count;
        result.PostgresLogicalIndexCount = postgresIndexes.Count;
        
        if (oracleIndexes.Count != postgresIndexes.Count)
        {
            result.IndexIssues.Add($"Index count mismatch: Oracle={oracleIndexes.Count}, PostgreSQL={postgresIndexes.Count}");
        }

        var bitmapIndexes = oracleIndexes.Where(i => i.Type == IndexType.Bitmap).ToList();
        if (bitmapIndexes.Any())
        {
            result.IndexIssues.Add($"ℹ️ {bitmapIndexes.Count} Oracle BITMAP indexes found (should be converted to GIN in PostgreSQL)");
        }
    }
    
    private void CompareCodeObjects(ComparisonResult result)
    {
        if (result.OracleSchema.SequenceCount != result.PostgresSchema.SequenceCount)
        {
            result.CodeObjectIssues.Add($"Sequence count mismatch: Oracle={result.OracleSchema.SequenceCount}, PostgreSQL={result.PostgresSchema.SequenceCount}");
        }

        if (result.OracleSchema.ViewCount != result.PostgresSchema.ViewCount)
        {
            result.CodeObjectIssues.Add($"View count mismatch: Oracle={result.OracleSchema.ViewCount}, PostgreSQL={result.PostgresSchema.ViewCount}");
        }

        if (result.OracleSchema.MaterializedViewCount != result.PostgresSchema.MaterializedViewCount)
        {
            result.CodeObjectIssues.Add($"Materialized view count mismatch: Oracle={result.OracleSchema.MaterializedViewCount}, PostgreSQL={result.PostgresSchema.MaterializedViewCount}");
        }

        if (result.OracleSchema.TriggerCount != result.PostgresSchema.TriggerCount)
        {
            result.CodeObjectIssues.Add($"Trigger count mismatch: Oracle={result.OracleSchema.TriggerCount}, PostgreSQL={result.PostgresSchema.TriggerCount}");
        }

        var oracleProcCount = result.OracleSchema.ProcedureCount + result.OracleSchema.FunctionCount;
        var postgresProcCount = result.PostgresSchema.ProcedureCount + result.PostgresSchema.FunctionCount;
        
        if (oracleProcCount != postgresProcCount)
        {
            result.CodeObjectIssues.Add($"Procedure/Function count mismatch: Oracle={oracleProcCount}, PostgreSQL={postgresProcCount}");
        }
    }
}

public class ComparisonResult
{
    public SchemaDefinition OracleSchema { get; set; } = new();
    public SchemaDefinition PostgresSchema { get; set; } = new();

    public int OracleLogicalTableCount { get; set; }
    public int PostgresLogicalTableCount { get; set; }
    public int PostgresPhysicalTableCount { get; set; }
    public int OracleLogicalColumnCount { get; set; }
    public int PostgresLogicalColumnCount { get; set; }
    public int OracleLogicalPrimaryKeyCount { get; set; }
    public int PostgresLogicalPrimaryKeyCount { get; set; }
    public int OracleLogicalForeignKeyCount { get; set; }
    public int PostgresLogicalForeignKeyCount { get; set; }
    public int OracleLogicalUniqueConstraintCount { get; set; }
    public int PostgresLogicalUniqueConstraintCount { get; set; }
    public int OracleLogicalCheckConstraintCount { get; set; }
    public int PostgresLogicalCheckConstraintCount { get; set; }
    public int OracleLogicalIndexCount { get; set; }
    public int PostgresLogicalIndexCount { get; set; }
    public int PostgresPartitionedTableCount { get; set; }
    public int PostgresPartitionCount { get; set; }
    public List<string> PartitionDetails { get; set; } = new();
    
    public List<string> TableIssues { get; set; } = new();
    public List<string> ConstraintIssues { get; set; } = new();
    public List<string> IndexIssues { get; set; } = new();
    public List<string> CodeObjectIssues { get; set; } = new();
    
    public int TotalIssues => TableIssues.Count + ConstraintIssues.Count + IndexIssues.Count + CodeObjectIssues.Count;
    
    public bool HasCriticalIssues => TableIssues.Any(i => i.Contains("❌")) || 
                                     ConstraintIssues.Any(i => i.Contains("❌"));
    
    public string OverallGrade
    {
        get
        {
            if (TotalIssues == 0) return "A+";
            if (TotalIssues <= 5 && !HasCriticalIssues) return "A";
            if (TotalIssues <= 10) return "B+";
            if (TotalIssues <= 20) return "B";
            return "C";
        }
    }
}
