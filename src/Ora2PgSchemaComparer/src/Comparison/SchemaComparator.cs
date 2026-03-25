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

        result.OracleExtractionErrors.AddRange(oracleSchema.ExtractionErrors);

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
        result.PostgresPhysicalTableCount = postgresSummary.PhysicalTableCount;
        result.PostgresPartitionedTableCount = postgresSummary.PartitionedTableCount;
        result.PostgresPartitionCount = postgresSummary.PartitionCount;
        result.PartitionDetails = postgresSummary.PartitionDetails;
        
        // Track total rowid columns for adjusting the logical column count
        int totalRowidColumns = 0;

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

            var oracleColumns = oracleTable.Columns.OrderBy(c => c.ColumnPosition).Select(c => c.ColumnName.ToUpper()).ToList();
            var postgresColumns = postgresTable.Columns.OrderBy(c => c.ColumnPosition).Select(c => c.ColumnName.ToUpper()).ToList();
            
            var rowidColumns = postgresColumns.Where(c => c.Equals("ROWID", StringComparison.OrdinalIgnoreCase)).ToList();
            var postgresColumnsExcludingRowid = postgresColumns.Where(c => !c.Equals("ROWID", StringComparison.OrdinalIgnoreCase)).ToList();
            
            if (rowidColumns.Any())
            {
                totalRowidColumns += rowidColumns.Count;
                result.DmsRowidColumnCount += rowidColumns.Count;
                result.DmsArtifacts.Add($"ℹ️ [DMS Expected] Table '{tableName}' has 'rowid' column (added by DMS for tables without PK)");
            }
            
            if (oracleColumns.Count != postgresColumnsExcludingRowid.Count)
            {
                result.TableIssues.Add($"⚠️ Column count mismatch in {tableName}: Oracle={oracleColumns.Count}, PostgreSQL={postgresColumnsExcludingRowid.Count} (excluding DMS rowid)");
            }
            
            for (int i = 0; i < Math.Min(oracleColumns.Count, postgresColumnsExcludingRowid.Count); i++)
            {
                if (oracleColumns[i] != postgresColumnsExcludingRowid[i])
                {
                    result.TableIssues.Add($"⚠️ Column order mismatch in {tableName} at position {i+1}: Oracle={oracleColumns[i]}, PostgreSQL={postgresColumnsExcludingRowid[i]}");
                }
            }
        }
        
        // Set the PostgreSQL logical column count excluding DMS rowid columns
        result.PostgresLogicalColumnCount = postgresSummary.LogicalColumnCount - totalRowidColumns;

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

        // Identify synthetic PKs (rowid) added by DMS
        var syntheticPKs = postgresPKs
            .Where(pk => pk.Columns.Count == 1 && 
                        pk.Columns[0].Equals("rowid", StringComparison.OrdinalIgnoreCase))
            .ToList();
        
        result.SyntheticPrimaryKeyCount = syntheticPKs.Count;
        
        foreach (var syntheticPK in syntheticPKs)
        {
            result.DmsArtifacts.Add($"ℹ️ [DMS Expected] Table '{syntheticPK.TableName}' has synthetic primary key 'rowid' (added by DMS for tables without PK)");
        }

        var postgresPKsExcludingSynthetic = postgresPKs.Except(syntheticPKs).ToList();

        result.OracleLogicalPrimaryKeyCount = oraclePKs.Count;
        result.PostgresLogicalPrimaryKeyCount = postgresPKsExcludingSynthetic.Count;
        
        if (oraclePKs.Count != postgresPKsExcludingSynthetic.Count)
        {
            var postgresPKTableSet = new HashSet<string>(
                postgresPKsExcludingSynthetic.Select(pk => pk.TableName), 
                StringComparer.OrdinalIgnoreCase);
            
            var missingPKs = oraclePKs
                .Where(pk => !postgresPKTableSet.Contains(pk.TableName))
                .ToList();
            
            foreach (var missingPK in missingPKs)
            {
                result.ConstraintIssues.Add($"❌ Missing PK: {missingPK.TableName}.{missingPK.ConstraintName}");
            }
            
            var oraclePKTableSet = new HashSet<string>(
                oraclePKs.Select(pk => pk.TableName), 
                StringComparer.OrdinalIgnoreCase);
            
            var extraPKs = postgresPKsExcludingSynthetic
                .Where(pk => !oraclePKTableSet.Contains(pk.TableName))
                .ToList();
            
            foreach (var extraPK in extraPKs)
            {
                result.ConstraintIssues.Add($"➕ Extra PK in PostgreSQL: {extraPK.TableName}.{extraPK.ConstraintName}");
            }
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
            var postgresFKSet = new HashSet<string>(
                postgresFKs.Select(fk => $"{fk.TableName}→{fk.ReferencedTableName}".ToUpperInvariant()));
            
            var missingFKs = oracleFKs
                .Where(fk => !postgresFKSet.Contains($"{fk.TableName}→{fk.ReferencedTableName}".ToUpperInvariant()))
                .ToList();
            
            foreach (var missingFK in missingFKs)
            {
                result.ConstraintIssues.Add($"⚠️ Missing FK: {missingFK.TableName}.{missingFK.ConstraintName} → {missingFK.ReferencedTableName}");
            }
            
            var oracleFKSet = new HashSet<string>(
                oracleFKs.Select(fk => $"{fk.TableName}→{fk.ReferencedTableName}".ToUpperInvariant()));
            
            var extraFKs = postgresFKs
                .Where(fk => !oracleFKSet.Contains($"{fk.TableName}→{fk.ReferencedTableName}".ToUpperInvariant()))
                .ToList();
            
            foreach (var extraFK in extraFKs)
            {
                result.ConstraintIssues.Add($"⚠️ Extra FK in PostgreSQL: {extraFK.TableName}.{extraFK.ConstraintName} → {extraFK.ReferencedTableName}");
            }
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

        var rowidIndexes = postgresIndexes
            .Where(i => i.IndexName.ToLowerInvariant().Contains("rowid") ||
                       (i.Columns != null && i.Columns.Any(c => c.ColumnName.Equals("rowid", StringComparison.OrdinalIgnoreCase))))
            .ToList();
        
        if (rowidIndexes.Any())
        {
            result.DmsRowidIndexCount = rowidIndexes.Count;
            foreach (var idx in rowidIndexes)
            {
                result.DmsArtifacts.Add($"ℹ️ [DMS Expected] Index '{idx.IndexName}' on table '{idx.TableName}' (rowid index added by DMS)");
            }
        }
        
        var postgresIndexesExcludingRowid = postgresIndexes.Except(rowidIndexes).ToList();
        
        result.OracleLogicalIndexCount = oracleIndexes.Count;
        result.PostgresLogicalIndexCount = postgresIndexesExcludingRowid.Count;
        
        if (oracleIndexes.Count != postgresIndexesExcludingRowid.Count)
        {
            var rowidNote = rowidIndexes.Any() ? $" (excluding {rowidIndexes.Count} DMS rowid indexes)" : "";
            result.IndexIssues.Add($"Index count mismatch: Oracle={oracleIndexes.Count}, PostgreSQL={postgresIndexesExcludingRowid.Count}{rowidNote}");
        }

        var bitmapIndexes = oracleIndexes.Where(i => i.Type == IndexType.Bitmap).ToList();
        if (bitmapIndexes.Any())
        {
            result.IndexIssues.Add($"ℹ️ {bitmapIndexes.Count} Oracle BITMAP indexes found (should be converted to GIN in PostgreSQL)");
        }
    }
    
    private void CompareCodeObjects(ComparisonResult result)
    {
        // Sequences
        var oracleSeqNames = new HashSet<string>(
            result.OracleSchema.Sequences.Select(s => s.SequenceName),
            StringComparer.OrdinalIgnoreCase);
        var postgresSeqNames = new HashSet<string>(
            result.PostgresSchema.Sequences.Select(s => s.SequenceName),
            StringComparer.OrdinalIgnoreCase);
        
        var rowidSequences = result.PostgresSchema.Sequences
            .Where(s => s.SequenceName.ToLowerInvariant().Contains("rowid"))
            .ToList();
        
        if (rowidSequences.Any())
        {
            result.DmsRowidSequenceCount = rowidSequences.Count;
            foreach (var seq in rowidSequences)
            {
                result.DmsArtifacts.Add($"ℹ️ [DMS Expected] Sequence '{seq.SequenceName}' (rowid sequence added by DMS)");
            }
        }
        
        var postgresSeqNamesExcludingRowid = result.PostgresSchema.Sequences
            .Where(s => !s.SequenceName.ToLowerInvariant().Contains("rowid"))
            .Select(s => s.SequenceName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        
        var effectivePostgresSeqCount = result.PostgresSchema.SequenceCount - rowidSequences.Count;
        
        if (result.OracleSchema.SequenceCount != effectivePostgresSeqCount)
        {
            const int maxItemsToShow = 5;
            
            var missingSeqs = result.OracleSchema.Sequences
                .Where(s => !postgresSeqNamesExcludingRowid.Contains(s.SequenceName))
                .Select(s => s.SequenceName)
                .ToList();
            
            var extraSeqs = result.PostgresSchema.Sequences
                .Where(s => !oracleSeqNames.Contains(s.SequenceName) && !s.SequenceName.ToLowerInvariant().Contains("rowid"))
                .Select(s => s.SequenceName)
                .ToList();
            
            // Build detailed message with missing/extra sequences
            var detailParts = new List<string>();
            
            if (missingSeqs.Any())
            {
                var seqList = string.Join(", ", missingSeqs.Take(maxItemsToShow));
                var remaining = missingSeqs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Missing in PostgreSQL: {seqList}{suffix}");
            }
            
            if (extraSeqs.Any())
            {
                var seqList = string.Join(", ", extraSeqs.Take(maxItemsToShow));
                var remaining = extraSeqs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Extra in PostgreSQL: {seqList}{suffix}");
            }
            
            var details = detailParts.Any() ? $" | {string.Join(" | ", detailParts)}" : "";
            var rowidNote = rowidSequences.Any() ? $" (excluding {rowidSequences.Count} DMS rowid sequences)" : "";
            result.CodeObjectIssues.Add($"ℹ Sequence count mismatch: Oracle={result.OracleSchema.SequenceCount}, PostgreSQL={effectivePostgresSeqCount}{rowidNote}{details}");
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

        // Compare Procedures separately
        if (result.OracleSchema.ProcedureCount != result.PostgresSchema.ProcedureCount)
        {
            const int maxItemsToShow = 5;
            
            var oracleProcNames = new HashSet<string>(
                result.OracleSchema.Procedures.Where(p => p.Type == ProcedureType.Procedure).Select(p => p.ProcedureName),
                StringComparer.OrdinalIgnoreCase);
            var postgresProcNames = new HashSet<string>(
                result.PostgresSchema.Procedures.Where(p => p.Type == ProcedureType.Procedure).Select(p => p.ProcedureName),
                StringComparer.OrdinalIgnoreCase);
            
            var missingProcs = result.OracleSchema.Procedures
                .Where(p => p.Type == ProcedureType.Procedure && !postgresProcNames.Contains(p.ProcedureName))
                .Select(p => p.ProcedureName)
                .ToList();
            
            var extraProcs = result.PostgresSchema.Procedures
                .Where(p => p.Type == ProcedureType.Procedure && !oracleProcNames.Contains(p.ProcedureName))
                .Select(p => p.ProcedureName)
                .ToList();
            
            var detailParts = new List<string>();
            
            if (missingProcs.Any())
            {
                var procList = string.Join(", ", missingProcs.Take(maxItemsToShow));
                var remaining = missingProcs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Missing in PostgreSQL: {procList}{suffix}");
            }
            
            if (extraProcs.Any())
            {
                var procList = string.Join(", ", extraProcs.Take(maxItemsToShow));
                var remaining = extraProcs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Extra in PostgreSQL: {procList}{suffix}");
            }
            
            var details = detailParts.Any() ? $" | {string.Join(" | ", detailParts)}" : "";
            result.CodeObjectIssues.Add($"ℹ Procedure count mismatch: Oracle={result.OracleSchema.ProcedureCount}, PostgreSQL={result.PostgresSchema.ProcedureCount}{details}");
        }

        // Compare Functions separately
        if (result.OracleSchema.FunctionCount != result.PostgresSchema.FunctionCount)
        {
            const int maxItemsToShow = 5;
            
            var oracleFuncNames = new HashSet<string>(
                result.OracleSchema.Procedures.Where(p => p.Type == ProcedureType.Function).Select(p => p.ProcedureName),
                StringComparer.OrdinalIgnoreCase);
            var postgresFuncNames = new HashSet<string>(
                result.PostgresSchema.Procedures.Where(p => p.Type == ProcedureType.Function).Select(p => p.ProcedureName),
                StringComparer.OrdinalIgnoreCase);
            
            var missingFuncs = result.OracleSchema.Procedures
                .Where(p => p.Type == ProcedureType.Function && !postgresFuncNames.Contains(p.ProcedureName))
                .Select(p => p.ProcedureName)
                .ToList();
            
            var extraFuncs = result.PostgresSchema.Procedures
                .Where(p => p.Type == ProcedureType.Function && !oracleFuncNames.Contains(p.ProcedureName))
                .Select(p => p.ProcedureName)
                .ToList();
            
            var detailParts = new List<string>();
            
            if (missingFuncs.Any())
            {
                var funcList = string.Join(", ", missingFuncs.Take(maxItemsToShow));
                var remaining = missingFuncs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Missing in PostgreSQL: {funcList}{suffix}");
            }
            
            if (extraFuncs.Any())
            {
                var funcList = string.Join(", ", extraFuncs.Take(maxItemsToShow));
                var remaining = extraFuncs.Count - maxItemsToShow;
                var suffix = remaining > 0 ? $" (and {remaining} more)" : "";
                detailParts.Add($"Extra in PostgreSQL: {funcList}{suffix}");
            }
            
            var details = detailParts.Any() ? $" | {string.Join(" | ", detailParts)}" : "";
            result.CodeObjectIssues.Add($"ℹ Functions (includes triggers) count mismatch: Oracle={result.OracleSchema.FunctionCount}, PostgreSQL={result.PostgresSchema.FunctionCount}{details}");
        }
    }
}

public class ComparisonResult
{
    public SchemaDefinition OracleSchema { get; set; } = new();
    public SchemaDefinition PostgresSchema { get; set; } = new();
    
    public string OracleDatabase { get; set; } = string.Empty;
    public string PostgresDatabase { get; set; } = string.Empty;

    public int OracleLogicalTableCount { get; set; }
    public int PostgresLogicalTableCount { get; set; }
    public int PostgresPhysicalTableCount { get; set; }
    public int OracleLogicalColumnCount { get; set; }
    public int PostgresLogicalColumnCount { get; set; }
    public int OracleLogicalPrimaryKeyCount { get; set; }
    public int PostgresLogicalPrimaryKeyCount { get; set; }
    public int SyntheticPrimaryKeyCount { get; set; }
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
    
    public int DmsRowidColumnCount { get; set; }
    public int DmsRowidSequenceCount { get; set; }
    public int DmsRowidIndexCount { get; set; }
    public List<string> DmsArtifacts { get; set; } = new();
    public int TotalDmsArtifacts => DmsRowidColumnCount + DmsRowidSequenceCount + DmsRowidIndexCount + SyntheticPrimaryKeyCount;
    
    public List<string> TableIssues { get; set; } = new();
    public List<string> ConstraintIssues { get; set; } = new();
    public List<string> IndexIssues { get; set; } = new();
    public List<string> CodeObjectIssues { get; set; } = new();
    
    public List<string> OracleExtractionErrors { get; set; } = new();
    public bool HasOracleExtractionErrors => OracleExtractionErrors.Any();
    
    public int TotalIssues => TableIssues.Count + ConstraintIssues.Count + IndexIssues.Count + CodeObjectIssues.Count;
    
    public bool HasCriticalIssues => TableIssues.Any(i => i.Contains("❌") && !IsDmsArtifactIssue(i)) ||
                                     ConstraintIssues.Any(i => i.Contains("❌") && !IsDmsArtifactIssue(i));
    
    private static bool IsDmsArtifactIssue(string issue)
    {
        var lowerIssue = issue.ToLowerInvariant();
        return lowerIssue.Contains("rowid") || 
               lowerIssue.Contains("dms") ||
               lowerIssue.Contains("synthetic");
    }
    
    public string OverallGrade
    {
        get
        {
            var actualIssues = TotalIssues - DmsArtifacts.Count;
            if (actualIssues <= 0 && !HasCriticalIssues) return "A+";
            if (actualIssues <= 5 && !HasCriticalIssues) return "A";
            if (actualIssues <= 10) return "B+";
            if (actualIssues <= 20) return "B";
            return "C";
        }
    }
}
