using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Comparison;

public record LogicalSchemaSummary(
    Dictionary<string, TableDefinition> LogicalTables,
    int LogicalTableCount,
    int LogicalColumnCount,
    int PhysicalTableCount,
    int PartitionedTableCount,
    int PartitionCount,
    List<string> PartitionDetails
);

public static class PartitionNormalization
{
    public static LogicalSchemaSummary BuildPostgresSummary(SchemaDefinition schema)
    {
        var partitionNames = schema.Partitions
            .SelectMany(p => p.Partitions)
            .Select(p => p.PartitionName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var logicalTables = schema.Tables
            .Where(t => !partitionNames.Contains(t.TableName))
            .ToDictionary(t => t.TableName.ToUpper(), StringComparer.OrdinalIgnoreCase);

        var logicalColumnCount = logicalTables.Values.Sum(t => t.Columns.Count);
        var partitionedTableCount = schema.Partitions.Count;
        var partitionCount = schema.Partitions.Sum(p => p.Partitions.Count);

        var partitionDetails = schema.Partitions
            .OrderBy(p => p.ParentTableName, StringComparer.OrdinalIgnoreCase)
            .Select(p =>
            {
                var strategy = p.Strategy == PartitionStrategy.None ? "UNKNOWN" : p.Strategy.ToString().ToUpperInvariant();
                var columns = p.PartitionColumns.Any() ? string.Join(",", p.PartitionColumns) : "UNKNOWN";
                return $"Partitioned table: {p.ParentTableName} (Strategy={strategy}, Columns={columns}, Partitions={p.Partitions.Count})";
            })
            .ToList();

        return new LogicalSchemaSummary(
            logicalTables,
            logicalTables.Count,
            logicalColumnCount,
            schema.Tables.Count,
            partitionedTableCount,
            partitionCount,
            partitionDetails
        );
    }
}
