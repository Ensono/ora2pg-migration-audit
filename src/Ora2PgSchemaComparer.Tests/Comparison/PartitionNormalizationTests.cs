using Ora2PgSchemaComparer.Comparison;
using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Tests.Comparison;

public class PartitionNormalizationTests
{
    [Fact]
    public void BuildPostgresSummary_ExcludesPartitionChildrenFromLogicalTables()
    {
        var schema = new SchemaDefinition
        {
            SchemaName = "osbptfl",
            DatabaseType = "PostgreSQL",
            Tables = new List<TableDefinition>
            {
                new TableDefinition { TableName = "asset", Columns = new List<ColumnDefinition> { new() { ColumnName = "id" } } },
                new TableDefinition { TableName = "asset_sys_p1", Columns = new List<ColumnDefinition> { new() { ColumnName = "id" } } },
                new TableDefinition { TableName = "organization", Columns = new List<ColumnDefinition> { new() { ColumnName = "id" }, new() { ColumnName = "name" } } }
            },
            Partitions = new List<PartitionMetadata>
            {
                new PartitionMetadata
                {
                    ParentTableName = "asset",
                    Strategy = PartitionStrategy.Range,
                    PartitionColumns = new List<string> { "id" },
                    Partitions = new List<PartitionDefinition>
                    {
                        new PartitionDefinition { PartitionName = "asset_sys_p1" }
                    }
                }
            }
        };

        var summary = PartitionNormalization.BuildPostgresSummary(schema);

        Assert.Equal(2, summary.LogicalTableCount);
        Assert.Equal(3, summary.PhysicalTableCount);
        Assert.Equal(1, summary.PartitionedTableCount);
        Assert.Equal(1, summary.PartitionCount);
        Assert.Equal(3, summary.LogicalColumnCount);
        Assert.True(summary.LogicalTables.ContainsKey("ASSET"));
        Assert.True(summary.LogicalTables.ContainsKey("ORGANIZATION"));
        Assert.False(summary.LogicalTables.ContainsKey("ASSET_SYS_P1"));
    }
}
