using Ora2PgSchemaComparer.Comparison;
using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Tests.Comparison;

public class SchemaComparatorTests
{
    [Fact]
    public void Compare_FiltersPartitionChildObjectsFromLogicalCounts()
    {
        var oracleSchema = new SchemaDefinition
        {
            SchemaName = "OSBPTFL",
            DatabaseType = "Oracle",
            Tables = new List<TableDefinition>
            {
                new()
                {
                    TableName = "ASSET_REL",
                    Columns = new List<ColumnDefinition>
                    {
                        new() { ColumnName = "ID", ColumnPosition = 1 }
                    }
                }
            },
            Constraints = new List<ConstraintDefinition>
            {
                new()
                {
                    ConstraintName = "PK_ASSET_REL",
                    TableName = "ASSET_REL",
                    Type = ConstraintType.PrimaryKey,
                    Columns = new List<string> { "ID" }
                }
            },
            Indexes = new List<IndexDefinition>
            {
                new()
                {
                    IndexName = "IDX_ASSET_REL_ID",
                    TableName = "ASSET_REL",
                    Type = IndexType.BTree
                }
            }
        };

        var postgresSchema = new SchemaDefinition
        {
            SchemaName = "osbptfl",
            DatabaseType = "PostgreSQL",
            Tables = new List<TableDefinition>
            {
                new()
                {
                    TableName = "asset_rel",
                    Columns = new List<ColumnDefinition>
                    {
                        new() { ColumnName = "id", ColumnPosition = 1 }
                    }
                },
                new()
                {
                    TableName = "asset_rel_asrel_part1",
                    Columns = new List<ColumnDefinition>
                    {
                        new() { ColumnName = "id", ColumnPosition = 1 }
                    }
                }
            },
            Partitions = new List<PartitionMetadata>
            {
                new()
                {
                    ParentTableName = "asset_rel",
                    Strategy = PartitionStrategy.Hash,
                    PartitionColumns = new List<string> { "id" },
                    Partitions = new List<PartitionDefinition>
                    {
                        new() { PartitionName = "asset_rel_asrel_part1" }
                    }
                }
            },
            Constraints = new List<ConstraintDefinition>
            {
                new()
                {
                    ConstraintName = "pk_asset_rel",
                    TableName = "asset_rel",
                    Type = ConstraintType.PrimaryKey,
                    Columns = new List<string> { "id" }
                },
                new()
                {
                    ConstraintName = "pk_asset_rel_asrel_part1",
                    TableName = "asset_rel_asrel_part1",
                    Type = ConstraintType.PrimaryKey,
                    Columns = new List<string> { "id" }
                }
            },
            Indexes = new List<IndexDefinition>
            {
                new()
                {
                    IndexName = "idx_asset_rel_id",
                    TableName = "asset_rel",
                    Type = IndexType.BTree
                },
                new()
                {
                    IndexName = "idx_asset_rel_asrel_part1_id",
                    TableName = "asset_rel_asrel_part1",
                    Type = IndexType.BTree
                }
            }
        };

        var comparator = new SchemaComparator();
        var result = comparator.Compare(oracleSchema, postgresSchema);

        Assert.Equal(1, result.PostgresLogicalTableCount);
        Assert.Equal(1, result.PostgresLogicalPrimaryKeyCount);
        Assert.Equal(1, result.PostgresLogicalIndexCount);

        Assert.DoesNotContain(result.TableIssues, issue => issue.Contains("Extra table in PostgreSQL: asset_rel_asrel_part1", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(result.ConstraintIssues, issue => issue.Contains("Primary key count mismatch", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(result.IndexIssues, issue => issue.Contains("Index count mismatch", StringComparison.OrdinalIgnoreCase));
    }
}
