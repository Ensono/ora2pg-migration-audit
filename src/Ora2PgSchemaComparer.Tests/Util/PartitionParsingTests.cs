using Ora2PgSchemaComparer.Model;
using Ora2PgSchemaComparer.Util;

namespace Ora2PgSchemaComparer.Tests.Util;

public class PartitionParsingTests
{
    [Theory]
    [InlineData("r", PartitionStrategy.Range)]
    [InlineData("R", PartitionStrategy.Range)]
    [InlineData("l", PartitionStrategy.List)]
    [InlineData("L", PartitionStrategy.List)]
    [InlineData("h", PartitionStrategy.Hash)]
    [InlineData("H", PartitionStrategy.Hash)]
    [InlineData("", PartitionStrategy.None)]
    [InlineData(null, PartitionStrategy.None)]
    public void ParsePostgresStrategy_MapsExpectedValues(string? strategyCode, PartitionStrategy expected)
    {
        var result = PartitionParsing.ParsePostgresStrategy(strategyCode);

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("RANGE", PartitionStrategy.Range)]
    [InlineData("range", PartitionStrategy.Range)]
    [InlineData("LIST", PartitionStrategy.List)]
    [InlineData("list", PartitionStrategy.List)]
    [InlineData("HASH", PartitionStrategy.Hash)]
    [InlineData("hash", PartitionStrategy.Hash)]
    [InlineData("", PartitionStrategy.None)]
    [InlineData(null, PartitionStrategy.None)]
    public void ParseOracleStrategy_MapsExpectedValues(string? strategyName, PartitionStrategy expected)
    {
        var result = PartitionParsing.ParseOracleStrategy(strategyName);

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("RANGE (id)", new[] { "id" })]
    [InlineData("LIST (region, sub_region)", new[] { "region", "sub_region" })]
    [InlineData("HASH (tenant_id)", new[] { "tenant_id" })]
    [InlineData("", new string[0])]
    [InlineData(null, new string[0])]
    [InlineData("RANGE", new string[0])]
    public void ParsePartitionColumns_ReturnsExpectedColumns(string? definition, string[] expected)
    {
        var result = PartitionParsing.ParsePartitionColumns(definition);

        Assert.Equal(expected, result);
    }
}
