using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Util;

public static class PartitionParsing
{
    public static PartitionStrategy ParsePostgresStrategy(string? strategyCode)
    {
        if (string.IsNullOrWhiteSpace(strategyCode))
        {
            return PartitionStrategy.None;
        }

        return strategyCode.Trim().ToLowerInvariant() switch
        {
            "r" => PartitionStrategy.Range,
            "l" => PartitionStrategy.List,
            _ => PartitionStrategy.None
        };
    }

    public static PartitionStrategy ParseOracleStrategy(string? strategyName)
    {
        if (string.IsNullOrWhiteSpace(strategyName))
        {
            return PartitionStrategy.None;
        }

        return strategyName.Trim().ToUpperInvariant() switch
        {
            "RANGE" => PartitionStrategy.Range,
            "LIST" => PartitionStrategy.List,
            _ => PartitionStrategy.None
        };
    }

    public static List<string> ParsePartitionColumns(string? partitionKeyDefinition)
    {
        if (string.IsNullOrWhiteSpace(partitionKeyDefinition))
        {
            return new List<string>();
        }

        var openParen = partitionKeyDefinition.IndexOf('(');
        var closeParen = partitionKeyDefinition.LastIndexOf(')');
        if (openParen < 0 || closeParen <= openParen)
        {
            return new List<string>();
        }

        var inside = partitionKeyDefinition.Substring(openParen + 1, closeParen - openParen - 1);
        return inside.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();
    }
}
