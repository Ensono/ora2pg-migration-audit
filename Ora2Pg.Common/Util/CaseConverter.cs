namespace Ora2Pg.Common.Util;

using Ora2Pg.Common.Connection;


public static class CaseConverter
{

    public static string NormalizeSchemaName(string schema, DatabaseType dbType)
    {
        return dbType == DatabaseType.Oracle 
            ? schema.ToUpper() 
            : schema.ToLower();
    }


    public static string NormalizeTableName(string table, DatabaseType dbType)
    {
        return dbType == DatabaseType.Oracle 
            ? table.ToUpper() 
            : table.ToLower();
    }


    public static string NormalizeTableReference(string tableRef, DatabaseType dbType)
    {
        if (!tableRef.Contains('.'))
        {
            throw new ArgumentException($"Table reference must be in format 'schema.table', got: {tableRef}");
        }

        var parts = tableRef.Split('.');
        string schema = NormalizeSchemaName(parts[0], dbType);
        string table = NormalizeTableName(parts[1], dbType);
        
        return $"{schema}.{table}";
    }


    public static Dictionary<string, string> ParseAndNormalizeMapping(string mappingsStr)
    {
        var result = new Dictionary<string, string>();

        if (string.IsNullOrWhiteSpace(mappingsStr))
        {
            return result;
        }


        var mappings = mappingsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        foreach (var mapping in mappings)
        {
            var parsed = ParseSingleMapping(mapping);
            if (parsed != null)
            {
                result[parsed.OracleTable] = parsed.PostgresTable;
            }
        }

        return result;
    }


    private static TableMapping? ParseSingleMapping(string mapping)
    {
        if (string.IsNullOrWhiteSpace(mapping))
        {
            return null;
        }


        if (mapping.Contains('='))
        {
            var parts = mapping.Split('=', 2);
            if (parts.Length != 2)
            {
                return null;
            }

            string oracleTable = NormalizeTableReference(parts[0].Trim(), DatabaseType.Oracle);
            string postgresTable = NormalizeTableReference(parts[1].Trim(), DatabaseType.PostgreSQL);
            
            return new TableMapping(oracleTable, postgresTable);
        }
        else
        {

            string oracleTable = NormalizeTableReference(mapping.Trim(), DatabaseType.Oracle);
            string postgresTable = NormalizeTableReference(mapping.Trim(), DatabaseType.PostgreSQL);
            
            return new TableMapping(oracleTable, postgresTable);
        }
    }

    public record TableMapping(string OracleTable, string PostgresTable);
}
