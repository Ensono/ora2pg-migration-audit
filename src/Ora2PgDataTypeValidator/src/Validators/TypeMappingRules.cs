using Ora2PgDataTypeValidator.Models;

namespace Ora2PgDataTypeValidator.Validators;


public static class TypeMappingRules
{
    public static TypeMappingResult GetExpectedMapping(ColumnMetadata oracleColumn)
    {
        var oracleType = oracleColumn.DataType.ToUpper();
        var precision = oracleColumn.DataPrecision;
        var scale = oracleColumn.DataScale;
        var length = oracleColumn.CharLength;

        return oracleType switch
        {
            // Numeric Types - Based on actual DMS conversion patterns
            "NUMBER" when scale == 0 && precision is >= 1 and <= 4 =>
                new TypeMappingResult("smallint", null, null, $"DMS maps NUMBER({precision},0) to SMALLINT for values 1-4 digits"),
            
            "NUMBER" when scale == 0 && precision is >= 5 and <= 9 =>
                new TypeMappingResult("integer", null, null, $"DMS maps NUMBER({precision},0) to INTEGER for values 5-9 digits"),
            
            "NUMBER" when scale == 0 && precision >= 10 =>
                new TypeMappingResult("bigint", null, null, $"DMS maps NUMBER({precision},0) to BIGINT for values 10+ digits"),
            
            "NUMBER" when scale == 0 && precision is null => 
                new TypeMappingResult("bigint", null, null, "DMS maps NUMBER with no precision to BIGINT"),
            
            "NUMBER" when scale > 0 =>
                new TypeMappingResult("numeric", precision, scale, $"DMS maps NUMBER({precision},{scale}) to DECIMAL({precision},{scale})"),
            
            "NUMBER" when scale == null && precision == null =>
                new TypeMappingResult("numeric", null, null, "DMS maps NUMBER with no precision/scale to DECIMAL (stored as NUMERIC in PostgreSQL)"),
            
            "INTEGER" or "INT" => 
                new TypeMappingResult("integer", null, null, "INTEGER maps to INTEGER"),
            
            "SMALLINT" => 
                new TypeMappingResult("smallint", null, null, "SMALLINT maps to SMALLINT"),
            
            "FLOAT" => 
                new TypeMappingResult("double precision", null, null, "FLOAT maps to DOUBLE PRECISION"),
            
            "BINARY_FLOAT" => 
                new TypeMappingResult("real", null, null, "BINARY_FLOAT maps to REAL"),
            
            "BINARY_DOUBLE" => 
                new TypeMappingResult("double precision", null, null, "BINARY_DOUBLE maps to DOUBLE PRECISION"),

            // String Types - VARCHAR2 maps to VARCHAR (character varying), not TEXT
            "VARCHAR2" =>
                new TypeMappingResult("character varying", length, null, $"DMS maps VARCHAR2({length} BYTE) to VARCHAR({length})"),
            
            "VARCHAR" => 
                new TypeMappingResult("character varying", length, null, $"VARCHAR({length}) maps to VARCHAR({length})"),
            
            "CHAR" => 
                new TypeMappingResult("character", length, null, $"CHAR({length}) maps to CHAR({length})"),
            
            "NCHAR" => 
                new TypeMappingResult("character", length, null, $"NCHAR({length}) maps to CHAR({length})"),
            
            "NVARCHAR2" => 
                new TypeMappingResult("character varying", length, null, $"NVARCHAR2({length}) maps to VARCHAR({length})"),
            
            "CLOB" => 
                new TypeMappingResult("text", null, null, "CLOB maps to TEXT"),
            
            "NCLOB" => 
                new TypeMappingResult("text", null, null, "NCLOB maps to TEXT"),
            
            "LONG" => 
                new TypeMappingResult("text", null, null, "LONG (deprecated) maps to TEXT"),

            // Date/Time Types - Oracle DATE maps to TIMESTAMP (not "timestamp without time zone")
            "DATE" =>
                new TypeMappingResult("timestamp", null, null, "DMS maps Oracle DATE (includes time) to PostgreSQL TIMESTAMP"),
            
            "TIMESTAMP" => 
                new TypeMappingResult("timestamp", null, null, "TIMESTAMP maps to TIMESTAMP WITHOUT TIME ZONE"),
            
            "TIMESTAMP WITH TIME ZONE" or "TIMESTAMP WITH TIMEZONE" => 
                new TypeMappingResult("timestamp with time zone", null, null, "TIMESTAMP WITH TIME ZONE maps to TIMESTAMPTZ"),
            
            "TIMESTAMP WITH LOCAL TIME ZONE" => 
                new TypeMappingResult("timestamp with time zone", null, null, "TIMESTAMP WITH LOCAL TIME ZONE maps to TIMESTAMPTZ"),
            
            "INTERVAL YEAR TO MONTH" => 
                new TypeMappingResult("interval", null, null, "INTERVAL YEAR TO MONTH maps to INTERVAL"),
            
            "INTERVAL DAY TO SECOND" => 
                new TypeMappingResult("interval", null, null, "INTERVAL DAY TO SECOND maps to INTERVAL"),

            // Binary Types
            "BLOB" =>
                new TypeMappingResult("bytea", null, null, "DMS maps BLOB to BYTEA"),
            
            "RAW" => 
                new TypeMappingResult("bytea", null, null, "DMS maps RAW to BYTEA"),
            
            "LONG RAW" => 
                new TypeMappingResult("bytea", null, null, "LONG RAW (deprecated) maps to BYTEA"),
            
            "BFILE" => 
                new TypeMappingResult("text", null, null, "BFILE maps to TEXT (file path)"),

            // XML/JSON Types
            "XMLTYPE" => 
                new TypeMappingResult("xml", null, null, "XMLTYPE maps to XML"),
            
            // ROWID Types (often added by DMS)
            "ROWID" or "UROWID" => 
                new TypeMappingResult("character varying", 18, null, "ROWID maps to VARCHAR(18)"),
            
            _ => new TypeMappingResult(null, null, null, $"Unknown Oracle type: {oracleType}")
        };
    }

    public static bool IsCompatibleMapping(
        ColumnMetadata oracleColumn, 
        ColumnMetadata postgresColumn,
        out string mismatchReason)
    {
        var expected = GetExpectedMapping(oracleColumn);
        var actualType = postgresColumn.DataType.ToLower();
        
        mismatchReason = string.Empty;

        if (expected.ExpectedType == null)
        {
            mismatchReason = expected.MappingDescription;
            return false;
        }

        var normalizedActual = NormalizePostgresType(actualType);
        var normalizedExpected = NormalizePostgresType(expected.ExpectedType);

        if (!TypesAreCompatible(normalizedExpected, normalizedActual))
        {
            mismatchReason = $"Expected {expected.ExpectedType.ToUpper()}, got {actualType.ToUpper()}";
            return false;
        }

        if (expected.ExpectedPrecision.HasValue || expected.ExpectedScale.HasValue)
        {
            if (expected.ExpectedPrecision != postgresColumn.DataPrecision)
            {
                mismatchReason = $"Precision mismatch: expected {expected.ExpectedPrecision}, got {postgresColumn.DataPrecision}";
                return false;
            }
            
            if (expected.ExpectedScale.HasValue && expected.ExpectedScale != postgresColumn.DataScale)
            {
                mismatchReason = $"Scale mismatch: expected {expected.ExpectedScale}, got {postgresColumn.DataScale}";
                return false;
            }
        }

        if (normalizedExpected.Contains("varying") || normalizedExpected == "character")
        {
            if (expected.ExpectedLength.HasValue && expected.ExpectedLength != postgresColumn.CharLength)
            {
                mismatchReason = $"Length mismatch: expected {expected.ExpectedLength}, got {postgresColumn.CharLength}";
                return false;
            }
        }

        return true;
    }

    private static string NormalizePostgresType(string type)
    {
        type = type.ToLower().Trim();
        
        var baseType = type.Split('(')[0].Trim();
        var suffix = type.Contains('(') ? type.Substring(type.IndexOf('(')) : "";
        
        var normalized = baseType switch
        {
            "varchar" => "character varying",
            "char" when !baseType.Contains("character") => "character",
            "int" => "integer",
            "int2" => "smallint",
            "int4" => "integer",
            "int8" => "bigint",
            "float8" => "double precision",
            "float4" => "real",
            "double" => "double precision",  // DOUBLE is an alias for DOUBLE PRECISION
            "bool" => "boolean",
            "timestamptz" => "timestamp with time zone",
            "decimal" => "numeric",  // DECIMAL is an alias for NUMERIC
            _ => baseType
        };
        
        return normalized + suffix;
    }

    private static bool TypesAreCompatible(string expected, string actual)
    {
        if (expected == actual) return true;

        var expectedBase = expected.Split('(')[0].Trim();
        var actualBase = actual.Split('(')[0].Trim();
        
        if (expectedBase == actualBase) return true;
        
        if (expectedBase == "character varying" && actualBase.StartsWith("character varying"))
            return true;
        
        if (expectedBase == "character" && actualBase.StartsWith("character") && !actualBase.Contains("varying"))
            return true;
        
        if (expectedBase == "numeric" && (actualBase.StartsWith("numeric") || actualBase.StartsWith("decimal")))
            return true;
        
        // TIMESTAMP compatibility - DMS uses just "timestamp", not "timestamp without time zone"
        if (expectedBase == "timestamp" && (actualBase == "timestamp without time zone" || actualBase.StartsWith("timestamp")))
            return true;
        
        if (expectedBase == "timestamp without time zone" && (actualBase == "timestamp" || actualBase.StartsWith("timestamp without")))
            return true;
        
        if (expectedBase == "timestamp with time zone" && (actualBase == "timestamptz" || actualBase.StartsWith("timestamp with")))
            return true;

        return false;
    }
}

public class TypeMappingResult
{
    public string? ExpectedType { get; set; }
    public int? ExpectedPrecision { get; set; }
    public int? ExpectedScale { get; set; }
    public int? ExpectedLength { get; set; }
    public string MappingDescription { get; set; }

    public TypeMappingResult(string? expectedType, int? precisionOrLength, int? scale, string description)
    {
        ExpectedType = expectedType;
        MappingDescription = description;
        
        // For numeric types, treat as precision/scale
        if (expectedType?.Contains("numeric") == true || 
            expectedType?.Contains("decimal") == true ||
            expectedType?.Contains("int") == true)
        {
            ExpectedPrecision = precisionOrLength;
            ExpectedScale = scale;
        }
        // For character types, treat as length
        else if (expectedType?.Contains("character") == true || 
                 expectedType?.Contains("varchar") == true)
        {
            ExpectedLength = precisionOrLength;
        }
    }
}
