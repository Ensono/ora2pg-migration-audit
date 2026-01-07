using Ora2PgDataTypeValidator.Models;
using Serilog;

namespace Ora2PgDataTypeValidator.Validators;

public class DataTypeValidator
{
    private readonly List<ValidationIssue> _issues = new();

    public ValidationResult Validate(
        List<ColumnMetadata> oracleColumns,
        List<ColumnMetadata> postgresColumns)
    {
        _issues.Clear();
        
        Log.Information("🔍 Starting data type validation...");

        var oracleDict = oracleColumns
            .GroupBy(c => $"{c.TableName.ToUpper()}.{c.ColumnName.ToUpper()}")
            .ToDictionary(g => g.Key, g => g.First());

        var postgresDict = postgresColumns
            .GroupBy(c => $"{c.TableName.ToUpper()}.{c.ColumnName.ToUpper()}")
            .ToDictionary(g => g.Key, g => g.First());

        int validated = 0;
        foreach (var kvp in oracleDict)
        {
            if (postgresDict.TryGetValue(kvp.Key, out var pgColumn))
            {
                ValidateColumnMapping(kvp.Value, pgColumn);
                validated++;
            }
        }

        Log.Information($"✅ Validated {validated} column mappings");

        return new ValidationResult
        {
            OracleSchema = oracleColumns.FirstOrDefault()?.SchemaName ?? "UNKNOWN",
            PostgresSchema = postgresColumns.FirstOrDefault()?.SchemaName ?? "unknown",
            Issues = _issues,
            TotalColumnsValidated = validated
        };
    }

    private void ValidateColumnMapping(ColumnMetadata oracle, ColumnMetadata postgres)
    {
        var oracleType = oracle.DataType.ToUpper();
        var postgresType = postgres.DataType.ToLower();

        ValidateNumericTypes(oracle, postgres, oracleType, postgresType);

        ValidateStringTypes(oracle, postgres, oracleType, postgresType);

        ValidateDateTimeTypes(oracle, postgres, oracleType, postgresType);

        ValidateBinaryTypes(oracle, postgres, oracleType, postgresType);

        ValidateBooleanTypes(oracle, postgres, oracleType, postgresType);

        ValidateAdvancedOracleTypes(oracle, postgres, oracleType, postgresType);

        ValidateEmptyStringHandling(oracle, postgres, oracleType, postgresType);
    }

    #region Numeric Type Validations

    private void ValidateNumericTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType == "NUMBER" && oracle.DataScale == 0)
        {
            ValidateNumberPrecisionZero(oracle, postgres, postgresType);
        }

        if (oracleType == "NUMBER" && oracle.DataPrecision.HasValue && oracle.DataScale.HasValue)
        {
            ValidateNumberWithPrecisionScale(oracle, postgres, postgresType);
        }

        if (oracleType is "FLOAT" or "BINARY_DOUBLE")
        {
            ValidateFloatTypes(oracle, postgres, oracleType, postgresType);
        }

        if (oracle.DefaultValue?.Contains("GENERATED") == true || 
            oracle.DefaultValue?.Contains("IDENTITY") == true)
        {
            ValidateIdentitySequence(oracle, postgres);
        }
    }

    private void ValidateNumberPrecisionZero(ColumnMetadata oracle, ColumnMetadata postgres, string postgresType)
    {
        var precision = oracle.DataPrecision ?? 38;

        if (precision > 9 && postgresType == "integer")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Critical, "Numeric Overflow Risk",
                $"NUMBER({precision},0) mapped to INTEGER but can overflow. Values > 2 billion will fail.",
                "Change to BIGINT for values > 2,147,483,647");
        }
        else if (precision <= 9 && postgresType == "bigint")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Info, "Storage Optimization",
                $"NUMBER({precision},0) uses BIGINT but INTEGER would suffice (8 bytes vs 4 bytes).",
                "Consider using INTEGER for better storage efficiency");
        }
        else if (!new[] { "integer", "bigint", "numeric", "smallint" }.Contains(postgresType))
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Invalid Mapping",
                $"NUMBER({precision},0) mapped to {postgresType.ToUpper()} instead of INTEGER/BIGINT.",
                "Use INTEGER (p≤9) or BIGINT (p>9) for whole numbers");
        }
    }

    private void ValidateNumberWithPrecisionScale(ColumnMetadata oracle, ColumnMetadata postgres, string postgresType)
    {
        var expectedType = $"numeric({oracle.DataPrecision},{oracle.DataScale})";
        
        if (!postgresType.StartsWith("numeric"))
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Precision/Scale Mismatch",
                $"NUMBER({oracle.DataPrecision},{oracle.DataScale}) should map to NUMERIC, not {postgresType.ToUpper()}.",
                $"Use NUMERIC({oracle.DataPrecision},{oracle.DataScale}) to preserve exact decimal values");
        }
        else
        {
            if (postgres.DataPrecision != oracle.DataPrecision || 
                postgres.DataScale != oracle.DataScale)
            {
                AddIssue(oracle, postgres, ValidationSeverity.Critical, "Precision/Scale Mismatch",
                    $"Precision/scale mismatch: Oracle NUMBER({oracle.DataPrecision},{oracle.DataScale}) " +
                    $"vs PostgreSQL NUMERIC({postgres.DataPrecision},{postgres.DataScale}). Money fields MUST match exactly.",
                    $"Change to NUMERIC({oracle.DataPrecision},{oracle.DataScale})");
            }
        }
    }

    private void ValidateFloatTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (postgresType != "double precision")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Float Type Mismatch",
                $"{oracleType} should map to DOUBLE PRECISION, not {postgresType.ToUpper()}.",
                "Use DOUBLE PRECISION for floating-point numbers");
        }
        else
        {
            AddIssue(oracle, postgres, ValidationSeverity.Warning, "Rounding Errors",
                $"{oracleType} -> DOUBLE PRECISION may introduce rounding errors in scientific calculations.",
                "Validate that precision loss is acceptable for your use case");
        }
    }

    private void ValidateIdentitySequence(ColumnMetadata oracle, ColumnMetadata postgres)
    {
        var postgresType = postgres.DataType.ToLower();
        var hasSerial = postgresType.Contains("serial") || postgresType.Contains("generated");
        var hasDefault = postgres.DefaultValue?.Contains("nextval") == true;

        if (!hasSerial && !hasDefault)
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Auto-Increment Missing",
                "Oracle IDENTITY/SEQUENCE not mapped to PostgreSQL SERIAL or GENERATED ALWAYS.",
                "Use SERIAL, BIGSERIAL, or GENERATED ALWAYS AS IDENTITY");
        }
        else
        {
            AddIssue(oracle, postgres, ValidationSeverity.Info, "Auto-Increment OK",
                "Auto-increment behavior verified.",
                null);
        }
    }

    #endregion

    #region String Type Validations

    private void ValidateStringTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType == "VARCHAR2")
        {
            ValidateVarchar2(oracle, postgres, postgresType);
        }

        if (oracleType == "CHAR")
        {
            ValidateChar(oracle, postgres, postgresType);
        }

        if (oracleType == "CLOB")
        {
            ValidateClob(oracle, postgres, postgresType);
        }
    }

    private void ValidateVarchar2(ColumnMetadata oracle, ColumnMetadata postgres, string postgresType)
    {
        if (!postgresType.StartsWith("character varying") && postgresType != "varchar" && postgresType != "text")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "String Type Mismatch",
                $"VARCHAR2 should map to VARCHAR(n) or TEXT, not {postgresType.ToUpper()}.",
                "Use CHARACTER VARYING(n) or TEXT");
        }

        if (oracle.CharLength.HasValue && postgres.CharLength.HasValue)
        {
            AddIssue(oracle, postgres, ValidationSeverity.Warning, "Character Encoding",
                $"Oracle VARCHAR2 length is in BYTES, PostgreSQL is in CHARACTERS. " +
                $"Multi-byte characters (emojis, accents) may not fit if Oracle byte length = Postgres char length.",
                "Verify character encoding compatibility for international data");
        }
    }

    private void ValidateChar(ColumnMetadata oracle, ColumnMetadata postgres, string postgresType)
    {
        if (postgresType.StartsWith("character varying") || postgresType == "varchar")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Warning, "Padding Behavior",
                $"CHAR({oracle.CharLength}) mapped to VARCHAR - padding behavior differs. " +
                "Oracle pads with spaces; PostgreSQL doesn't.",
                "Verify ETL logic handles space trimming correctly");
        }
        else if (!postgresType.StartsWith("character") && postgresType != "char")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Fixed-Length Type Mismatch",
                $"CHAR should map to CHAR(n), not {postgresType.ToUpper()}.",
                $"Use CHAR({oracle.CharLength})");
        }
    }

    private void ValidateClob(ColumnMetadata oracle, ColumnMetadata postgres, string postgresType)
    {
        if (postgresType != "text")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Error, "Large Text Type Mismatch",
                $"CLOB should map to TEXT, not {postgresType.ToUpper()}.",
                "Use TEXT for large text blocks");
        }

        if (postgres.CharLength.HasValue && postgres.CharLength < 4000)
        {
            AddIssue(oracle, postgres, ValidationSeverity.Critical, "Text Truncation Risk",
                $"CLOB mapped to {postgresType.ToUpper()}({postgres.CharLength}) - text blocks >4000 chars will truncate!",
                "Remove length limit or use TEXT type");
        }
    }

    #endregion

    #region Date/Time Type Validations

    private void ValidateDateTimeTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType == "DATE")
        {
            if (postgresType == "date" && !postgresType.Contains("timestamp"))
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Time Data Loss",
                    "Oracle DATE contains time (HH:MM:SS). PostgreSQL DATE does not. Time data will be lost!",
                    "Use TIMESTAMP or TIMESTAMPTZ if time component is needed");
            }
            else if (postgresType == "timestamp without time zone" || postgresType == "timestamp")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Info, "Date Mapping OK",
                    "Oracle DATE correctly mapped to TIMESTAMP.",
                    null);
            }
        }

        if (oracleType == "TIMESTAMP")
        {
            if (postgresType != "timestamp without time zone" && postgresType != "timestamp")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Timestamp Type Mismatch",
                    $"TIMESTAMP should map to TIMESTAMP WITHOUT TIME ZONE, not {postgresType.ToUpper()}.",
                    "Use TIMESTAMP or TIMESTAMP WITHOUT TIME ZONE");
            }
        }

        if (oracleType == "TIMESTAMP WITH TIME ZONE")
        {
            if (postgresType != "timestamp with time zone" && postgresType != "timestamptz")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Timezone Type Mismatch",
                    $"TIMESTAMP WITH TIME ZONE should map to TIMESTAMPTZ, not {postgresType.ToUpper()}.",
                    "Use TIMESTAMP WITH TIME ZONE or TIMESTAMPTZ");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "UTC Conversion",
                    "Verify UTC conversions are correct for TIMESTAMPTZ.",
                    "Test timezone-sensitive queries");
            }
        }
    }

    #endregion

    #region Binary Type Validations

    private void ValidateBinaryTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType is "BLOB" or "RAW")
        {
            if (postgresType != "bytea")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Binary Type Mismatch",
                    $"{oracleType} should map to BYTEA, not {postgresType.ToUpper()}.",
                    "Use BYTEA for binary data");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Binary File Validation",
                    $"{oracleType} -> BYTEA: Verify binary files (PDFs, images) can be retrieved and opened correctly.",
                    "Test file integrity after migration");
            }
        }
    }

    #endregion

    #region Boolean Type Validations

    private void ValidateBooleanTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if ((oracleType == "NUMBER" && oracle.DataPrecision == 1) || 
            (oracleType == "CHAR" && oracle.CharLength == 1))
        {
            if (postgresType == "boolean")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Boolean Conversion",
                    $"Oracle {oracleType}(1) mapped to BOOLEAN. Verify application handles TRUE/FALSE vs 0/1 or Y/N conversion.",
                    "Test boolean logic in application code");
            }
        }
    }

    #endregion

    #region Advanced Oracle Type Validations

    private void ValidateAdvancedOracleTypes(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType == "LONG")
        {
            if (postgresType != "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Legacy LONG Type",
                    "Oracle LONG (deprecated) should map to TEXT in PostgreSQL.",
                    "Use TEXT type. Note: LONG limited to 2GB in Oracle");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Deprecated Type",
                    "LONG is deprecated in Oracle. Consider migrating to CLOB/TEXT.",
                    "Review if this column can be upgraded to CLOB in source");
            }
        }

        if (oracleType == "LONG RAW")
        {
            if (postgresType != "bytea")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Legacy LONG RAW Type",
                    "Oracle LONG RAW (deprecated) should map to BYTEA in PostgreSQL.",
                    "Use BYTEA type. Note: LONG RAW limited to 2GB in Oracle");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Deprecated Binary Type",
                    "LONG RAW is deprecated in Oracle. Consider migrating to BLOB/BYTEA.",
                    "Review if this column can be upgraded to BLOB in source");
            }
        }

        if (oracleType.StartsWith("RAW"))
        {
            if (postgresType != "bytea")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Fixed Binary Type",
                    $"Oracle {oracleType} should map to BYTEA, not {postgresType.ToUpper()}.",
                    "Use BYTEA for fixed-length binary data");
            }
        }

        if (oracleType == "XMLTYPE")
        {
            if (postgresType != "xml" && postgresType != "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "XML Type Mismatch",
                    "Oracle XMLTYPE should map to PostgreSQL XML or TEXT.",
                    "Use XML type for native XML support, or TEXT if XML parsing not needed");
            }
            else if (postgresType == "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "XML Features Lost",
                    "XMLTYPE mapped to TEXT - XML query/validation features lost.",
                    "Consider using PostgreSQL XML type for XPath/XQuery support");
            }
        }

        if (oracleType == "JSON" || oracleType.Contains("JSON"))
        {
            if (postgresType != "jsonb" && postgresType != "json")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "JSON Type Mismatch",
                    "Oracle JSON should map to PostgreSQL JSONB or JSON.",
                    "Use JSONB for better query performance and indexing");
            }
            else if (postgresType == "json")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Info, "JSONB Recommended",
                    "JSON type mapped correctly, but JSONB offers better performance.",
                    "Consider using JSONB for improved indexing and query speed");
            }
        }

        if (oracleType == "NVARCHAR2")
        {
            if (!postgresType.StartsWith("character varying") && postgresType != "varchar" && postgresType != "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "National String Type",
                    "Oracle NVARCHAR2 should map to VARCHAR or TEXT in PostgreSQL.",
                    "Use CHARACTER VARYING or TEXT. PostgreSQL uses UTF-8 by default");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Info, "Unicode Support",
                    "NVARCHAR2 -> VARCHAR: PostgreSQL uses UTF-8 natively, no special type needed.",
                    null);
            }
        }

        if (oracleType == "NCHAR")
        {
            if (!postgresType.StartsWith("character") && postgresType != "char")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "National Fixed String",
                    "Oracle NCHAR should map to CHAR in PostgreSQL.",
                    "Use CHAR type. PostgreSQL uses UTF-8 by default");
            }
        }

        if (oracleType == "NCLOB")
        {
            if (postgresType != "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "National Large Text",
                    "Oracle NCLOB should map to TEXT in PostgreSQL.",
                    "Use TEXT type. PostgreSQL uses UTF-8 by default");
            }
        }

        if (oracleType == "BINARY_FLOAT")
        {
            if (postgresType != "real")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Single Precision Float",
                    "Oracle BINARY_FLOAT should map to REAL (4 bytes), not {postgresType.ToUpper()}.",
                    "Use REAL for 4-byte floating point");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "Float Precision",
                    "BINARY_FLOAT -> REAL: Both are 4-byte floats, but rounding may differ slightly.",
                    "Validate precision-sensitive calculations");
            }
        }

        if (oracleType == "BINARY_DOUBLE")
        {
            if (postgresType != "double precision")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Double Precision Float",
                    "Oracle BINARY_DOUBLE should map to DOUBLE PRECISION, not {postgresType.ToUpper()}.",
                    "Use DOUBLE PRECISION for 8-byte floating point");
            }
        }

        if (oracleType.StartsWith("INTERVAL YEAR"))
        {
            if (postgresType != "interval")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Interval Type",
                    "Oracle INTERVAL YEAR TO MONTH should map to PostgreSQL INTERVAL.",
                    "Use INTERVAL type for date/time durations");
            }
        }

        if (oracleType.StartsWith("INTERVAL DAY"))
        {
            if (postgresType != "interval")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Interval Type",
                    "Oracle INTERVAL DAY TO SECOND should map to PostgreSQL INTERVAL.",
                    "Use INTERVAL type for date/time durations");
            }
        }

        if (oracleType == "ROWID" || oracleType == "UROWID")
        {
            if (postgresType != "varchar" && postgresType != "character varying" && postgresType != "text")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Error, "Row Identifier Type",
                    $"Oracle {oracleType} should map to VARCHAR or TEXT.",
                    "Use VARCHAR(18) for ROWID, VARCHAR(4000) for UROWID");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Warning, "ROWID Behavior Lost",
                    $"{oracleType} mapped to {postgresType.ToUpper()} - physical row access lost. " +
                    "ROWIDs are Oracle-specific and don't have PostgreSQL equivalent.",
                    "Review queries using ROWID - rewrite using primary keys or ctid if needed");
            }
        }

        if (oracleType == "BFILE")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Critical, "External File Pointer",
                "Oracle BFILE (external file pointer) has no PostgreSQL equivalent. " +
                "BFILE stores pointer to OS file, not the file content itself.",
                "Migrate external files to database BYTEA or use application file storage (S3, file server)");
        }

        if (oracleType == "SDO_GEOMETRY" || oracleType.Contains("MDSYS.SDO_GEOMETRY"))
        {
            if (postgresType != "geometry" && postgresType != "geography")
            {
                AddIssue(oracle, postgres, ValidationSeverity.Critical, "Spatial Type Missing",
                    "Oracle SDO_GEOMETRY requires PostGIS extension in PostgreSQL.",
                    "Install PostGIS and use GEOMETRY or GEOGRAPHY type. Review spatial indexing strategy");
            }
            else
            {
                AddIssue(oracle, postgres, ValidationSeverity.Info, "Spatial Type OK",
                    "SDO_GEOMETRY correctly mapped to PostGIS type.",
                    "Verify spatial queries and indexes are migrated correctly");
            }
        }

        if (oracleType.Contains(".") || oracleType.Contains("OBJECT"))
        {
            AddIssue(oracle, postgres, ValidationSeverity.Critical, "User-Defined Type",
                $"Oracle User-Defined Type (UDT) '{oracleType}' requires manual conversion. " +
                "PostgreSQL uses composite types or JSON for complex structures.",
                "Review UDT structure and convert to PostgreSQL composite type, JSON, or normalize to separate tables");
        }
    }

    #endregion

    #region Special Cases

    private void ValidateEmptyStringHandling(ColumnMetadata oracle, ColumnMetadata postgres, string oracleType, string postgresType)
    {
        if (oracleType is "VARCHAR2" or "CHAR" or "CLOB")
        {
            AddIssue(oracle, postgres, ValidationSeverity.Critical, "Empty String Handling",
                "CRITICAL: Oracle treats '' (empty string) as NULL. PostgreSQL treats '' as an empty string. " +
                "Validate ETL logic handled this conversion correctly.",
                "Review data migration scripts for empty string -> NULL conversions");
        }
    }

    #endregion

    private void AddIssue(
        ColumnMetadata oracle,
        ColumnMetadata postgres,
        ValidationSeverity severity,
        string category,
        string message,
        string? recommendation)
    {
        _issues.Add(new ValidationIssue
        {
            SchemaName = oracle.SchemaName,
            TableName = oracle.TableName,
            ColumnName = oracle.ColumnName,
            OracleType = FormatOracleType(oracle),
            PostgresType = FormatPostgresType(postgres),
            Severity = severity,
            Category = category,
            Message = message,
            Recommendation = recommendation,
            Metadata = new Dictionary<string, string>
            {
                ["OraclePrecision"] = oracle.DataPrecision?.ToString() ?? "N/A",
                ["OracleScale"] = oracle.DataScale?.ToString() ?? "N/A",
                ["PostgresPrecision"] = postgres.DataPrecision?.ToString() ?? "N/A",
                ["PostgresScale"] = postgres.DataScale?.ToString() ?? "N/A"
            }
        });
    }

    private string FormatOracleType(ColumnMetadata col)
    {
        var type = col.DataType;
        if (col.DataPrecision.HasValue && col.DataScale.HasValue)
            return $"{type}({col.DataPrecision},{col.DataScale})";
        if (col.DataPrecision.HasValue)
            return $"{type}({col.DataPrecision})";
        if (col.CharLength.HasValue)
            return $"{type}({col.CharLength})";
        return type;
    }

    private string FormatPostgresType(ColumnMetadata col)
    {
        var type = col.DataType;
        if (col.DataPrecision.HasValue && col.DataScale.HasValue)
            return $"{type}({col.DataPrecision},{col.DataScale})";
        if (col.CharLength.HasValue)
            return $"{type}({col.CharLength})";
        return type;
    }
}
