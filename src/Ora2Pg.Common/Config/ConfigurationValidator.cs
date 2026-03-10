using Serilog;

namespace Ora2Pg.Common.Config;

public static class ConfigurationValidator
{

    public static bool ValidateComparisonModeConfig(ApplicationProperties props)
    {
        var errors = new List<string>();

        // Oracle configuration
        ValidateRequired(props, "ORACLE_HOST", errors);
        ValidateRequired(props, "ORACLE_PORT", errors);
        ValidateRequired(props, "ORACLE_SERVICE", errors);
        ValidateRequired(props, "ORACLE_USER", errors);
        ValidateRequired(props, "ORACLE_PASSWORD", errors);
        ValidateRequired(props, "ORACLE_SCHEMA", errors);

        // PostgreSQL configuration
        ValidateRequired(props, "POSTGRES_HOST", errors);
        ValidateRequired(props, "POSTGRES_PORT", errors);
        ValidateRequired(props, "POSTGRES_DB", errors);
        ValidateRequired(props, "POSTGRES_USER", errors);
        ValidateRequired(props, "POSTGRES_PASSWORD", errors);
        ValidateRequired(props, "POSTGRES_SCHEMA", errors);

        if (errors.Count > 0)
        {
            Log.Error("");
            Log.Error("✗ Configuration Validation Failed");
            Log.Error("  The following required environment variables are missing or empty:");
            Log.Error("");
            foreach (var error in errors)
            {
                Log.Error("  • {Error}", error);
            }
            Log.Error("");
            Log.Error("  Please ensure all required variables are set in your .env file");
            Log.Error("  Location: ora2pg-migration-audit/.env");
            Log.Error("");
            return false;
        }

        return true;
    }

    public static bool ValidateSingleDatabaseModeConfig(ApplicationProperties props, string targetDatabase)
    {
        var errors = new List<string>();

        if (targetDatabase.Equals("ORACLE", StringComparison.OrdinalIgnoreCase))
        {
            ValidateRequired(props, "ORACLE_HOST", errors);
            ValidateRequired(props, "ORACLE_PORT", errors);
            ValidateRequired(props, "ORACLE_SERVICE", errors);
            ValidateRequired(props, "ORACLE_USER", errors);
            ValidateRequired(props, "ORACLE_PASSWORD", errors);
            ValidateRequired(props, "ORACLE_SCHEMA", errors);
        }
        else if (targetDatabase.Equals("POSTGRESQL", StringComparison.OrdinalIgnoreCase))
        {
            ValidateRequired(props, "POSTGRES_HOST", errors);
            ValidateRequired(props, "POSTGRES_PORT", errors);
            ValidateRequired(props, "POSTGRES_DB", errors);
            ValidateRequired(props, "POSTGRES_USER", errors);
            ValidateRequired(props, "POSTGRES_PASSWORD", errors);
            ValidateRequired(props, "POSTGRES_SCHEMA", errors);
        }
        else
        {
            errors.Add($"TARGET_DATABASE must be 'ORACLE' or 'POSTGRESQL', got: '{targetDatabase}'");
        }

        if (errors.Count > 0)
        {
            Log.Error("");
            Log.Error("✗ Configuration Validation Failed");
            Log.Error("  The following required environment variables are missing or empty:");
            Log.Error("");
            foreach (var error in errors)
            {
                Log.Error("  • {Error}", error);
            }
            Log.Error("");
            Log.Error("  Please ensure all required variables are set in your .env file");
            Log.Error("  Location: ora2pg-migration-audit/.env");
            Log.Error("");
            return false;
        }

        return true;
    }

    private static void ValidateRequired(ApplicationProperties props, string key, List<string> errors)
    {
        string value = props.Get(key, string.Empty);
        if (string.IsNullOrWhiteSpace(value))
        {
            errors.Add($"{key} is required but not set");
        }
    }

    public static void ValidatePasswordSecurity(ApplicationProperties props)
    {
        var warnings = new List<string>();

        string oraclePassword = props.Get("ORACLE_PASSWORD", string.Empty);
        string postgresPassword = props.Get("POSTGRES_PASSWORD", string.Empty);

        // Check for common weak passwords
        var weakPasswords = new[] { "password", "admin", "123456", "oracle", "postgres", "changeme" };

        if (!string.IsNullOrEmpty(oraclePassword) && 
            weakPasswords.Any(weak => oraclePassword.Equals(weak, StringComparison.OrdinalIgnoreCase)))
        {
            warnings.Add("ORACLE_PASSWORD appears to be a weak/common password");
        }

        if (!string.IsNullOrEmpty(postgresPassword) && 
            weakPasswords.Any(weak => postgresPassword.Equals(weak, StringComparison.OrdinalIgnoreCase)))
        {
            warnings.Add("POSTGRES_PASSWORD appears to be a weak/common password");
        }

        // Check for special characters that might cause issues
        if (!string.IsNullOrEmpty(oraclePassword) && !oraclePassword.StartsWith("'") && 
            (oraclePassword.Contains('"') || oraclePassword.Contains('`') || oraclePassword.Contains('$')))
        {
            warnings.Add("ORACLE_PASSWORD contains special characters - consider wrapping in single quotes: PASSWORD='your_password'");
        }

        if (!string.IsNullOrEmpty(postgresPassword) && !postgresPassword.StartsWith("'") && 
            (postgresPassword.Contains('"') || postgresPassword.Contains('`') || postgresPassword.Contains('$')))
        {
            warnings.Add("POSTGRES_PASSWORD contains special characters - consider wrapping in single quotes: PASSWORD='your_password'");
        }

        if (warnings.Count > 0)
        {
            Log.Warning("");
            Log.Warning("⚠ Password Security Warnings:");
            foreach (var warning in warnings)
            {
                Log.Warning("  • {Warning}", warning);
            }
            Log.Warning("");
        }
    }

    public static bool ValidateComparisonTargets(ApplicationProperties props)
    {
        string tablesConfig = props.Get("TABLES_TO_COMPARE", props.Get("tables.to.compare", ""));
        string viewsConfig = props.Get("VIEWS_TO_COMPARE", "");

        if (string.IsNullOrWhiteSpace(tablesConfig) && string.IsNullOrWhiteSpace(viewsConfig))
        {
            Log.Error("");
            Log.Error("✗ Configuration Error: No comparison targets specified");
            Log.Error("  At least one of the following must be set:");
            Log.Error("");
            Log.Error("  • TABLES_TO_COMPARE=ALL  (or comma-separated list)");
            Log.Error("  • VIEWS_TO_COMPARE=ALL   (or comma-separated list)");
            Log.Error("");
            Log.Error("  Example configuration:");
            Log.Error("    TABLES_TO_COMPARE=ALL");
            Log.Error("    VIEWS_TO_COMPARE=ALL");
            Log.Error("");
            return false;
        }

        bool needsSchemas = false;
        if (!string.IsNullOrWhiteSpace(tablesConfig) && tablesConfig.Trim().Equals("ALL", StringComparison.OrdinalIgnoreCase))
        {
            needsSchemas = true;
        }
        if (!string.IsNullOrWhiteSpace(viewsConfig) && viewsConfig.Trim().Equals("ALL", StringComparison.OrdinalIgnoreCase))
        {
            needsSchemas = true;
        }

        if (needsSchemas)
        {
            var errors = new List<string>();
            ValidateRequired(props, "ORACLE_SCHEMA", errors);
            ValidateRequired(props, "POSTGRES_SCHEMA", errors);

            if (errors.Count > 0)
            {
                Log.Error("");
                Log.Error("✗ Configuration Error: Schema names required for auto-discovery");
                Log.Error("  When using TABLES_TO_COMPARE=ALL or VIEWS_TO_COMPARE=ALL,");
                Log.Error("  you must specify schema names:");
                Log.Error("");
                foreach (var error in errors)
                {
                    Log.Error("  • {Error}", error);
                }
                Log.Error("");
                Log.Error("  Example:");
                Log.Error("    ORACLE_SCHEMA=MYAPP");
                Log.Error("    POSTGRES_SCHEMA=myapp");
                Log.Error("");
                return false;
            }
        }

        return true;
    }
}
