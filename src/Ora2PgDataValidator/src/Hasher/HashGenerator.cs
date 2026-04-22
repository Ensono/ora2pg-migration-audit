using System.Security.Cryptography;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Ora2Pg.Common.Config;
using Serilog;

namespace Ora2PgDataValidator.Hasher;

public static class HashGenerator
{
    private static readonly bool _skipBlobColumns;

    // When DEBUG_ROWS=N is set, log structural debug info (column names + types) for the first N rows.
    // Optionally restrict to a specific table with DEBUG_TABLE=<tablename>.
    // Actual row values are NEVER written to the main log to avoid leaking sensitive data.
    // Set DEBUG_LOG_ROW_VALUES=true to write actual values to a separate dedicated file
    // (debug-row-values-<date>.log) with an explicit security warning.
    private static readonly int _debugRows;
    private static readonly string _debugTable;
    private static readonly bool _logRowValues;
    private static readonly string? _rowValuesFilePath;
    private static readonly object _rowValuesFileLock = new();
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _debugCounters = new();

    static HashGenerator()
    {
        var props = ApplicationProperties.Instance;
        
        _skipBlobColumns = props.Get("SKIP_LOB_COLUMNS",
                                     props.Get("SKIP_BLOB_COLUMNS", "false"))
                                .Equals("true", StringComparison.OrdinalIgnoreCase);

        _debugRows  = props.GetInt("DEBUG_ROWS", 0);
        _debugTable = props.Get("DEBUG_TABLE", "").Trim();
        _logRowValues = props.Get("DEBUG_LOG_ROW_VALUES", "false")
                             .Equals("true", StringComparison.OrdinalIgnoreCase);

        if (_debugRows > 0)
        {
            if (_logRowValues)
            {
                string fileName = $"debug-row-values-{DateTime.UtcNow:yyyyMMdd-HHmmss}.log";
                _rowValuesFilePath = Path.Combine(AppContext.BaseDirectory, fileName);
                Log.Warning("HashGenerator debug mode ON — actual row values will be written to '{FilePath}'. " +
                            "⚠ This file may contain SENSITIVE DATA. Do not share or commit it.",
                    _rowValuesFilePath);
                File.WriteAllText(_rowValuesFilePath,
                    $"# DEBUG ROW VALUES — generated {DateTime.UtcNow:u}\n" +
                    $"# ⚠ SENSITIVE: contains actual database values. Do not share or commit.\n\n");
            }
            else
            {
                Log.Warning("HashGenerator debug mode ON — will log column structure (names/types) for first {N} rows{Filter}. " +
                            "Actual values are suppressed. Set DEBUG_LOG_ROW_VALUES=true to enable value logging to a separate file.",
                    _debugRows,
                    string.IsNullOrEmpty(_debugTable) ? "" : $" of table '{_debugTable}'");
            }
        }
    }
    
    public static string GenerateHash(Dictionary<string, object?> rowData, string algorithm = "SHA256",
                                       string? tableHint = null)
    {
        var sortedKeys = rowData.Keys.OrderBy(k => k).ToList();

        var sb = new StringBuilder();

        bool shouldDebug = _debugRows > 0
            && (string.IsNullOrEmpty(_debugTable)
                || tableHint != null && tableHint.Contains(_debugTable, StringComparison.OrdinalIgnoreCase));

        var debugParts = shouldDebug ? new List<string>() : null;

        foreach (var key in sortedKeys)
        {
            var value = rowData[key];
            string valueStr = ConvertValueToString(value);

            debugParts?.Add($"{key}[{value?.GetType().Name ?? "null"}]={valueStr}");

            if (sb.Length > 0)
            {
                sb.Append('|');
            }
            sb.Append(valueStr);
        }

        string input = sb.ToString();

        if (debugParts != null)
        {
            string counterKey = tableHint ?? "?";
            int logged = _debugCounters.AddOrUpdate(counterKey, 1, (_, v) => v + 1);
            if (logged <= _debugRows)
            {
                var columnStructure = debugParts.Select(p =>
                {
                    int eq = p.LastIndexOf('=');
                    return eq >= 0 ? p[..eq] : p;
                });
                Log.Information("[DEBUG_ROWS] Table={Table} Row={Row} Columns(names+types): {Cols}",
                    tableHint ?? "?", logged, string.Join(" | ", columnStructure));

                if (_logRowValues && _rowValuesFilePath != null)
                {
                    var lines = new System.Text.StringBuilder();
                    lines.AppendLine($"[Row {logged}] Table={tableHint ?? "?"} PreHashString={input}");
                    lines.AppendLine($"  Columns: {string.Join(" | ", debugParts)}");
                    lines.AppendLine();

                    lock (_rowValuesFileLock)
                    {
                        File.AppendAllText(_rowValuesFilePath, lines.ToString());
                    }
                }
            }
        }

        byte[] inputBytes = Encoding.UTF8.GetBytes(input);
        
        byte[] hashBytes = algorithm.ToUpper() switch
        {
            "MD5" => MD5.HashData(inputBytes),
            "SHA256" => SHA256.HashData(inputBytes),
            _ => SHA256.HashData(inputBytes)
        };
        
        return Convert.ToHexString(hashBytes).ToLower();
    }
    
    public static string ConvertValueToStringPublic(object? value) => ConvertValueToString(value);
    
    private static string ConvertValueToString(object? value)
    {
        if (value is null || value is DBNull)
        {
            return "NULL";
        }

        if (value is byte[] bytes)
        {
            // Skip BLOB columns if configured
            if (_skipBlobColumns)
            {
                return "BLOB_SKIPPED";
            }
            
            return Convert.ToHexString(bytes).ToLower();
        }

        if (value is decimal decVal)
        {
            if (decVal == Math.Truncate(decVal) && decVal >= long.MinValue && decVal <= long.MaxValue)
            {
                return ((long)decVal).ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            if (decVal == Math.Truncate(decVal))
            {
                return ((double)decVal).ToString("G17", System.Globalization.CultureInfo.InvariantCulture);
            }

            return decVal.ToString("G29", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is double dblVal)
        {
            return dblVal.ToString("G17", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is float fltVal)
        {
            return fltVal.ToString("G9", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is long || value is int || value is short || value is byte ||
            value is ulong || value is uint || value is ushort || value is sbyte)
        {
            return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL";
        }

        if (value is DateTime dt)
        {
            return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is DateTimeOffset dto)
        {
            // Normalize to UTC so that the same instant with different offsets
            // (e.g. Oracle -04:00 vs PostgreSQL +00:00) produces the same hash.
            var utc = dto.ToUniversalTime();
            return utc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is TimeSpan ts)
        {
            return ts.ToString("c", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is bool boolVal)
        {
            return boolVal ? "true" : "false";
        }

        if (value is Guid guidVal)
        {
            return guidVal.ToString("D").ToLower(); // Lowercase with hyphens
        }

        if (value is string strVal)
        {
            if (strVal.Length >= 8 && IsLikelyGuidString(strVal))
            {
                return strVal.ToLowerInvariant();
            }
            
            if (strVal.TrimStart().StartsWith("<"))
            {
                var normalized = TryNormalizeXml(strVal);
                if (normalized != null)
                {
                    return normalized; // Use normalized XML for consistent hashing
                }
                
                normalized = strVal;
                normalized = System.Text.RegularExpressions.Regex.Replace(normalized, @"<\?xml[^?]*\?>", "");
                normalized = System.Text.RegularExpressions.Regex.Replace(normalized, @">\s+<", "><");
                normalized = normalized.Trim();
                return normalized;
            }
            
            return strVal.TrimEnd(); // Only trim trailing whitespace (Oracle CHAR padding)
        }

        if (value is char charVal)
        {
            return charVal.ToString().TrimEnd();
        }
        if (value is string rawStr)
        {
            return rawStr.TrimEnd();
        }

        var stringValue = value.ToString() ?? "NULL";

        if (TryParseOracleTimestampString(stringValue, out var parsedDt))
        {
            return parsedDt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);
        }

        return stringValue;
    }

    private static bool TryParseOracleTimestampString(string value, out DateTime result)
    {
        result = default;

        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var oracleNlsFormats = new[]
        {
            "d-MMM-yy h.mm.ss.ffffff tt",
            "d-MMM-yy h.mm.ss.fffff tt",
            "d-MMM-yy h.mm.ss.ffff tt",
            "d-MMM-yy h.mm.ss.fff tt",
            "d-MMM-yy h.mm.ss.ff tt",
            "d-MMM-yy h.mm.ss.f tt",
            "d-MMM-yy h.mm.ss tt",
            "d-MMM-yyyy h.mm.ss.ffffff tt",
            "d-MMM-yyyy h.mm.ss.fffff tt",
            "d-MMM-yyyy h.mm.ss.ffff tt",
            "d-MMM-yyyy h.mm.ss.fff tt",
            "d-MMM-yyyy h.mm.ss tt",
        };

        foreach (var fmt in oracleNlsFormats)
        {
            if (DateTime.TryParseExact(value, fmt,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out result))
            {
                return true;
            }
        }

        var isoFormats = new[]
        {
            "yyyy-MM-dd HH:mm:ss.ffffff",
            "yyyy-MM-dd HH:mm:ss.fffff",
            "yyyy-MM-dd HH:mm:ss.ffff",
            "yyyy-MM-dd HH:mm:ss.fff",
            "yyyy-MM-dd HH:mm:ss.ff",
            "yyyy-MM-dd HH:mm:ss.f",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd",
        };

        foreach (var fmt in isoFormats)
        {
            if (DateTime.TryParseExact(value, fmt,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out result))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsLikelyGuidString(string value)
    {
        bool hasHyphen = false;
        foreach (char c in value)
        {
            if (c == '-')
            {
                hasHyphen = true;
            }
            else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
            {
                return false; // Not a hex character
            }
        }
        
        return hasHyphen || value.Length == 32;
    }


    private static string? TryNormalizeXml(string xmlContent)
    {
        try
        {
            var decoded = System.Net.WebUtility.HtmlDecode(xmlContent);
            
            var doc = XDocument.Parse(decoded, LoadOptions.PreserveWhitespace);
            
            NormalizeXmlElement(doc.Root);
            
            var result = doc.ToString(SaveOptions.DisableFormatting);
            
            result = System.Text.RegularExpressions.Regex.Replace(result, @">\s+<", "><");
            
            return result;
        }
        catch
        {
            return null;
        }
    }

    private static void NormalizeXmlElement(XElement? element)
    {
        if (element == null) return;
        
        var sortedAttrs = element.Attributes()
            .Where(a => !a.IsNamespaceDeclaration)
            .OrderBy(a => a.Name.NamespaceName)
            .ThenBy(a => a.Name.LocalName)
            .ToList();
        
        var namespaceAttrs = element.Attributes()
            .Where(a => a.IsNamespaceDeclaration)
            .OrderBy(a => a.Name.LocalName)
            .ToList();
        
        element.RemoveAttributes();
        
        foreach (var ns in namespaceAttrs)
        {
            element.Add(ns);
        }
        
        foreach (var attr in sortedAttrs)
        {
            attr.Value = attr.Value.Trim();
            element.Add(attr);
        }
        
        var textNodes = element.Nodes().OfType<XText>().ToList();
        foreach (var textNode in textNodes)
        {
            if (string.IsNullOrWhiteSpace(textNode.Value))
            {
                textNode.Remove();
            }
        }
        
        var childElements = element.Elements().ToList();
        foreach (var child in childElements)
        {
            child.Remove();
        }
        
        var sortedChildren = childElements
            .OrderBy(e => e.Name.NamespaceName)
            .ThenBy(e => e.Name.LocalName)
            .ToList();
        
        foreach (var child in sortedChildren)
        {
            element.Add(child);
        }
        
        foreach (var child in element.Elements())
        {
            NormalizeXmlElement(child);
        }
        
        if (!element.HasElements && !string.IsNullOrWhiteSpace(element.Value))
        {
            element.Value = element.Value.Trim();
        }
    }
    
    public static string GenerateHash(string input, string algorithm = "SHA256")
    {
        byte[] inputBytes = Encoding.UTF8.GetBytes(input);
        
        byte[] hashBytes = algorithm.ToUpper() switch
        {
            "MD5" => MD5.HashData(inputBytes),
            "SHA256" => SHA256.HashData(inputBytes),
            _ => SHA256.HashData(inputBytes)
        };
        
        return Convert.ToHexString(hashBytes).ToLower();
    }
}
