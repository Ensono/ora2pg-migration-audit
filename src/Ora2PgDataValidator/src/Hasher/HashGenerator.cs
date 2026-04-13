using System.Security.Cryptography;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Ora2Pg.Common.Config;

namespace Ora2PgDataValidator.Hasher;

public static class HashGenerator
{
    private static readonly bool _skipBlobColumns;
    
    static HashGenerator()
    {
        var props = ApplicationProperties.Instance;
        
        _skipBlobColumns = props.Get("SKIP_LOB_COLUMNS",
                                     props.Get("SKIP_BLOB_COLUMNS", "false"))
                                .Equals("true", StringComparison.OrdinalIgnoreCase);
    }
    
    public static string GenerateHash(Dictionary<string, object?> rowData, string algorithm = "SHA256")
    {
        var sortedKeys = rowData.Keys.OrderBy(k => k).ToList();

        var sb = new StringBuilder();
        foreach (var key in sortedKeys)
        {
            var value = rowData[key];
            string valueStr = ConvertValueToString(value);
            
            if (sb.Length > 0)
            {
                sb.Append('|');
            }
            sb.Append(valueStr);
        }
        
        string input = sb.ToString();
        byte[] inputBytes = Encoding.UTF8.GetBytes(input);
        
        byte[] hashBytes = algorithm.ToUpper() switch
        {
            "MD5" => MD5.HashData(inputBytes),
            "SHA256" => SHA256.HashData(inputBytes),
            _ => SHA256.HashData(inputBytes)
        };
        
        return Convert.ToHexString(hashBytes).ToLower();
    }
    
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
            return dto.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz", System.Globalization.CultureInfo.InvariantCulture);
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
