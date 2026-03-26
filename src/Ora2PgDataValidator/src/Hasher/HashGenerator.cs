using System.Security.Cryptography;
using System.Text;

namespace Ora2PgDataValidator.Hasher;

public static class HashGenerator
{
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
            return Convert.ToHexString(bytes).ToLower();
        }

        if (value is decimal decVal)
        {
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
            return dt.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is DateTimeOffset dto)
        {
            return dto.ToString("yyyy-MM-dd HH:mm:ss.fffzzz", System.Globalization.CultureInfo.InvariantCulture);
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
            return strVal.TrimEnd();
        }

        if (value is char charVal)
        {
            return charVal.ToString();
        }

        return value.ToString() ?? "NULL";
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
