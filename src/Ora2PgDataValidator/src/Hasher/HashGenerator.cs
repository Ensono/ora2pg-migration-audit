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
        if (value is null)
        {
            return "NULL";
        }

       if (value is DateTime dt)
        {
            // Format: "yyyy-MM-dd HH:mm:ss.fff" (e.g., "2024-03-10 14:30:00.123")
            return dt.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is DateTimeOffset dto)
        {
            // Format: "yyyy-MM-dd HH:mm:ss.fffzzz" (e.g., "2024-03-10 14:30:00.123+00:00")
            return dto.ToString("yyyy-MM-dd HH:mm:ss.fffzzz", System.Globalization.CultureInfo.InvariantCulture);
        }

        if (value is decimal || value is double || value is float)
        {
            return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL";
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
