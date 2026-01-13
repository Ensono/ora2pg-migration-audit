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
            string valueStr = value?.ToString() ?? "NULL";
            
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
