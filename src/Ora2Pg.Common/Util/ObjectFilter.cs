using Serilog;
using Ora2Pg.Common.Config;

namespace Ora2Pg.Common.Util;

public class ObjectFilter
{
    private readonly ILogger _logger = Log.ForContext<ObjectFilter>();
    private readonly List<string> _tableExclusionPatterns;
    private readonly Dictionary<string, HashSet<string>> _ignoredObjects;

    public ObjectFilter(ApplicationProperties props)
    {
        var patternsRaw = props.Get("TABLE_EXCLUSION_PATTERNS", props.Get("table.exclusion.patterns", string.Empty));
        _tableExclusionPatterns = patternsRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(pattern => !string.IsNullOrWhiteSpace(pattern))
            .ToList();

        var ignoredRaw = props.Get("IGNORED_OBJECTS", props.Get("ignored.objects", string.Empty));
        _ignoredObjects = ParseIgnoredObjects(ignoredRaw);

        if (_tableExclusionPatterns.Count > 0)
        {
            _logger.Information("Table exclusion patterns enabled: {Patterns}", string.Join(", ", _tableExclusionPatterns));
        }

        if (_ignoredObjects.Count > 0)
        {
            var formatted = _ignoredObjects
                .Select(entry => $"{entry.Key}={string.Join('|', entry.Value)}")
                .ToList();
            _logger.Information("Ignored objects configured: {IgnoredObjects}", string.Join(", ", formatted));
        }
    }

    public static ObjectFilter FromProperties(ApplicationProperties? props = null)
    {
        return new ObjectFilter(props ?? ApplicationProperties.Instance);
    }

    public bool IsTableExcluded(string tableReference, string? schemaName = null)
    {
        if (string.IsNullOrWhiteSpace(tableReference))
        {
            return false;
        }

        var fullName = tableReference.Trim();
        var shortName = ExtractTableName(fullName);

        if (MatchesPattern(fullName) || MatchesPattern(shortName))
        {
            return true;
        }

        if (IsObjectIgnored("table", fullName, schemaName) || IsObjectIgnored("table", shortName, schemaName))
        {
            return true;
        }

        return false;
    }

    public bool IsObjectIgnored(string objectType, string objectName, string? schemaName = null)
    {
        if (string.IsNullOrWhiteSpace(objectType) || string.IsNullOrWhiteSpace(objectName))
        {
            return false;
        }

        var typeKey = objectType.Trim().ToLowerInvariant();
        if (!_ignoredObjects.TryGetValue(typeKey, out var names) || names.Count == 0)
        {
            return false;
        }

        var name = objectName.Trim();
        if (names.Contains(name))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(schemaName))
        {
            var fullName = $"{schemaName}.{name}";
            if (names.Contains(fullName))
            {
                return true;
            }
        }

        if (name.Contains('.'))
        {
            var unqualified = name.Split('.', 2)[1];
            if (names.Contains(unqualified))
            {
                return true;
            }
        }

        return false;
    }

    public List<string> FilterTables(IEnumerable<string> tableNames, string? schemaName = null)
    {
        var filtered = new List<string>();
        foreach (var tableName in tableNames)
        {
            if (IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            filtered.Add(tableName);
        }

        return filtered;
    }

    private bool MatchesPattern(string tableName)
    {
        if (_tableExclusionPatterns.Count == 0)
        {
            return false;
        }

        foreach (var pattern in _tableExclusionPatterns)
        {
            if (tableName.Contains(pattern, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string ExtractTableName(string tableReference)
    {
        if (tableReference.Contains('.'))
        {
            return tableReference.Split('.', 2)[1];
        }

        return tableReference;
    }

    private static Dictionary<string, HashSet<string>> ParseIgnoredObjects(string raw)
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(raw))
        {
            return result;
        }

        var entries = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var entry in entries)
        {
            var parts = entry.Split('=', 2, StringSplitOptions.TrimEntries);
            if (parts.Length != 2 || string.IsNullOrWhiteSpace(parts[0]) || string.IsNullOrWhiteSpace(parts[1]))
            {
                continue;
            }

            var typeKey = parts[0].Trim().ToLowerInvariant();
            var name = parts[1].Trim();

            if (!result.TryGetValue(typeKey, out var names))
            {
                names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                result[typeKey] = names;
            }

            names.Add(name);
        }

        return result;
    }
}
