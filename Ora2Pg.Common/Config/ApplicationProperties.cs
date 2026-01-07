using DotNetEnv;
using Serilog;

namespace Ora2Pg.Common.Config;

public class ApplicationProperties
{
    private static ApplicationProperties? _instance;
    private static readonly object _lock = new();
    private readonly Dictionary<string, string> _properties = new();
    private readonly ILogger _logger = Log.ForContext<ApplicationProperties>();

    private ApplicationProperties()
    {
        LoadEnvironmentVariables();
        _logger.Information("✓ Application properties loaded successfully");
    }

    public static ApplicationProperties Instance
    {
        get
        {
            if (_instance == null)
            {
                lock (_lock)
                {
                    _instance ??= new ApplicationProperties();
                }
            }
            return _instance;
        }
    }

    private void LoadEnvironmentVariables()
    {
        string currentDir = Directory.GetCurrentDirectory();
        _logger.Information("Current working directory: {CurrentDir}", currentDir);

        string? envFilePath = null;

        var solutionDir = FindSolutionRoot(currentDir);
        if (solutionDir != null)
        {
            var solutionEnvPath = Path.Combine(solutionDir, ".env");
            if (File.Exists(solutionEnvPath))
            {
                envFilePath = solutionEnvPath;
                _logger.Information("Found shared .env at solution root: {SolutionDir}", solutionDir);
            }
        }
        
        if (envFilePath == null)
        {
            var projectEnvPath = Path.Combine(currentDir, ".env");
            if (File.Exists(projectEnvPath))
            {
                envFilePath = projectEnvPath;
                _logger.Information("Found project-specific .env in: {CurrentDir}", currentDir);
            }
        }
        
        if (envFilePath == null && currentDir.Contains("bin"))
        {
            var parentDir = Directory.GetParent(currentDir)?.Parent?.Parent?.FullName;
            if (parentDir != null)
            {
                var parentEnvPath = Path.Combine(parentDir, ".env");
                if (File.Exists(parentEnvPath))
                {
                    envFilePath = parentEnvPath;
                    _logger.Information("Found .env in project root: {ParentDir}", parentDir);
                }
            }
        }

        if (envFilePath != null)
        {
            Env.Load(envFilePath);
            _logger.Information("✓ .env file loaded from: {EnvFilePath}", envFilePath);
        }
        else
        {
            _logger.Warning("⚠ .env file not found - checked solution root, project directory, and parent directories");
            _logger.Warning("  Using environment variables and default values only");
        }

        foreach (System.Collections.DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            string key = entry.Key?.ToString() ?? string.Empty;
            string value = entry.Value?.ToString() ?? string.Empty;
            if (!string.IsNullOrEmpty(key))
            {
                _properties[key] = value;
            }
        }
    }


    private string? FindSolutionRoot(string startPath)
    {
        var currentDir = new DirectoryInfo(startPath);
        
        while (currentDir != null)
        {
            if (currentDir.GetFiles("*.sln").Any())
            {
                return currentDir.FullName;
            }
            
            var subdirs = currentDir.GetDirectories();
            if (subdirs.Any(d => d.Name == "Ora2Pg.Common") && 
                subdirs.Any(d => d.Name.StartsWith("Ora2Pg")))
            {
                return currentDir.FullName;
            }
            
            currentDir = currentDir.Parent;
        }
        
        return null;
    }

    public string Get(string key, string defaultValue = "")
    {
        string? envValue = Environment.GetEnvironmentVariable(key);
        if (!string.IsNullOrEmpty(envValue))
        {
            return envValue;
        }

        if (_properties.TryGetValue(key, out string? value) && !string.IsNullOrEmpty(value))
        {
            return value;
        }

        return defaultValue;
    }

    public int GetInt(string key, int defaultValue = 0)
    {
        string value = Get(key);
        if (int.TryParse(value, out int result))
        {
            return result;
        }
        return defaultValue;
    }

    public bool GetBool(string key, bool defaultValue = false)
    {
        string value = Get(key);
        if (bool.TryParse(value, out bool result))
        {
            return result;
        }
        return defaultValue;
    }

    public string[] GetArray(string key)
    {
        string value = Get(key);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }
        return Array.Empty<string>();
    }
}
