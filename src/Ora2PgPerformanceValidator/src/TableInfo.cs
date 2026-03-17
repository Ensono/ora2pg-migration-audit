namespace Ora2PgPerformanceValidator.Models;

public class TableInfo
{
    public string Name { get; set; } = string.Empty;
    public PrimaryKeyInfo? PrimaryKey { get; set; }
    public long RowCount { get; set; }
}

public class PrimaryKeyInfo
{
    public string Column { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
}
