namespace Ora2PgDataValidator.Comparison;

public class ComparisonResult
{
    public string SourceTable { get; }
    public string TargetTable { get; }

    public int SourceRowCount { get; set; }
    public int TargetRowCount { get; set; }
    public int MatchingRows { get; set; }
    public int MismatchedRows { get; set; }
    public int MissingInTarget { get; set; }
    public int ExtraInTarget { get; set; }
    public bool IsMatch { get; set; }
    public string? Error { get; set; }

    public Dictionary<int, string> MissingRows { get; } = new();
    public Dictionary<int, string> ExtraRows { get; } = new();
    public Dictionary<int, HashPair> MismatchedRowDetails { get; } = new();

    public Dictionary<int, Dictionary<string, object?>> MissingRowPrimaryKeys { get; } = new();
    public Dictionary<int, Dictionary<string, object?>> ExtraRowPrimaryKeys { get; } = new();
    public Dictionary<int, RowPrimaryKeyPair> MismatchedRowPrimaryKeys { get; } = new();

    public ComparisonResult(string sourceTable, string targetTable)
    {
        SourceTable = sourceTable;
        TargetTable = targetTable;
    }

    public void AddMissingRow(int rowId, string hash)
    {
        MissingRows[rowId] = hash;
    }

    public void AddMissingRow(int rowId, string hash, Dictionary<string, object?> primaryKeyValues)
    {
        MissingRows[rowId] = hash;
        MissingRowPrimaryKeys[rowId] = primaryKeyValues;
    }

    public void AddExtraRow(int rowId, string hash)
    {
        ExtraRows[rowId] = hash;
    }

    public void AddExtraRow(int rowId, string hash, Dictionary<string, object?> primaryKeyValues)
    {
        ExtraRows[rowId] = hash;
        ExtraRowPrimaryKeys[rowId] = primaryKeyValues;
    }

    public void AddMismatchedRow(int rowId, string oracleHash, string postgresHash)
    {
        MismatchedRowDetails[rowId] = new HashPair(oracleHash, postgresHash);
    }

    public void AddMismatchedRow(int rowId, string oracleHash, string postgresHash, 
                                 Dictionary<string, object?> oraclePrimaryKeyValues,
                                 Dictionary<string, object?> postgresPrimaryKeyValues)
    {
        MismatchedRowDetails[rowId] = new HashPair(oracleHash, postgresHash);
        MismatchedRowPrimaryKeys[rowId] = new RowPrimaryKeyPair(oraclePrimaryKeyValues, postgresPrimaryKeyValues);
    }


    public double MatchPercentage =>
        SourceRowCount == 0 ? 0.0 : (double)MatchingRows / SourceRowCount * 100.0;


    public bool HasError => !string.IsNullOrEmpty(Error);

    public override string ToString()
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("ComparisonResult{");
        sb.Append($"source={SourceTable}");
        sb.Append($", target={TargetTable}");
        sb.Append($", sourceRows={SourceRowCount}");
        sb.Append($", targetRows={TargetRowCount}");
        sb.Append($", matching={MatchingRows}");
        sb.Append($", mismatched={MismatchedRows}");
        sb.Append($", missing={MissingInTarget}");
        sb.Append($", extra={ExtraInTarget}");
        sb.Append($", match={IsMatch}");
        if (HasError)
        {
            sb.Append($", error='{Error}'");
        }
        sb.Append("}");
        return sb.ToString();
    }
    

    public record HashPair(string OracleHash, string PostgresHash)
    {
        public override string ToString() =>
            $"Oracle: {OracleHash} ≠ PostgreSQL: {PostgresHash}";
    }


    public record RowPrimaryKeyPair(
        Dictionary<string, object?> OraclePrimaryKeys, 
        Dictionary<string, object?> PostgresPrimaryKeys)
    {
        public override string ToString()
        {
            var oraclePk = string.Join(", ", OraclePrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
            var postgresPk = string.Join(", ", PostgresPrimaryKeys.Select(kv => $"{kv.Key}={kv.Value}"));
            return $"Oracle: [{oraclePk}] ≠ PostgreSQL: [{postgresPk}]";
        }
    }
}