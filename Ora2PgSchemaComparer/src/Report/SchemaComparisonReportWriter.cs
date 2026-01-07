using System.Text;
using Ora2PgSchemaComparer.Comparison;

namespace Ora2PgSchemaComparer.Report;

public class SchemaComparisonReportWriter
{
    public string GenerateReport(ComparisonResult result)
    {
        var report = new StringBuilder();

        report.AppendLine("================================================================================");
        report.AppendLine("ORACLE TO POSTGRESQL SCHEMA COMPARISON");
        report.AppendLine("================================================================================");
        report.AppendLine($"Source (Oracle):      {result.OracleSchema.SchemaName} schema");
        report.AppendLine($"Target (PostgreSQL):  {result.PostgresSchema.SchemaName} schema");
        report.AppendLine($"Comparison Date:      {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        report.AppendLine();

        report.AppendLine("SCHEMA SUMMARY");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine($"  Tables:       Oracle={result.OracleSchema.TableCount,-3}  PostgreSQL={result.PostgresSchema.TableCount,-3}");
        report.AppendLine($"  Columns:      Oracle={result.OracleSchema.ColumnCount,-3}  PostgreSQL={result.PostgresSchema.ColumnCount,-3}");
        report.AppendLine($"  Primary Keys: Oracle={result.OracleSchema.PrimaryKeyCount,-3}  PostgreSQL={result.PostgresSchema.PrimaryKeyCount,-3}");
        report.AppendLine($"  Foreign Keys: Oracle={result.OracleSchema.ForeignKeyCount,-3}  PostgreSQL={result.PostgresSchema.ForeignKeyCount,-3}");
        report.AppendLine();

        report.AppendLine("1. TABLES & STRUCTURE");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.TableCount == result.PostgresSchema.TableCount) + 
            $" Table Count:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.TableCount} tables");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.TableCount} tables");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.ColumnCount == result.PostgresSchema.ColumnCount) +
            $" Column Count:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.ColumnCount} columns");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.ColumnCount} columns");
        report.AppendLine();
        
        if (result.TableIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.TableIssues)
            {
                report.AppendLine($"     {issue}");
            }
            report.AppendLine();
        }

        report.AppendLine("2. CONSTRAINTS");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.PrimaryKeyCount == result.PostgresSchema.PrimaryKeyCount) +
            $" Primary Keys:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.PrimaryKeyCount} primary keys");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.PrimaryKeyCount} primary keys");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.ForeignKeyCount == result.PostgresSchema.ForeignKeyCount) +
            $" Foreign Keys:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.ForeignKeyCount} foreign keys");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.ForeignKeyCount} foreign keys");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.UniqueConstraintCount == result.PostgresSchema.UniqueConstraintCount) +
            $" Unique Constraints:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.UniqueConstraintCount} unique constraints");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.UniqueConstraintCount} unique constraints");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.CheckConstraintCount == result.PostgresSchema.CheckConstraintCount) +
            $" Check Constraints:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.CheckConstraintCount} check constraints");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.CheckConstraintCount} check constraints");
        report.AppendLine();
        
        if (result.ConstraintIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.ConstraintIssues)
            {
                report.AppendLine($"     {issue}");
            }
            report.AppendLine();
        }

        report.AppendLine("3. INDEXES");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.IndexCount == result.PostgresSchema.IndexCount) +
            $" Index Count:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.IndexCount} indexes");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.IndexCount} indexes");
        report.AppendLine();
        
        if (result.IndexIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.IndexIssues)
            {
                report.AppendLine($"     {issue}");
            }
            report.AppendLine();
        }

        report.AppendLine("4. CODE OBJECTS (Views, Sequences, Triggers, Procedures)");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.SequenceCount == result.PostgresSchema.SequenceCount) +
            $" Sequences:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.SequenceCount} sequences");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.SequenceCount} sequences");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.ViewCount == result.PostgresSchema.ViewCount) +
            $" Views:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.ViewCount} views");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.ViewCount} views");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.MaterializedViewCount == result.PostgresSchema.MaterializedViewCount) +
            $" Materialized Views:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.MaterializedViewCount} materialized views");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.MaterializedViewCount} materialized views");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.TriggerCount == result.PostgresSchema.TriggerCount) +
            $" Triggers:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.TriggerCount} triggers");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.TriggerCount} triggers");
        report.AppendLine();
        
        var oracleProcCount = result.OracleSchema.ProcedureCount + result.OracleSchema.FunctionCount;
        var postgresProcCount = result.PostgresSchema.ProcedureCount + result.PostgresSchema.FunctionCount;
        report.AppendLine("   " + GetCheckmark(oracleProcCount == postgresProcCount) +
            $" Procedures/Functions:");
        report.AppendLine($"     Oracle:     {oracleProcCount} procedures/functions");
        report.AppendLine($"     PostgreSQL: {postgresProcCount} procedures/functions");
        report.AppendLine();
        
        if (result.CodeObjectIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.CodeObjectIssues)
            {
                report.AppendLine($"     {issue}");
            }
            report.AppendLine();
        }

        report.AppendLine("================================================================================");
        report.AppendLine("COMPARISON SUMMARY");
        report.AppendLine("================================================================================");
        report.AppendLine($"Total Issues Found:    {result.TotalIssues}");
        report.AppendLine($"Critical Issues:       {(result.HasCriticalIssues ? "YES ❌" : "NO ✓")}");
        report.AppendLine($"Migration Quality:     {result.OverallGrade}");
        report.AppendLine();
        
        if (result.TotalIssues == 0)
        {
            report.AppendLine("✓ Schema migration is complete and accurate.");
        }
        else if (!result.HasCriticalIssues)
        {
            report.AppendLine("⚠️ Schema migration completed with minor warnings (review issues above).");
        }
        else
        {
            report.AppendLine("❌ Critical issues found - review and resolve before proceeding.");
        }
        
        report.AppendLine("================================================================================");
        
        return report.ToString();
    }
    
    private string GetCheckmark(bool condition)
    {
        return condition ? "✓" : "⚠️";
    }
}
