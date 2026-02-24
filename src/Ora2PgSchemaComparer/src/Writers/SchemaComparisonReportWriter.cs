using System.Text;
using Ora2PgSchemaComparer.Comparison;

namespace Ora2PgSchemaComparer.src.Writers;

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
        report.AppendLine($"  Tables:       Oracle={result.OracleLogicalTableCount,-3}  PostgreSQL={result.PostgresLogicalTableCount,-3}");
        report.AppendLine($"  Columns:      Oracle={result.OracleLogicalColumnCount,-3}  PostgreSQL={result.PostgresLogicalColumnCount,-3}");
        report.AppendLine($"  Primary Keys: Oracle={result.OracleLogicalPrimaryKeyCount,-3}  PostgreSQL={result.PostgresLogicalPrimaryKeyCount,-3}");
        report.AppendLine($"  Foreign Keys: Oracle={result.OracleLogicalForeignKeyCount,-3}  PostgreSQL={result.PostgresLogicalForeignKeyCount,-3}");
        report.AppendLine();

        report.AppendLine("1. TABLES & STRUCTURE");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalTableCount == result.PostgresLogicalTableCount) + 
            $" Table Count:");
        report.AppendLine($"     Oracle (Logical):     {result.OracleLogicalTableCount} tables");
        report.AppendLine($"     PostgreSQL (Logical): {result.PostgresLogicalTableCount} tables");
        report.AppendLine($"     PostgreSQL (Physical): {result.PostgresPhysicalTableCount} tables");
        report.AppendLine($"     Partitioned Tables:  {result.PostgresPartitionedTableCount}");
        report.AppendLine($"     Total Partitions:    {result.PostgresPartitionCount}");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalColumnCount == result.PostgresLogicalColumnCount) +
            $" Column Count:");
        report.AppendLine($"     Oracle (Logical):     {result.OracleLogicalColumnCount} columns");
        report.AppendLine($"     PostgreSQL (Logical): {result.PostgresLogicalColumnCount} columns");
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

        if (result.PartitionDetails.Any())
        {
            report.AppendLine("   Partition Details:");
            foreach (var detail in result.PartitionDetails)
            {
                report.AppendLine($"     {detail}");
            }
            report.AppendLine();
        }

        report.AppendLine("2. CONSTRAINTS");
        report.AppendLine("--------------------------------------------------------------------------------");
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalPrimaryKeyCount == result.PostgresLogicalPrimaryKeyCount) +
            $" Primary Keys:");
        report.AppendLine($"     Oracle:     {result.OracleLogicalPrimaryKeyCount} primary keys");
        report.AppendLine($"     PostgreSQL: {result.PostgresLogicalPrimaryKeyCount} primary keys");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalForeignKeyCount == result.PostgresLogicalForeignKeyCount) +
            $" Foreign Keys:");
        report.AppendLine($"     Oracle:     {result.OracleLogicalForeignKeyCount} foreign keys");
        report.AppendLine($"     PostgreSQL: {result.PostgresLogicalForeignKeyCount} foreign keys");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalUniqueConstraintCount == result.PostgresLogicalUniqueConstraintCount) +
            $" Unique Constraints:");
        report.AppendLine($"     Oracle:     {result.OracleLogicalUniqueConstraintCount} unique constraints");
        report.AppendLine($"     PostgreSQL: {result.PostgresLogicalUniqueConstraintCount} unique constraints");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalCheckConstraintCount == result.PostgresLogicalCheckConstraintCount) +
            $" Check Constraints:");
        report.AppendLine($"     Oracle:     {result.OracleLogicalCheckConstraintCount} check constraints");
        report.AppendLine($"     PostgreSQL: {result.PostgresLogicalCheckConstraintCount} check constraints");
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
        report.AppendLine("   " + GetCheckmark(result.OracleLogicalIndexCount == result.PostgresLogicalIndexCount) +
            $" Index Count:");
        report.AppendLine($"     Oracle:     {result.OracleLogicalIndexCount} indexes");
        report.AppendLine($"     PostgreSQL: {result.PostgresLogicalIndexCount} indexes");
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
