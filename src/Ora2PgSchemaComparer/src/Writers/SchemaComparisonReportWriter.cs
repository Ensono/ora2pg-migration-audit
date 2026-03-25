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
        
        if (!string.IsNullOrEmpty(result.OracleDatabase))
        {
            report.AppendLine($"Oracle Database:      {result.OracleDatabase}");
        }
        if (!string.IsNullOrEmpty(result.PostgresDatabase))
        {
            report.AppendLine($"PostgreSQL Database:  {result.PostgresDatabase}");
        }
        
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
        report.AppendLine($"     PostgreSQL (Logical): {result.PostgresLogicalColumnCount} columns (excluding DMS rowid)");
        
        if (result.DmsRowidColumnCount > 0)
        {
            report.AppendLine($"     DMS Rowid Columns:   {result.DmsRowidColumnCount} (expected - added by DMS)");
        }
        report.AppendLine();
        
        if (result.TableIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.TableIssues)
            {
                var severity = GetIssueSeverity(issue);
                var prefix = severity switch
                {
                    "Critical" => "     [CRITICAL]",
                    "Warning" => "     [WARNING] ",
                    "Info" => "     [INFO]    ",
                    _ => "     "
                };
                report.AppendLine($"{prefix} {issue}");
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
        
        if (result.SyntheticPrimaryKeyCount > 0)
        {
            report.AppendLine($"     Synthetic:  {result.SyntheticPrimaryKeyCount} synthetic PKs (DMS rowid - expected)");
        }
        
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
                var severity = GetIssueSeverity(issue);
                var prefix = severity switch
                {
                    "Critical" => "     [CRITICAL]",
                    "Warning" => "     [WARNING] ",
                    "Info" => "     [INFO]    ",
                    _ => "     "
                };
                report.AppendLine($"{prefix} {issue}");
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
                var severity = GetIssueSeverity(issue);
                var prefix = severity switch
                {
                    "Critical" => "     [CRITICAL]",
                    "Warning" => "     [WARNING] ",
                    "Info" => "     [INFO]    ",
                    _ => "     "
                };
                report.AppendLine($"{prefix} {issue}");
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
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.ProcedureCount == result.PostgresSchema.ProcedureCount) +
            $" Procedures:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.ProcedureCount} procedures");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.ProcedureCount} procedures");
        report.AppendLine();
        
        report.AppendLine("   " + GetCheckmark(result.OracleSchema.FunctionCount == result.PostgresSchema.FunctionCount) +
            $" Functions:");
        report.AppendLine($"     Oracle:     {result.OracleSchema.FunctionCount} functions");
        report.AppendLine($"     PostgreSQL: {result.PostgresSchema.FunctionCount} functions");
        report.AppendLine();
        
        if (result.CodeObjectIssues.Any())
        {
            report.AppendLine("   Issues Found:");
            foreach (var issue in result.CodeObjectIssues)
            {
                var severity = GetIssueSeverity(issue);
                var prefix = severity switch
                {
                    "Critical" => "     [CRITICAL]",
                    "Warning" => "     [WARNING] ",
                    "Info" => "     [INFO]    ",
                    _ => "     "
                };
                report.AppendLine($"{prefix} {issue}");
            }
            report.AppendLine();
        }

        report.AppendLine("================================================================================");
        report.AppendLine("COMPARISON SUMMARY");
        report.AppendLine("================================================================================");
        report.AppendLine($"Total Issues Found:    {result.TotalIssues}");
        report.AppendLine($"Critical Issues:       {(result.HasCriticalIssues ? "YES ❌" : "NO ✓")}");
        report.AppendLine($"DMS Artifacts:         {result.TotalDmsArtifacts} (expected, not counted as issues)");
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
        
        // DMS Artifacts Section
        if (result.DmsArtifacts.Any())
        {
            report.AppendLine();
            report.AppendLine("================================================================================");
            report.AppendLine("DMS ARTIFACTS (EXPECTED)");
            report.AppendLine("================================================================================");
            report.AppendLine();
            report.AppendLine("The following objects were added by DMS:");
            report.AppendLine();
            
            foreach (var artifact in result.DmsArtifacts)
            {
                var cleanArtifact = artifact.Replace("ℹ️ [DMS Expected] ", "");
                report.AppendLine($"  ℹ️ {cleanArtifact}");
            }
            
            report.AppendLine();
            report.AppendLine($"Summary: {result.TotalDmsArtifacts} DMS artifacts");
            report.AppendLine($"  • Synthetic PKs:    {result.SyntheticPrimaryKeyCount}");
            report.AppendLine($"  • Rowid Columns:    {result.DmsRowidColumnCount}");
            report.AppendLine($"  • Rowid Sequences:  {result.DmsRowidSequenceCount}");
            report.AppendLine($"  • Rowid Indexes:    {result.DmsRowidIndexCount}");
            report.AppendLine();
            report.AppendLine("NOTE: These artifacts are created by DMS for tables without primary keys");
            report.AppendLine("      and do not indicate migration issues.");
        }
        
        bool hasZeroCounts = result.OracleSchema.SequenceCount == 0 || result.OracleSchema.TriggerCount == 0 ||
                             result.OracleSchema.ProcedureCount == 0 || result.OracleSchema.FunctionCount == 0;
        
        if (result.HasOracleExtractionErrors)
        {
            report.AppendLine();
            report.AppendLine("================================================================================");
            report.AppendLine("WARNING: ORACLE CODE OBJECTS EXTRACTION ERRORS");
            report.AppendLine("================================================================================");
            report.AppendLine();
            report.AppendLine("The following errors occurred while extracting Oracle code objects:");
            report.AppendLine();
            foreach (var error in result.OracleExtractionErrors)
            {
                report.AppendLine($"  • {error}");
            }
            report.AppendLine();
            report.AppendLine("These errors may occur due to:");
            report.AppendLine();
            report.AppendLine("  • User Permissions: Database user may lack SELECT privileges on system views");
            report.AppendLine("    (all_sequences, all_triggers, all_objects)");
            report.AppendLine("  • Schema Scope: Objects may exist in different schemas or system accounts");
            report.AppendLine("  • Database Configuration: Oracle configuration may restrict metadata access");
            report.AppendLine();
            report.AppendLine("Check application logs for detailed information and verify database permissions.");
        }
        
        report.AppendLine();
        report.AppendLine("================================================================================");
        
        return report.ToString();
    }
    
    private string GetCheckmark(bool condition)
    {
        return condition ? "✓" : "⚠️";
    }

    private string GetIssueSeverity(string issue)
    {
        if (issue.Contains("❌")) return "Critical";
        if (issue.Contains("⚠️")) return "Warning";
        if (issue.Contains("ℹ️") || issue.Contains("ℹ")) return "Info";
        return "Info";
    }
}
