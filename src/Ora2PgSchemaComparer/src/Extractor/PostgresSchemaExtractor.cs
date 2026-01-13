using Npgsql;
using Serilog;
using Ora2Pg.Common.Connection;
using Ora2PgSchemaComparer.Model;

namespace Ora2PgSchemaComparer.Extractor;

public class PostgresSchemaExtractor
{
    private readonly ILogger _logger = Log.ForContext<PostgresSchemaExtractor>();
    private readonly DatabaseConnectionManager _connectionManager;
    
    public PostgresSchemaExtractor(DatabaseConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
    }
    
    public SchemaDefinition ExtractSchema(string schemaName)
    {
        _logger.Information("Extracting PostgreSQL schema: {SchemaName}", schemaName);
        
        var schema = new SchemaDefinition
        {
            SchemaName = schemaName.ToLower(),
            DatabaseType = "PostgreSQL"
        };
        
        schema.Tables = ExtractTables(schemaName);
        schema.Constraints = ExtractConstraints(schemaName);
        schema.Indexes = ExtractIndexes(schemaName);
        schema.Sequences = ExtractSequences(schemaName);
        schema.Views = ExtractViews(schemaName);
        schema.Triggers = ExtractTriggers(schemaName);
        schema.Procedures = ExtractProcedures(schemaName);
        
        _logger.Information("✓ Extracted PostgreSQL schema: {TableCount} tables, {ConstraintCount} constraints, {IndexCount} indexes",
            schema.TableCount, schema.Constraints.Count, schema.IndexCount);
        
        return schema;
    }
    
    private List<TableDefinition> ExtractTables(string schemaName)
    {
        var tables = new List<TableDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT t.table_name,
                   pg_catalog.obj_description((quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass, 'pg_class') as table_comment,
                   EXISTS(SELECT 1 FROM pg_catalog.pg_partitioned_table WHERE partrelid = (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass) as is_partitioned
            FROM information_schema.tables t
            WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var table = new TableDefinition
            {
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(0),
                TableComment = reader.IsDBNull(1) ? null : reader.GetString(1),
                IsPartitioned = reader.GetBoolean(2)
            };
            
            table.Columns = ExtractColumns(schemaName, table.TableName);
            tables.Add(table);
        }
        
        return tables;
    }
    
    private List<ColumnDefinition> ExtractColumns(string schemaName, string tableName)
    {
        var columns = new List<ColumnDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT c.column_name, c.ordinal_position, c.data_type, 
                   c.character_maximum_length, c.numeric_precision, c.numeric_scale,
                   c.is_nullable, c.column_default,
                   pg_catalog.col_description((quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass, c.ordinal_position) as column_comment
            FROM information_schema.columns c
            WHERE c.table_schema = $1 AND c.table_name = $2
            ORDER BY c.ordinal_position";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        cmd.Parameters.AddWithValue(tableName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            columns.Add(new ColumnDefinition
            {
                ColumnName = reader.GetString(0),
                ColumnPosition = reader.GetInt32(1),
                DataType = reader.GetString(2).ToUpper(),
                DataLength = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                DataPrecision = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                DataScale = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                IsNullable = reader.GetString(6) == "YES",
                DefaultValue = reader.IsDBNull(7) ? null : reader.GetString(7),
                ColumnComment = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }
        
        return columns;
    }
    
    private List<ConstraintDefinition> ExtractConstraints(string schemaName)
    {
        var constraints = new List<ConstraintDefinition>();
        
        constraints.AddRange(ExtractPrimaryKeys(schemaName));
        constraints.AddRange(ExtractForeignKeys(schemaName));
        constraints.AddRange(ExtractUniqueConstraints(schemaName));
        constraints.AddRange(ExtractCheckConstraints(schemaName));
        
        return constraints;
    }
    
    private List<ConstraintDefinition> ExtractPrimaryKeys(string schemaName)
    {
        var pks = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT tc.constraint_name, tc.table_name,
                   string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_schema = kcu.constraint_schema 
                AND tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_schema = $1 AND tc.constraint_type = 'PRIMARY KEY'
            GROUP BY tc.constraint_name, tc.table_name
            ORDER BY tc.table_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            pks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                Type = ConstraintType.PrimaryKey,
                Columns = reader.GetString(2).Split(',').ToList()
            });
        }
        
        return pks;
    }
    
    private List<ConstraintDefinition> ExtractForeignKeys(string schemaName)
    {
        var fks = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT tc.constraint_name, tc.table_name,
                   ccu.table_schema as ref_schema, ccu.table_name as ref_table,
                   rc.delete_rule, rc.update_rule,
                   CASE WHEN con.condeferrable THEN true ELSE false END as is_deferrable,
                   CASE WHEN con.condeferred THEN true ELSE false END as is_initially_deferred,
                   string_agg(DISTINCT kcu.column_name, ',' ORDER BY kcu.column_name) as columns,
                   string_agg(DISTINCT ccu.column_name, ',' ORDER BY ccu.column_name) as ref_columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.constraint_schema = kcu.constraint_schema
            JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.constraint_schema = ccu.constraint_schema
            JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name AND tc.constraint_schema = rc.constraint_schema
            JOIN pg_catalog.pg_constraint con ON tc.constraint_name = con.conname
            WHERE tc.constraint_schema = $1 AND tc.constraint_type = 'FOREIGN KEY'
            GROUP BY tc.constraint_name, tc.table_name, ccu.table_schema, ccu.table_name, rc.delete_rule, rc.update_rule, con.condeferrable, con.condeferred
            ORDER BY tc.table_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            fks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                Type = ConstraintType.ForeignKey,
                ReferencedSchemaName = reader.GetString(2),
                ReferencedTableName = reader.GetString(3),
                OnDeleteRule = reader.GetString(4),
                OnUpdateRule = reader.GetString(5),
                IsDeferrable = reader.GetBoolean(6),
                IsInitiallyDeferred = reader.GetBoolean(7),
                Columns = reader.GetString(8).Split(',').ToList(),
                ReferencedColumns = reader.GetString(9).Split(',').ToList()
            });
        }
        
        return fks;
    }
    
    private List<ConstraintDefinition> ExtractUniqueConstraints(string schemaName)
    {
        var uniques = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT tc.constraint_name, tc.table_name,
                   string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_schema = kcu.constraint_schema 
                AND tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_schema = $1 AND tc.constraint_type = 'UNIQUE'
            GROUP BY tc.constraint_name, tc.table_name
            ORDER BY tc.table_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            uniques.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                Type = ConstraintType.Unique,
                Columns = reader.GetString(2).Split(',').ToList()
            });
        }
        
        return uniques;
    }
    
    private List<ConstraintDefinition> ExtractCheckConstraints(string schemaName)
    {
        var checks = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT tc.constraint_name, tc.table_name, cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.constraint_schema = $1 AND tc.constraint_type = 'CHECK'
            ORDER BY tc.table_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            checks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                Type = ConstraintType.Check,
                CheckCondition = reader.GetString(2)
            });
        }
        
        return checks;
    }
    
    private List<IndexDefinition> ExtractIndexes(string schemaName)
    {
        var indexes = new List<IndexDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT i.indexname, i.tablename, am.amname, idx.indisunique
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            JOIN pg_index idx ON idx.indexrelid = c.oid
            JOIN pg_am am ON am.oid = c.relam
            WHERE i.schemaname = $1
            AND i.indexname NOT IN (
                SELECT constraint_name FROM information_schema.table_constraints 
                WHERE constraint_schema = $1 AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            )
            ORDER BY i.tablename, i.indexname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var amname = reader.GetString(2);
            var index = new IndexDefinition
            {
                IndexName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                Type = amname == "gin" ? IndexType.GIN : 
                       amname == "gist" ? IndexType.GiST :
                       amname == "hash" ? IndexType.Hash : IndexType.BTree,
                IsUnique = reader.GetBoolean(3),
                Columns = new List<IndexColumnDefinition>() // Simplified - would need additional query
            };
            
            indexes.Add(index);
        }
        
        return indexes;
    }
    
    private List<SequenceDefinition> ExtractSequences(string schemaName)
    {
        var sequences = new List<SequenceDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT sequence_name, start_value, increment, minimum_value, maximum_value, cycle_option
            FROM information_schema.sequences
            WHERE sequence_schema = $1
            ORDER BY sequence_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            sequences.Add(new SequenceDefinition
            {
                SequenceName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                CurrentValue = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IncrementBy = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                MinValue = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                MaxValue = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                IsCycle = reader.GetString(5) == "YES"
            });
        }
        
        return sequences;
    }
    
    private List<ViewDefinition> ExtractViews(string schemaName)
    {
        var views = new List<ViewDefinition>();
        

        using (var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL))
        {
            connection.Open();
            
            var query = "SELECT table_name FROM information_schema.views WHERE table_schema = $1 ORDER BY table_name";
            using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
            cmd.Parameters.AddWithValue(schemaName.ToLower());
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                views.Add(new ViewDefinition
                {
                    ViewName = reader.GetString(0),
                    SchemaName = schemaName.ToLower(),
                    IsMaterialized = false
                });
            }
        }
        

        using (var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL))
        {
            connection.Open();
            
            var query = @"
                SELECT matviewname 
                FROM pg_matviews 
                WHERE schemaname = $1 
                ORDER BY matviewname";
            using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
            cmd.Parameters.AddWithValue(schemaName.ToLower());
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                views.Add(new ViewDefinition
                {
                    ViewName = reader.GetString(0),
                    SchemaName = schemaName.ToLower(),
                    IsMaterialized = true
                });
            }
        }
        
        return views;
    }
    
    private List<TriggerDefinition> ExtractTriggers(string schemaName)
    {
        var triggers = new List<TriggerDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT trigger_name, event_object_table, event_manipulation, action_timing
            FROM information_schema.triggers
            WHERE trigger_schema = $1
            ORDER BY event_object_table, trigger_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            triggers.Add(new TriggerDefinition
            {
                TriggerName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = reader.GetString(1),
                TriggerEvent = reader.GetString(2),
                TriggerTiming = reader.GetString(3),
                IsEnabled = true // PostgreSQL doesn't easily expose this
            });
        }
        
        return triggers;
    }
    
    private List<ProcedureDefinition> ExtractProcedures(string schemaName)
    {
        var procedures = new List<ProcedureDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT routine_name, routine_type, data_type
            FROM information_schema.routines
            WHERE routine_schema = $1
            ORDER BY routine_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            procedures.Add(new ProcedureDefinition
            {
                ProcedureName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                Type = reader.GetString(1) == "FUNCTION" ? ProcedureType.Function : ProcedureType.Procedure,
                ReturnType = reader.IsDBNull(2) ? null : reader.GetString(2)
            });
        }
        
        return procedures;
    }
}
