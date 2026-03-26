using System.Data;
using Npgsql;
using Serilog;
using Ora2Pg.Common.Connection;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Util;
using Ora2PgSchemaComparer.Model;
using Ora2PgSchemaComparer.Util;
using System.Collections.Concurrent;

namespace Ora2PgSchemaComparer.Extractor;

public class PostgresSchemaExtractor
{
    private readonly ILogger _logger = Log.ForContext<PostgresSchemaExtractor>();
    private readonly DatabaseConnectionManager _connectionManager;
    private readonly HashSet<string> _columnsToSkip;
    private readonly ObjectFilter _objectFilter;
    private readonly bool _enableParallelExtraction;
    private const int CommandTimeoutSeconds = 240;

    public PostgresSchemaExtractor(DatabaseConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;

        var skipColumnsEnv = ApplicationProperties.Instance.Get("POSTGRES_SKIP_COLUMNS", string.Empty);
        _objectFilter = ObjectFilter.FromProperties();
        _columnsToSkip = new HashSet<string>(
            skipColumnsEnv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            StringComparer.OrdinalIgnoreCase
        );
        
        _enableParallelExtraction = ApplicationProperties.Instance.GetInt("PARALLEL_TABLES", 4) > 1;

        if (_columnsToSkip.Any())
        {
            _logger.Information("Columns to skip in PostgreSQL schema: {SkipColumns}", string.Join(", ", _columnsToSkip));
        }
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
        schema.Partitions = ExtractPartitionMetadata(schemaName);
        AttachPartitionMetadata(schema);

        if (_enableParallelExtraction)
        {
            _logger.Information("Extracting schema objects in parallel...");
            
            var tasks = new List<Task>();
            var errors = new ConcurrentBag<string>();
            
            List<ConstraintDefinition>? constraints = null;
            List<IndexDefinition>? indexes = null;
            List<SequenceDefinition>? sequences = null;
            List<ViewDefinition>? views = null;
            List<TriggerDefinition>? triggers = null;
            List<ProcedureDefinition>? procedures = null;

            tasks.Add(Task.Run(() => {
                try { constraints = ExtractConstraints(schemaName); }
                catch (Exception ex) { errors.Add($"Constraints: {ex.Message}"); constraints = new List<ConstraintDefinition>(); }
            }));

            tasks.Add(Task.Run(() => {
                try { indexes = ExtractIndexes(schemaName); }
                catch (Exception ex) { errors.Add($"Indexes: {ex.Message}"); indexes = new List<IndexDefinition>(); }
            }));

            tasks.Add(Task.Run(() => {
                try { sequences = ExtractSequences(schemaName); }
                catch (Exception ex) { errors.Add($"Sequences: {ex.Message}"); sequences = new List<SequenceDefinition>(); }
            }));

            tasks.Add(Task.Run(() => {
                try { views = ExtractViews(schemaName); }
                catch (Exception ex) { errors.Add($"Views: {ex.Message}"); views = new List<ViewDefinition>(); }
            }));

            tasks.Add(Task.Run(() => {
                try { triggers = ExtractTriggers(schemaName); }
                catch (Exception ex) { errors.Add($"Triggers: {ex.Message}"); triggers = new List<TriggerDefinition>(); }
            }));

            tasks.Add(Task.Run(() => {
                try { procedures = ExtractProcedures(schemaName); }
                catch (Exception ex) { errors.Add($"Procedures: {ex.Message}"); procedures = new List<ProcedureDefinition>(); }
            }));

            Task.WaitAll(tasks.ToArray());

            schema.Constraints = constraints!;
            schema.Indexes = indexes!;
            schema.Sequences = sequences!;
            schema.Views = views!;
            schema.Triggers = triggers!;
            schema.Procedures = procedures!;
            
            foreach (var error in errors)
            {
                schema.ExtractionErrors.Add(error);
            }
        }
        else
        {
            schema.Constraints = ExtractConstraints(schemaName);
            schema.Indexes = ExtractIndexes(schemaName);
            schema.Sequences = ExtractSequences(schemaName);
            schema.Views = ExtractViews(schemaName);
            schema.Triggers = ExtractTriggers(schemaName);
            schema.Procedures = ExtractProcedures(schemaName);
        }

        _logger.Information("✓ Extracted PostgreSQL schema: {TableCount} tables, {ConstraintCount} constraints, {IndexCount} indexes",
            schema.TableCount, schema.Constraints.Count, schema.IndexCount);

        return schema;
    }

    private void AttachPartitionMetadata(SchemaDefinition schema)
    {
        if (!schema.Partitions.Any())
        {
            return;
        }

        var tableMap = schema.Tables.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);
        foreach (var metadata in schema.Partitions)
        {
            if (!tableMap.TryGetValue(metadata.ParentTableName, out var table))
            {
                continue;
            }

            table.IsPartitioned = true;
            table.PartitionStrategy = metadata.Strategy;
            table.PartitionColumns = metadata.PartitionColumns.ToList();
            table.Partitions = metadata.Partitions.ToList();
        }
    }

    private List<PartitionMetadata> ExtractPartitionMetadata(string schemaName)
    {
        var partitions = new Dictionary<string, PartitionMetadata>(StringComparer.OrdinalIgnoreCase);

        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();

        var query = @"
            SELECT parent.relname as parent_table,
                   part.partstrat as strategy,
                   pg_get_partkeydef(parent.oid) as partition_key,
                   child.relname as partition_name,
                   pg_get_expr(child.relpartbound, child.oid) as partition_bound
            FROM pg_catalog.pg_partitioned_table part
            JOIN pg_catalog.pg_class parent ON parent.oid = part.partrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = parent.relnamespace
            LEFT JOIN pg_catalog.pg_inherits inh ON inh.inhparent = parent.oid
            LEFT JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
            WHERE n.nspname = $1
            ORDER BY parent.relname, child.relname";

        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var parentTable = reader.GetString(0);
            if (_objectFilter.IsTableExcluded(parentTable, schemaName))
            {
                continue;
            }

            string? strategyCode = null;
            if (!reader.IsDBNull(1))
            {
                var strategyChar = reader.GetFieldValue<char>(1);
                strategyCode = strategyChar.ToString();
            }
            var strategy = PartitionParsing.ParsePostgresStrategy(strategyCode);

            if (!partitions.TryGetValue(parentTable, out var metadata))
            {
                var partitionKeyDefinition = reader.IsDBNull(2) ? null : reader.GetString(2);
                metadata = new PartitionMetadata
                {
                    ParentTableName = parentTable,
                    Strategy = strategy,
                    PartitionColumns = PartitionParsing.ParsePartitionColumns(partitionKeyDefinition)
                };
                partitions[parentTable] = metadata;
            }

            if (!reader.IsDBNull(3))
            {
                var partitionName = reader.GetString(3);
                if (!_objectFilter.IsTableExcluded(partitionName, schemaName))
                {
                    metadata.Partitions.Add(new PartitionDefinition
                    {
                        PartitionName = partitionName,
                        BoundaryDefinition = reader.IsDBNull(4) ? null : reader.GetString(4)
                    });
                }
            }
        }

        return partitions.Values.ToList();
    }

    private List<TableDefinition> ExtractTables(string schemaName)
    {
        var tables = new List<TableDefinition>();

        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();

        var query = @"
            SELECT c.relname as table_name,
                   pg_catalog.obj_description(c.oid, 'pg_class') as table_comment,
                   c.relispartition OR EXISTS(SELECT 1 FROM pg_catalog.pg_partitioned_table WHERE partrelid = c.oid) as is_partitioned
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 
                            AND c.relkind IN ('r', 'p')
            ORDER BY c.relname";

        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var tableName = reader.GetString(0);
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            var table = new TableDefinition
            {
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
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
            SELECT a.attname as column_name,
                   a.attnum as ordinal_position,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                   CASE WHEN t.typname IN ('varchar', 'char', 'bpchar') THEN a.atttypmod - 4 ELSE NULL END as character_maximum_length,
                   CASE WHEN t.typname IN ('numeric', 'decimal') THEN ((a.atttypmod - 4) >> 16) & 65535 ELSE NULL END as numeric_precision,
                   CASE WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535 ELSE NULL END as numeric_scale,
                   NOT a.attnotnull as is_nullable,
                   pg_catalog.pg_get_expr(d.adbin, d.adrelid) as column_default,
                   pg_catalog.col_description(c.oid, a.attnum) as column_comment
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = $1 
              AND c.relname = $2
              AND a.attnum > 0 
              AND NOT a.attisdropped
            ORDER BY a.attnum";

        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        cmd.Parameters.AddWithValue(tableName.ToLower());

        using var reader = cmd.ExecuteReader();
        int totalColumns = 0;
        int skippedColumns = 0;

        while (reader.Read())
        {
            totalColumns++;
            string columnName = reader.GetString(0);

            if (_columnsToSkip.Contains(columnName))
            {
                skippedColumns++;
                _logger.Debug("Skipping column: {ColumnName} in PostgreSQL table {TableName}", columnName, tableName);
                continue;
            }

            columns.Add(new ColumnDefinition
            {
                ColumnName = columnName,
                ColumnPosition = reader.GetInt32(1),
                DataType = reader.GetString(2).ToUpper(),
                DataLength = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                DataPrecision = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                DataScale = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                IsNullable = reader.GetBoolean(6),
                DefaultValue = reader.IsDBNull(7) ? null : reader.GetString(7),
                ColumnComment = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        if (skippedColumns > 0)
        {
            _logger.Information("Skipped {SkippedCount} column(s) in PostgreSQL table {SchemaName}.{TableName} (Total: {TotalCount}, Extracted: {ExtractedCount})",
                skippedColumns, schemaName, tableName, totalColumns, columns.Count);
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
            SELECT con.conname as constraint_name,
                   t.relname as table_name,
                   array_to_string(ARRAY(
                       SELECT a.attname 
                       FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
                       JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.conrelid
                       ORDER BY u.ord
                   ), ',') as columns
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE con.contype = 'p'
              AND n.nspname = $1
            ORDER BY t.relname, con.conname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var tableName = reader.GetString(1);
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            pks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
                Type = ConstraintType.PrimaryKey,
                Columns = reader.GetString(2).Split(',').ToList()
            });
        }
        
        return pks;
    }
    
    private List<ConstraintDefinition> ExtractForeignKeys(string schemaName)
    {
        var fks = new List<ConstraintDefinition>();
        var excludedFKs = new List<string>();
        int totalFKsFound = 0;
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT 
                con.conname as constraint_name,
                t.relname as table_name,
                rn.nspname as ref_schema,
                rt.relname as ref_table,
                CASE con.confdeltype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END as delete_rule,
                CASE con.confupdtype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                END as update_rule,
                con.condeferrable as is_deferrable,
                con.condeferred as is_initially_deferred,
                array_to_string(ARRAY(
                    SELECT a.attname 
                    FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
                    JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.conrelid
                    ORDER BY u.ord
                ), ',') as columns,
                array_to_string(ARRAY(
                    SELECT a.attname 
                    FROM unnest(con.confkey) WITH ORDINALITY AS u(attnum, ord)
                    JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.confrelid
                    ORDER BY u.ord
                ), ',') as ref_columns
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
            JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
            JOIN pg_catalog.pg_class rt ON rt.oid = con.confrelid
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rt.relnamespace
            WHERE con.contype = 'f'
              AND tn.nspname = $1
            ORDER BY t.relname, con.conname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            totalFKsFound++;
            var constraintName = reader.GetString(0);
            var tableName = reader.GetString(1);
            var refSchema = reader.GetString(2);
            var referencedTable = reader.GetString(3);
            
            var tableExcluded = _objectFilter.IsTableExcluded(tableName, schemaName);
            var refTableExcluded = _objectFilter.IsTableExcluded(referencedTable, refSchema);
            var fkIgnored = _objectFilter.IsObjectIgnored("foreignkey", constraintName, schemaName) ||
                           _objectFilter.IsObjectIgnored("fk", constraintName, schemaName);
            
            if (tableExcluded || refTableExcluded || fkIgnored)
            {
                var reason = tableExcluded 
                    ? $"source table '{tableName}' excluded" 
                    : refTableExcluded 
                    ? $"referenced table '{referencedTable}' excluded"
                    : "foreign key in IGNORED_OBJECTS";
                excludedFKs.Add($"{constraintName} ({tableName} → {referencedTable}): {reason}");
                _logger.Debug("Excluding FK {ConstraintName}: {Table} → {RefTable} ({Reason})", 
                    constraintName, tableName, referencedTable, reason);
                continue;
            }

            fks.Add(new ConstraintDefinition
            {
                ConstraintName = constraintName,
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
                Type = ConstraintType.ForeignKey,
                ReferencedSchemaName = refSchema,
                ReferencedTableName = referencedTable,
                OnDeleteRule = reader.GetString(4),
                OnUpdateRule = reader.GetString(5),
                IsDeferrable = reader.GetBoolean(6),
                IsInitiallyDeferred = reader.GetBoolean(7),
                Columns = reader.GetString(8).Split(',').ToList(),
                ReferencedColumns = reader.GetString(9).Split(',').ToList()
            });
        }
        
        if (excludedFKs.Count > 0)
        {
            _logger.Information("PostgreSQL FK Summary: {Total} total, {Included} included, {Excluded} excluded by filters",
                totalFKsFound, fks.Count, excludedFKs.Count);
            foreach (var excluded in excludedFKs)
            {
                _logger.Information("  ⊗ Excluded FK: {Details}", excluded);
            }
        }
        else
        {
            _logger.Information("PostgreSQL FK Summary: {Total} foreign keys extracted (no exclusions)", totalFKsFound);
        }
        
        return fks;
    }
    
    private List<ConstraintDefinition> ExtractUniqueConstraints(string schemaName)
    {
        var uniques = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT con.conname as constraint_name,
                   t.relname as table_name,
                   array_to_string(ARRAY(
                       SELECT a.attname 
                       FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
                       JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.conrelid
                       ORDER BY u.ord
                   ), ',') as columns
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE con.contype = 'u'
              AND n.nspname = $1
            ORDER BY t.relname, con.conname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var tableName = reader.GetString(1);
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            uniques.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
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
            SELECT con.conname as constraint_name,
                   t.relname as table_name,
                   pg_catalog.pg_get_constraintdef(con.oid, true) as check_clause
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE con.contype = 'c'
              AND n.nspname = $1
            ORDER BY t.relname, con.conname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var tableName = reader.GetString(1);
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            checks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
                Type = ConstraintType.Check,
                CheckCondition = reader.GetString(2)
            });
        }
        
        return checks;
    }
    
    private List<IndexDefinition> ExtractIndexes(string schemaName)
    {
        var indexes = new List<IndexDefinition>();
        var excludedIndexes = new List<string>();
        int totalIndexesFound = 0;
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var fkInfoQuery = @"
            SELECT 
                con.conname as constraint_name,
                t.relname as table_name,
                array_to_string(ARRAY(
                    SELECT a.attname 
                    FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
                    JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = con.conrelid
                    ORDER BY u.ord
                ), ',') as fk_columns
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
            JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
            WHERE con.contype = 'f'
              AND tn.nspname = $1";
        
        var fkColumnSets = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var fkConstraintNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        
        using (var fkCmd = new NpgsqlCommand(fkInfoQuery, (NpgsqlConnection)connection))
        {
            fkCmd.CommandTimeout = CommandTimeoutSeconds;
            fkCmd.Parameters.AddWithValue(schemaName.ToLower());
            
            using var fkReader = fkCmd.ExecuteReader();
            while (fkReader.Read())
            {
                var constraintName = fkReader.GetString(0);
                var tableName = fkReader.GetString(1);
                var fkColumns = fkReader.GetString(2);
                
                if (!fkColumnSets.ContainsKey(tableName))
                {
                    fkColumnSets[tableName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }
                fkColumnSets[tableName].Add(fkColumns);
                
                if (!fkConstraintNames.ContainsKey(tableName))
                {
                    fkConstraintNames[tableName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }
                fkConstraintNames[tableName].Add(constraintName);
                
                _logger.Debug("Found FK constraint {ConstraintName} on {Table}({Columns})", 
                    constraintName, tableName, fkColumns);
            }
        }
        
        var query = @"
            SELECT ic.relname as indexname,
                   t.relname as tablename,
                   am.amname,
                   idx.indisunique,
                   array_to_string(ARRAY(
                       SELECT a.attname
                       FROM unnest(idx.indkey) WITH ORDINALITY AS u(attnum, ord)
                       JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = idx.indrelid
                       WHERE a.attnum > 0
                       ORDER BY u.ord
                   ), ',') as index_columns
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class ic ON ic.oid = idx.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = idx.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_am am ON am.oid = ic.relam
            WHERE n.nspname = $1
              AND NOT idx.indisprimary
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_constraint con 
                  WHERE con.conindid = idx.indexrelid AND con.contype IN ('u')
              )
            ORDER BY t.relname, ic.relname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            totalIndexesFound++;
            var indexName = reader.GetString(0);
            var tableName = reader.GetString(1);
            var indexColumns = reader.GetString(4);
            
            if (_objectFilter.IsTableExcluded(tableName, schemaName) ||
                _objectFilter.IsObjectIgnored("index", indexName, schemaName))
            {
                excludedIndexes.Add($"{indexName} on {tableName}: table/index excluded by filter");
                _logger.Debug("Excluding index {IndexName} on {Table}: filtered", indexName, tableName);
                continue;
            }

            var matchesColumns = fkColumnSets.ContainsKey(tableName) &&
                               fkColumnSets[tableName].Contains(indexColumns);

            var containsFkInName = indexName.Contains("(fk)", StringComparison.OrdinalIgnoreCase) ||
                                  System.Text.RegularExpressions.Regex.IsMatch(indexName, @"_fk\d*$", 
                                      System.Text.RegularExpressions.RegexOptions.IgnoreCase) ||
                                  System.Text.RegularExpressions.Regex.IsMatch(indexName, @"^fk[a-z0-9]+", 
                                      System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            
            var matchesConstraintName = false;
            if (fkConstraintNames.ContainsKey(tableName))
            {
                matchesConstraintName = fkConstraintNames[tableName].Any(fkName =>
                    indexName.Equals(fkName, StringComparison.OrdinalIgnoreCase) ||
                    indexName.StartsWith(fkName + "_", StringComparison.OrdinalIgnoreCase));
            }
            
            var isFKSupportingIndex = matchesColumns || containsFkInName || matchesConstraintName;
            
            if (isFKSupportingIndex)
            {
                var reasons = new List<string>();
                if (matchesColumns) reasons.Add("columns match FK");
                if (containsFkInName) reasons.Add("name contains FK pattern");
                if (matchesConstraintName) reasons.Add("name matches FK constraint");
                
                _logger.Debug("Index {IndexName} on {Table}({Columns}) identified as FK-supporting: {Reasons}", 
                    indexName, tableName, indexColumns, string.Join(", ", reasons));
            }

            var amname = reader.GetString(2);
            var index = new IndexDefinition
            {
                IndexName = indexName,
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
                Type = amname == "gin" ? IndexType.GIN : 
                       amname == "gist" ? IndexType.GiST :
                       amname == "hash" ? IndexType.Hash : IndexType.BTree,
                IsUnique = reader.GetBoolean(3),
                Columns = new List<IndexColumnDefinition>() // Simplified - would need additional query
            };
            
            indexes.Add(index);
        }
        
        if (excludedIndexes.Count > 0)
        {
            _logger.Information("PostgreSQL Index Summary: {Total} total, {Included} included, {Excluded} excluded by filters",
                totalIndexesFound, indexes.Count, excludedIndexes.Count);
            foreach (var excluded in excludedIndexes)
            {
                _logger.Information("  ⊗ Excluded Index: {Details}", excluded);
            }
        }
        else
        {
            _logger.Information("PostgreSQL Index Summary: {Total} indexes extracted (no exclusions)", totalIndexesFound);
        }
        
        var fkSupportingCount = indexes.Count(i => 
            fkColumnSets.ContainsKey(i.TableName) && 
            fkColumnSets[i.TableName].Any());
        
        if (fkSupportingCount > 0)
        {
            _logger.Information("  ℹ {Count} indexes identified as FK-supporting indexes (created by DMS for foreign key performance)", 
                fkSupportingCount);
        }
        
        return indexes;
    }
    
    private List<SequenceDefinition> ExtractSequences(string schemaName)
    {
        var sequences = new List<SequenceDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.PostgreSQL);
        connection.Open();
        
        var query = @"
            SELECT sequence_name, 
                   CAST(start_value AS BIGINT) as start_value,
                   CAST(increment AS BIGINT) as increment_by,
                   CAST(minimum_value AS BIGINT) as min_value,
                   CAST(maximum_value AS BIGINT) as max_value,
                   CASE WHEN cycle_option = 'YES' THEN true ELSE false END as cycle
            FROM information_schema.sequences
            WHERE sequence_schema = $1
            ORDER BY sequence_name";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var sequenceName = reader.GetString(0);
            if (_objectFilter.IsObjectIgnored("sequence", sequenceName, schemaName) ||
                IsSequenceForExcludedTable(sequenceName, schemaName))
            {
                continue;
            }

            sequences.Add(new SequenceDefinition
            {
                SequenceName = sequenceName,
                SchemaName = schemaName.ToLower(),
                CurrentValue = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IncrementBy = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                MinValue = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                MaxValue = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                IsCycle = reader.GetBoolean(5)
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
            
            var query = @"
                SELECT c.relname 
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relkind = 'v'
                ORDER BY c.relname";
            using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(schemaName.ToLower());
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var viewName = reader.GetString(0);
                if (_objectFilter.IsObjectIgnored("view", viewName, schemaName))
                {
                    continue;
                }

                views.Add(new ViewDefinition
                {
                    ViewName = viewName,
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
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue(schemaName.ToLower());
            
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var viewName = reader.GetString(0);
                if (_objectFilter.IsObjectIgnored("materialized_view", viewName, schemaName))
                {
                    continue;
                }

                views.Add(new ViewDefinition
                {
                    ViewName = viewName,
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
            SELECT tg.tgname as trigger_name,
                   t.relname as table_name,
                   CASE 
                       WHEN tg.tgtype & 4 = 4 THEN 'INSERT'
                       WHEN tg.tgtype & 8 = 8 THEN 'DELETE'
                       WHEN tg.tgtype & 16 = 16 THEN 'UPDATE'
                       ELSE 'UNKNOWN'
                   END as event_manipulation,
                   CASE WHEN tg.tgtype & 2 = 2 THEN 'BEFORE' ELSE 'AFTER' END as action_timing
            FROM pg_catalog.pg_trigger tg
            JOIN pg_catalog.pg_class t ON t.oid = tg.tgrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND NOT tg.tgisinternal
            ORDER BY t.relname, tg.tgname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var triggerName = reader.GetString(0);
            var tableName = reader.GetString(1);
            if (_objectFilter.IsTableExcluded(tableName, schemaName) ||
                _objectFilter.IsObjectIgnored("trigger", triggerName, schemaName))
            {
                continue;
            }

            triggers.Add(new TriggerDefinition
            {
                TriggerName = triggerName,
                SchemaName = schemaName.ToLower(),
                TableName = tableName,
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
            SELECT p.proname as routine_name,
                   CASE WHEN p.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END as routine_type,
                   pg_catalog.format_type(p.prorettype, NULL) as return_type
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = $1
              AND p.prokind IN ('f', 'p')
            ORDER BY p.proname";
        
        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)connection);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(schemaName.ToLower());
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var procedureName = reader.GetString(0);
            var typeKey = reader.GetString(1) == "FUNCTION" ? "function" : "procedure";

            if (_objectFilter.IsObjectIgnored(typeKey, procedureName, schemaName) ||
                IsProcedureForExcludedObject(procedureName, schemaName))
            {
                continue;
            }

            procedures.Add(new ProcedureDefinition
            {
                ProcedureName = procedureName,
                SchemaName = schemaName.ToLower(),
                Type = reader.GetString(1) == "FUNCTION" ? ProcedureType.Function : ProcedureType.Procedure,
                ReturnType = reader.IsDBNull(2) ? null : reader.GetString(2)
            });
        }
        
        return procedures;
    }

    private bool IsSequenceForExcludedTable(string sequenceName, string schemaName)
    {
        var seqNameLower = sequenceName.ToLower();
        
        // Remove common suffixes
        var withoutSeqSuffix = seqNameLower.Replace("_seq", "").Replace("_id", "");
        
        // Remove common prefixes
        var withoutSeqPrefix = seqNameLower.StartsWith("seq_") ? seqNameLower.Substring(4) : seqNameLower;
        
        // Check if the base name (without SEQ suffix/prefix) matches an excluded table
        var potentialTableNames = new[]
        {
            withoutSeqSuffix,
            withoutSeqPrefix,
            seqNameLower.Replace("_seq", "").Replace("_id_seq", ""), // Handle table_id_seq pattern
        };
        
        foreach (var tableName in potentialTableNames.Distinct())
        {
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                return true;
            }
        }
        
        return false;
    }

    private bool IsProcedureForExcludedObject(string procedureName, string schemaName)
    {
        var procNameLower = procedureName.ToLower();
        
        // Remove common prefixes
        var withoutProcPrefix = procNameLower.StartsWith("proc_") ? procNameLower.Substring(5) :
                               procNameLower.StartsWith("sp_") ? procNameLower.Substring(3) :
                               procNameLower.StartsWith("fn_") ? procNameLower.Substring(3) :
                               procNameLower;
        
        // Remove common suffixes
        var withoutProcSuffix = procNameLower.Replace("_proc", "")
                                            .Replace("_sp", "")
                                            .Replace("_fn", "")
                                            .Replace("_insert", "")
                                            .Replace("_update", "")
                                            .Replace("_delete", "")
                                            .Replace("_select", "")
                                            .Replace("_get", "")
                                            .Replace("_set", "");
        
        // Check if the base name matches an excluded table
        var potentialTableNames = new[]
        {
            withoutProcPrefix,
            withoutProcSuffix,
            procNameLower.Replace("_proc", "").Replace("_insert", "").Replace("_update", "").Replace("_delete", "")
        };
        
        foreach (var tableName in potentialTableNames.Distinct())
        {
            if (string.IsNullOrWhiteSpace(tableName))
                continue;
                
            // Check if table is excluded via patterns
            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                return true;
            }
            
            // Check if table is in IGNORED_OBJECTS
            if (_objectFilter.IsObjectIgnored("table", tableName, schemaName))
            {
                return true;
            }
        }
        
        // Check if the procedure/function belongs to an ignored package (schema in PostgreSQL)
        // PostgreSQL uses schemas, but check for package-like naming conventions
        var potentialPackageNames = new[]
        {
            withoutProcPrefix,
            procNameLower.Split('_')[0] // First part might be package/module name
        };
        
        foreach (var packageName in potentialPackageNames.Distinct())
        {
            if (string.IsNullOrWhiteSpace(packageName))
                continue;
                
            if (_objectFilter.IsObjectIgnored("package", packageName, schemaName))
            {
                return true;
            }
        }
        
        return false;
    }
}
