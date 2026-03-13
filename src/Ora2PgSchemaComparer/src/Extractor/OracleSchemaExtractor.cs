using System.Data;
using Oracle.ManagedDataAccess.Client;
using Serilog;
using Ora2Pg.Common.Connection;
using Ora2Pg.Common.Config;
using Ora2Pg.Common.Util;
using Ora2PgSchemaComparer.Model;
using Ora2PgSchemaComparer.Util;

namespace Ora2PgSchemaComparer.Extractor;


public class OracleSchemaExtractor
{
    private readonly ILogger _logger = Log.ForContext<OracleSchemaExtractor>();
    private readonly DatabaseConnectionManager _connectionManager;
    private readonly HashSet<string> _columnsToSkip;
    private readonly ObjectFilter _objectFilter;

    public OracleSchemaExtractor(DatabaseConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;

        var skipColumnsEnv = ApplicationProperties.Instance.Get("ORACLE_SKIP_COLUMNS", string.Empty);
        _objectFilter = ObjectFilter.FromProperties();
        _columnsToSkip = new HashSet<string>(
            skipColumnsEnv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            StringComparer.OrdinalIgnoreCase
        );

        if (_columnsToSkip.Any())
        {
            _logger.Information("Columns to skip in Oracle schema: {SkipColumns}", string.Join(", ", _columnsToSkip));
        }
    }

    public SchemaDefinition ExtractSchema(string schemaName)
    {
        _logger.Information("Extracting Oracle schema: {SchemaName}", schemaName);

        var schema = new SchemaDefinition
        {
            SchemaName = schemaName.ToUpper(),
            DatabaseType = "Oracle"
        };

        schema.Tables = ExtractTables(schemaName);
        schema.Partitions = ExtractPartitionMetadata(schemaName);
        AttachPartitionMetadata(schema);
        schema.Constraints = ExtractConstraints(schemaName);
        schema.Indexes = ExtractIndexes(schemaName);
        schema.Sequences = ExtractSequencesWithErrorTracking(schemaName, schema);
        schema.Views = ExtractViewsWithErrorTracking(schemaName, schema);
        schema.Triggers = ExtractTriggersWithErrorTracking(schemaName, schema);
        schema.Procedures = ExtractProceduresWithErrorTracking(schemaName, schema);

        _logger.Information("✓ Extracted Oracle schema: {TableCount} tables, {ConstraintCount} constraints, {IndexCount} indexes, {SequenceCount} sequences, {ViewCount} views, {TriggerCount} triggers, {ProcedureCount} procedures/functions",
            schema.TableCount, schema.Constraints.Count, schema.IndexCount, schema.SequenceCount, schema.ViewCount + schema.MaterializedViewCount, schema.TriggerCount, schema.Procedures.Count);

        return schema;
    }

    private List<SequenceDefinition> ExtractSequencesWithErrorTracking(string schemaName, SchemaDefinition schema)
    {
        try
        {
            return ExtractSequences(schemaName);
        }
        catch (Exception ex)
        {
            schema.ExtractionErrors.Add($"Sequences: {ex.Message}");
            return new List<SequenceDefinition>();
        }
    }

    private List<ViewDefinition> ExtractViewsWithErrorTracking(string schemaName, SchemaDefinition schema)
    {
        try
        {
            return ExtractViews(schemaName);
        }
        catch (Exception ex)
        {
            schema.ExtractionErrors.Add($"Views: {ex.Message}");
            return new List<ViewDefinition>();
        }
    }

    private List<TriggerDefinition> ExtractTriggersWithErrorTracking(string schemaName, SchemaDefinition schema)
    {
        try
        {
            return ExtractTriggers(schemaName);
        }
        catch (Exception ex)
        {
            schema.ExtractionErrors.Add($"Triggers: {ex.Message}");
            return new List<TriggerDefinition>();
        }
    }

    private List<ProcedureDefinition> ExtractProceduresWithErrorTracking(string schemaName, SchemaDefinition schema)
    {
        try
        {
            return ExtractProcedures(schemaName);
        }
        catch (Exception ex)
        {
            schema.ExtractionErrors.Add($"Procedures/Functions: {ex.Message}");
            return new List<ProcedureDefinition>();
        }
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

        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();

        var tableQuery = @"
            SELECT pt.table_name,
                   pt.partitioning_type,
                   pk.column_name,
                   pk.column_position
            FROM all_part_tables pt
            LEFT JOIN all_part_key_columns pk
              ON pk.owner = pt.owner AND pk.name = pt.table_name
            WHERE pt.owner = :schema
            ORDER BY pt.table_name, pk.column_position";

        using (var cmd = new OracleCommand(tableQuery, (OracleConnection)connection))
        {
            cmd.Parameters.Add(new OracleParameter("schema", schemaName.ToUpper()));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var tableName = reader.GetString(0);
                if (_objectFilter.IsTableExcluded(tableName, schemaName))
                {
                    continue;
                }

                var strategyName = reader.IsDBNull(1) ? null : reader.GetString(1);
                var strategy = PartitionParsing.ParseOracleStrategy(strategyName);
                if (strategy == PartitionStrategy.None)
                {
                    if (!string.IsNullOrWhiteSpace(strategyName))
                    {
                        _logger.Information("Skipping {Strategy} partition metadata for Oracle table {SchemaName}.{TableName}", strategyName, schemaName, tableName);
                    }

                    continue;
                }

                if (!partitions.TryGetValue(tableName, out var metadata))
                {
                    metadata = new PartitionMetadata
                    {
                        ParentTableName = tableName,
                        Strategy = strategy
                    };
                    partitions[tableName] = metadata;
                }

                if (!reader.IsDBNull(2))
                {
                    metadata.PartitionColumns.Add(reader.GetString(2));
                }
            }
        }

        if (!partitions.Any())
        {
            return partitions.Values.ToList();
        }

         var partitionQuery = @"
             SELECT table_name,
                 partition_name
             FROM all_tab_partitions
             WHERE table_owner = :schema
             ORDER BY table_name, partition_position";

        using (var cmd = new OracleCommand(partitionQuery, (OracleConnection)connection))
        {
            cmd.Parameters.Add(new OracleParameter("schema", schemaName.ToUpper()));

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var tableName = reader.GetString(0);
                if (!partitions.TryGetValue(tableName, out var metadata))
                {
                    continue;
                }

                var partitionName = reader.GetString(1);
                metadata.Partitions.Add(new PartitionDefinition
                {
                    PartitionName = partitionName,
                    BoundaryDefinition = null
                });
            }
        }

        return partitions.Values.ToList();
    }

    private List<TableDefinition> ExtractTables(string schemaName)
    {
        var tables = new List<TableDefinition>();

        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();

        var query = $@"
            SELECT table_name, partitioned,
                   (SELECT comments FROM all_tab_comments WHERE owner = t.owner AND table_name = t.table_name) as table_comment
            FROM all_tables t
            WHERE owner = '{schemaName.ToUpper()}'
            ORDER BY table_name";

        using var cmd = new OracleCommand(query, (OracleConnection)connection);

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
                SchemaName = schemaName.ToUpper(),
                TableName = tableName,
                IsPartitioned = reader.GetString(1) == "YES",
                TableComment = reader.IsDBNull(2) ? null : reader.GetString(2)
            };

            table.Columns = ExtractColumns(schemaName, table.TableName);
            tables.Add(table);
        }

        return tables;
    }

    private List<ColumnDefinition> ExtractColumns(string schemaName, string tableName)
    {
        var columns = new List<ColumnDefinition>();

        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();

        var query = $@"
            SELECT column_name, column_id, data_type, data_length, data_precision, data_scale,
                   nullable, data_default,
                   (SELECT comments FROM all_col_comments WHERE owner = c.owner AND table_name = c.table_name AND column_name = c.column_name) as column_comment
            FROM all_tab_columns c
            WHERE owner = '{schemaName.ToUpper()}' AND table_name = '{tableName.ToUpper()}'
            ORDER BY column_id";

        using var cmd = new OracleCommand(query, (OracleConnection)connection);

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
                _logger.Debug("Skipping column: {ColumnName} in Oracle table {TableName}", columnName, tableName);
                continue;
            }

            columns.Add(new ColumnDefinition
            {
                ColumnName = columnName,
                ColumnPosition = reader.GetInt32(1),
                DataType = reader.GetString(2),
                DataLength = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                DataPrecision = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4)),
                DataScale = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5)),
                IsNullable = reader.GetString(6) == "Y",
                DefaultValue = reader.IsDBNull(7) ? null : reader.GetString(7),
                ColumnComment = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        if (skippedColumns > 0)
        {
            _logger.Information("Skipped {SkippedCount} column(s) in Oracle table {SchemaName}.{TableName} (Total: {TotalCount}, Extracted: {ExtractedCount})",
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
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT c.constraint_name, c.table_name, 
                   LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) as columns
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
            WHERE c.owner = '{schemaName.ToUpper()}' AND c.constraint_type = 'P'
            GROUP BY c.constraint_name, c.table_name
            ORDER BY c.table_name";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
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
                SchemaName = schemaName.ToUpper(),
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
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT c.constraint_name, c.table_name, c.r_owner, rc.table_name as ref_table,
                   c.delete_rule, c.deferrable, c.deferred,
                   LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) as columns,
                   LISTAGG(rcc.column_name, ',') WITHIN GROUP (ORDER BY rcc.position) as ref_columns
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
            JOIN all_constraints rc ON c.r_owner = rc.owner AND c.r_constraint_name = rc.constraint_name
            JOIN all_cons_columns rcc ON rc.owner = rcc.owner AND rc.constraint_name = rcc.constraint_name
            WHERE c.owner = '{schemaName.ToUpper()}' AND c.constraint_type = 'R'
            GROUP BY c.constraint_name, c.table_name, c.r_owner, rc.table_name, c.delete_rule, c.deferrable, c.deferred
            ORDER BY c.table_name";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
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
            
            if (tableExcluded || refTableExcluded)
            {
                var reason = tableExcluded 
                    ? $"source table '{tableName}' excluded" 
                    : $"referenced table '{referencedTable}' excluded";
                excludedFKs.Add($"{constraintName} ({tableName} → {referencedTable}): {reason}");
                _logger.Debug("Excluding FK {ConstraintName}: {Table} → {RefTable} ({Reason})", 
                    constraintName, tableName, referencedTable, reason);
                continue;
            }

            fks.Add(new ConstraintDefinition
            {
                ConstraintName = constraintName,
                SchemaName = schemaName.ToUpper(),
                TableName = tableName,
                Type = ConstraintType.ForeignKey,
                ReferencedSchemaName = refSchema,
                ReferencedTableName = referencedTable,
                OnDeleteRule = reader.GetString(4),
                OnUpdateRule = "NO ACTION", // Oracle doesn't have ON UPDATE
                IsDeferrable = reader.GetString(5) == "DEFERRABLE",
                IsInitiallyDeferred = reader.GetString(6) == "DEFERRED",
                Columns = reader.GetString(7).Split(',').ToList(),
                ReferencedColumns = reader.GetString(8).Split(',').ToList()
            });
        }
        
        if (excludedFKs.Count > 0)
        {
            _logger.Information("Oracle FK Summary: {Total} total, {Included} included, {Excluded} excluded by filters",
                totalFKsFound, fks.Count, excludedFKs.Count);
            foreach (var excluded in excludedFKs)
            {
                _logger.Information("  ⊗ Excluded FK: {Details}", excluded);
            }
        }
        else
        {
            _logger.Information("Oracle FK Summary: {Total} foreign keys extracted (no exclusions)", totalFKsFound);
        }
        
        return fks;
    }
    
    private List<ConstraintDefinition> ExtractUniqueConstraints(string schemaName)
    {
        var uniques = new List<ConstraintDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT c.constraint_name, c.table_name,
                   LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) as columns
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
            WHERE c.owner = '{schemaName.ToUpper()}' AND c.constraint_type = 'U'
            GROUP BY c.constraint_name, c.table_name
            ORDER BY c.table_name";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
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
                SchemaName = schemaName.ToUpper(),
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
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT constraint_name, table_name, search_condition
            FROM all_constraints
            WHERE owner = '{schemaName.ToUpper()}' AND constraint_type = 'C'
            AND constraint_name NOT LIKE 'SYS_%'
            ORDER BY table_name";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var condition = reader.IsDBNull(2) ? null : reader.GetString(2);
            var tableName = reader.GetString(1);

            if (_objectFilter.IsTableExcluded(tableName, schemaName))
            {
                continue;
            }

            if (condition != null && condition.Contains("IS NOT NULL"))
                continue;
            
            checks.Add(new ConstraintDefinition
            {
                ConstraintName = reader.GetString(0),
                SchemaName = schemaName.ToUpper(),
                TableName = tableName,
                Type = ConstraintType.Check,
                CheckCondition = condition
            });
        }
        
        return checks;
    }
    
    private List<IndexDefinition> ExtractIndexes(string schemaName)
    {
        var indexes = new List<IndexDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT i.index_name, i.table_name, i.index_type, i.uniqueness
            FROM all_indexes i
            WHERE i.owner = '{schemaName.ToUpper()}'
            AND i.index_name NOT IN (
                SELECT constraint_name FROM all_constraints 
                WHERE owner = '{schemaName.ToUpper()}' AND constraint_type IN ('P', 'U')
            )
            ORDER BY i.table_name, i.index_name";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var indexName = reader.GetString(0);
            var tableName = reader.GetString(1);
            if (_objectFilter.IsTableExcluded(tableName, schemaName) ||
                _objectFilter.IsObjectIgnored("index", indexName, schemaName))
            {
                continue;
            }

            var indexType = reader.GetString(2);
            var index = new IndexDefinition
            {
                IndexName = indexName,
                SchemaName = schemaName.ToUpper(),
                TableName = tableName,
                Type = indexType == "BITMAP" ? IndexType.Bitmap : IndexType.BTree,
                IsUnique = reader.GetString(3) == "UNIQUE",
                Columns = ExtractIndexColumns(schemaName, indexName)
            };
            
            indexes.Add(index);
        }
        
        return indexes;
    }
    
    private List<IndexColumnDefinition> ExtractIndexColumns(string schemaName, string indexName)
    {
        var columns = new List<IndexColumnDefinition>();
        
        using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
        connection.Open();
        
        var query = $@"
            SELECT column_name, column_position, descend
            FROM all_ind_columns
            WHERE index_owner = '{schemaName.ToUpper()}' AND index_name = '{indexName}'
            ORDER BY column_position";
        
        using var cmd = new OracleCommand(query, (OracleConnection)connection);
        
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            columns.Add(new IndexColumnDefinition
            {
                ColumnName = reader.GetString(0),
                ColumnPosition = reader.GetInt32(1),
                SortOrder = reader.GetString(2) == "DESC" ? "DESC" : "ASC"
            });
        }
        
        return columns;
    }
    
    private List<SequenceDefinition> ExtractSequences(string schemaName)
    {
        var sequences = new List<SequenceDefinition>();
        
        try
        {
            using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
            connection.Open();
            
            // Try dba_sequences first (requires DBA privileges), fallback to all_sequences
            var query = $@"
                SELECT sequence_name, last_number, increment_by, min_value, max_value, 
                       cycle_flag, cache_size
                FROM dba_sequences
                WHERE sequence_owner = '{schemaName.ToUpper()}'
                ORDER BY sequence_name";
            
            try
            {
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
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
                        SchemaName = schemaName.ToUpper(),
                        CurrentValue = reader.IsDBNull(1) ? null : reader.GetDecimal(1),
                        IncrementBy = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                        MinValue = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        MaxValue = reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                        IsCycle = reader.GetString(5) == "Y",
                        CacheSize = reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6))
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle sequences from schema {SchemaName} (using dba_sequences)", sequences.Count, schemaName);
            }
            catch (Oracle.ManagedDataAccess.Client.OracleException ex) when (ex.Number == 942) // ORA-00942: table or view does not exist
            {
                _logger.Warning("No access to dba_sequences, falling back to all_sequences");
                
                // Fallback to all_sequences
                var fallbackQuery = $@"
                    SELECT sequence_name, last_number, increment_by, min_value, max_value, 
                           cycle_flag, cache_size
                    FROM all_sequences
                    WHERE sequence_owner = '{schemaName.ToUpper()}'
                    ORDER BY sequence_name";
                
                using var cmd = new OracleCommand(fallbackQuery, (OracleConnection)connection);
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
                        SchemaName = schemaName.ToUpper(),
                        CurrentValue = reader.IsDBNull(1) ? null : reader.GetDecimal(1),
                        IncrementBy = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                        MinValue = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        MaxValue = reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                        IsCycle = reader.GetString(5) == "Y",
                        CacheSize = reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6))
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle sequences from schema {SchemaName} (using all_sequences)", sequences.Count, schemaName);
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to extract Oracle sequences from schema {SchemaName}", schemaName);
            throw;
        }
        
        return sequences;
    }
    
    private List<ViewDefinition> ExtractViews(string schemaName)
    {
        var views = new List<ViewDefinition>();

        try
        {
            using (var connection = _connectionManager.GetConnection(DatabaseType.Oracle))
            {
                connection.Open();
                
                var query = $"SELECT view_name FROM all_views WHERE owner = '{schemaName.ToUpper()}' ORDER BY view_name";
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
                
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
                        SchemaName = schemaName.ToUpper(),
                        IsMaterialized = false
                    });
                }
            }

            using (var connection = _connectionManager.GetConnection(DatabaseType.Oracle))
            {
                connection.Open();
                
                var query = $"SELECT mview_name, refresh_method FROM all_mviews WHERE owner = '{schemaName.ToUpper()}' ORDER BY mview_name";
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
                
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
                        SchemaName = schemaName.ToUpper(),
                        IsMaterialized = true,
                        RefreshMethod = reader.IsDBNull(1) ? null : reader.GetString(1)
                    });
                }
            }
            
            _logger.Information("✓ Extracted {Count} Oracle views ({MaterializedCount} materialized) from schema {SchemaName}", 
                views.Count, views.Count(v => v.IsMaterialized), schemaName);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to extract Oracle views from schema {SchemaName}", schemaName);
            throw;
        }
        
        return views;
    }
    
    private List<TriggerDefinition> ExtractTriggers(string schemaName)
    {
        var triggers = new List<TriggerDefinition>();
        
        try
        {
            using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
            connection.Open();
            
            // Try dba_triggers first (requires DBA privileges), fallback to all_triggers
            string query;
            
            try
            {
                query = $@"
                    SELECT trigger_name, table_name, triggering_event, trigger_type, status
                    FROM dba_triggers
                    WHERE owner = '{schemaName.ToUpper()}'
                    ORDER BY table_name, trigger_name";
                
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
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
                        SchemaName = schemaName.ToUpper(),
                        TableName = tableName,
                        TriggerEvent = reader.GetString(2),
                        TriggerTiming = reader.GetString(3),
                        IsEnabled = reader.GetString(4) == "ENABLED"
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle triggers from schema {SchemaName} (using dba_triggers)", triggers.Count, schemaName);
            }
            catch (OracleException ex) when (ex.Number == 942) // ORA-00942: table or view does not exist
            {
                _logger.Warning("No access to dba_triggers (ORA-00942), falling back to all_triggers");
                
                query = $@"
                    SELECT trigger_name, table_name, triggering_event, trigger_type, status
                    FROM all_triggers
                    WHERE owner = '{schemaName.ToUpper()}'
                    ORDER BY table_name, trigger_name";
                
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
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
                        SchemaName = schemaName.ToUpper(),
                        TableName = tableName,
                        TriggerEvent = reader.GetString(2),
                        TriggerTiming = reader.GetString(3),
                        IsEnabled = reader.GetString(4) == "ENABLED"
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle triggers from schema {SchemaName} (using all_triggers)", triggers.Count, schemaName);
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to extract Oracle triggers from schema {SchemaName}", schemaName);
            throw;
        }
        
        return triggers;
    }
    
    private List<ProcedureDefinition> ExtractProcedures(string schemaName)
    {
        var procedures = new List<ProcedureDefinition>();
        
        try
        {
            using var connection = _connectionManager.GetConnection(DatabaseType.Oracle);
            connection.Open();
            
            // Try dba_objects first (requires DBA privileges), fallback to all_objects
            try
            {
                var query = $@"
                    SELECT object_name, object_type
                    FROM dba_objects
                    WHERE owner = '{schemaName.ToUpper()}' 
                      AND object_type IN ('PROCEDURE', 'FUNCTION', 'TRIGGER')
                      AND object_name NOT LIKE 'SYS_%'
                      AND status = 'VALID'
                    ORDER BY object_name";
                
                using var cmd = new OracleCommand(query, (OracleConnection)connection);
                using var reader = cmd.ExecuteReader();
                
                while (reader.Read())
                {
                    var procedureName = reader.GetString(0);
                    var objType = reader.GetString(1);
                    
                    // Map Oracle trigger to function (like PostgreSQL does)
                    var typeKey = objType == "FUNCTION" ? "function" : 
                                  objType == "TRIGGER" ? "function" : "procedure";
                    var procedureType = objType == "FUNCTION" || objType == "TRIGGER" ? 
                                       ProcedureType.Function : ProcedureType.Procedure;

                    if (_objectFilter.IsObjectIgnored(typeKey, procedureName, schemaName) ||
                        IsProcedureForExcludedObject(procedureName, schemaName))
                    {
                        continue;
                    }

                    procedures.Add(new ProcedureDefinition
                    {
                        ProcedureName = procedureName,
                        SchemaName = schemaName.ToUpper(),
                        Type = procedureType
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle procedures/functions from schema {SchemaName} (using dba_objects)", procedures.Count, schemaName);
            }
            catch (OracleException ex) when (ex.Number == 942) // ORA-00942: table or view does not exist
            {
                _logger.Warning("No access to dba_objects (ORA-00942), falling back to all_objects");
                
                var fallbackQuery = $@"
                    SELECT object_name, object_type
                    FROM all_objects
                    WHERE owner = '{schemaName.ToUpper()}' 
                      AND object_type IN ('PROCEDURE', 'FUNCTION', 'TRIGGER')
                      AND object_name NOT LIKE 'SYS_%'
                      AND status = 'VALID'
                    ORDER BY object_name";
                
                using var cmd = new OracleCommand(fallbackQuery, (OracleConnection)connection);
                using var reader = cmd.ExecuteReader();
                
                while (reader.Read())
                {
                    var procedureName = reader.GetString(0);
                    var objType = reader.GetString(1);
                    
                    // Map Oracle trigger to function (like PostgreSQL does)
                    var typeKey = objType == "FUNCTION" ? "function" : 
                                  objType == "TRIGGER" ? "function" : "procedure";
                    var procedureType = objType == "FUNCTION" || objType == "TRIGGER" ? 
                                       ProcedureType.Function : ProcedureType.Procedure;

                    if (_objectFilter.IsObjectIgnored(typeKey, procedureName, schemaName) ||
                        IsProcedureForExcludedObject(procedureName, schemaName))
                    {
                        continue;
                    }

                    procedures.Add(new ProcedureDefinition
                    {
                        ProcedureName = procedureName,
                        SchemaName = schemaName.ToUpper(),
                        Type = procedureType
                    });
                }
                
                _logger.Information("✓ Extracted {Count} Oracle procedures/functions from schema {SchemaName} (using all_objects)", procedures.Count, schemaName);
            }
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to extract Oracle procedures from schema {SchemaName}", schemaName);
            throw;
        }
        
        return procedures;
    }


    private bool IsSequenceForExcludedTable(string sequenceName, string schemaName)
    {
        var seqNameUpper = sequenceName.ToUpper();
        
        // Remove common suffixes
        var withoutSeqSuffix = seqNameUpper.Replace("_SEQ", "").Replace("_ID", "");
        
        // Remove common prefixes
        var withoutSeqPrefix = seqNameUpper.StartsWith("SEQ_") ? seqNameUpper.Substring(4) : seqNameUpper;
        
        // Check if the base name (without SEQ suffix/prefix) matches an excluded table
        var potentialTableNames = new[]
        {
            withoutSeqSuffix,
            withoutSeqPrefix,
            seqNameUpper.Replace("_SEQ", "").Replace("_ID_SEQ", ""), // Handle TABLE_ID_SEQ pattern
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
        var procNameUpper = procedureName.ToUpper();
        
        // Remove common prefixes
        var withoutProcPrefix = procNameUpper.StartsWith("PROC_") ? procNameUpper.Substring(5) :
                               procNameUpper.StartsWith("SP_") ? procNameUpper.Substring(3) :
                               procNameUpper.StartsWith("FN_") ? procNameUpper.Substring(3) :
                               procNameUpper.StartsWith("PKG_") ? procNameUpper.Substring(4) :
                               procNameUpper;
        
        // Remove common suffixes
        var withoutProcSuffix = procNameUpper.Replace("_PROC", "")
                                            .Replace("_SP", "")
                                            .Replace("_FN", "")
                                            .Replace("_PKG", "")
                                            .Replace("_INSERT", "")
                                            .Replace("_UPDATE", "")
                                            .Replace("_DELETE", "")
                                            .Replace("_SELECT", "")
                                            .Replace("_GET", "")
                                            .Replace("_SET", "");
        
        var potentialTableNames = new[]
        {
            withoutProcPrefix,
            withoutProcSuffix,
            procNameUpper.Replace("_PROC", "").Replace("_INSERT", "").Replace("_UPDATE", "").Replace("_DELETE", "")
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
        
        // Check if the procedure/function belongs to an ignored package
        // Pattern: PKG_NAME.PROCEDURE_NAME or PKG_NAME_PROCEDURE_NAME
        var potentialPackageNames = new[]
        {
            withoutProcPrefix,
            procNameUpper.Split('_')[0] // First part might be package name
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
