package com.mode.trino.honeycomb;

import io.airlift.slice.Slice;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorResolvedIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class LoggingConnectorMetadata implements ConnectorMetadata {
    private static final Logger LOGGER = Logger.getLogger(LoggingConnectorMetadata.class.getCanonicalName());

    private final ConnectorMetadata delegate;

    public LoggingConnectorMetadata(ConnectorMetadata delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        LOGGER.info("Calling schemaExists([io.trino.spi.connector.ConnectorSession, java.lang.String])");
        return delegate.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        LOGGER.info("Calling listSchemaNames([io.trino.spi.connector.ConnectorSession])");
        return delegate.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                               SchemaTableName tableName) {
        LOGGER.info("Calling getTableHandle([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.getTableHandle(session, tableName);
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(
        ConnectorSession session, SchemaTableName tableName,
        Map<String, Object> analyzeProperties) {
        LOGGER.info("Calling getTableHandleForStatisticsCollection([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, java.util.Map<java.lang.String,java.lang.Object>])");
        return delegate.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
        ConnectorSession session, ConnectorTableHandle tableHandle,
        String procedureName, Map<String, Object> executeProperties) {
        LOGGER.info("Calling getTableHandleForExecute([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.lang.String, java.util.Map<java.lang.String,java.lang.Object>])");
        return delegate.getTableHandleForExecute(session, tableHandle, procedureName, executeProperties);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getLayoutForTableExecute(
        ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle) {
        LOGGER.info("Calling getLayoutForTableExecute([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableExecuteHandle])");
        return delegate.getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
        ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle,
        ConnectorTableHandle updatedSourceTableHandle) {
        LOGGER.info("Calling beginTableExecute([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableExecuteHandle, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.beginTableExecute(session, tableExecuteHandle, updatedSourceTableHandle);
    }

    @Override
    public void finishTableExecute(ConnectorSession session,
                                   ConnectorTableExecuteHandle tableExecuteHandle,
                                   Collection<Slice> fragments,
                                   List<Object> tableExecuteState) {
        LOGGER.info("Calling finishTableExecute([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableExecuteHandle, java.util.Collection<io.airlift.slice.Slice>, java.util.List<java.lang.Object>])");
        delegate.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session,
                                                SchemaTableName tableName) {
        LOGGER.info("Calling getSystemTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    @Deprecated
    public List<ConnectorTableLayoutResult> getTableLayouts(
        ConnectorSession session, ConnectorTableHandle table,
        Constraint constraint,
        Optional<Set<ColumnHandle>> desiredColumns) {
        LOGGER.info("Calling getTableLayouts([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.Constraint, java.util.Optional<java.util.Set<io.trino.spi.connector.ColumnHandle>>])");
        return delegate.getTableLayouts(session, table, constraint, desiredColumns);
    }

    @Override
    @Deprecated
    public ConnectorTableLayout getTableLayout(ConnectorSession session,
                                               ConnectorTableLayoutHandle handle) {
        LOGGER.info("Calling getTableLayout([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableLayoutHandle])");
        return delegate.getTableLayout(session, handle);
    }

    @Override
    @Deprecated
    public ConnectorTableLayoutHandle makeCompatiblePartitioning(
        ConnectorSession session, ConnectorTableLayoutHandle tableLayoutHandle,
        ConnectorPartitioningHandle partitioningHandle) {
        LOGGER.info("Calling makeCompatiblePartitioning([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableLayoutHandle, io.trino.spi.connector.ConnectorPartitioningHandle])");
        return delegate.makeCompatiblePartitioning(session, tableLayoutHandle, partitioningHandle);
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session,
                                                           ConnectorTableHandle tableHandle,
                                                           ConnectorPartitioningHandle partitioningHandle) {
        LOGGER.info("Calling makeCompatiblePartitioning([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ConnectorPartitioningHandle])");
        return delegate.makeCompatiblePartitioning(session, tableHandle, partitioningHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(
        ConnectorSession session, ConnectorPartitioningHandle left,
        ConnectorPartitioningHandle right) {
        LOGGER.info("Calling getCommonPartitioningHandle([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorPartitioningHandle, io.trino.spi.connector.ConnectorPartitioningHandle])");
        return delegate.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session,
                                               ConnectorTableHandle table) {
        LOGGER.info("Calling getTableSchema([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getTableSchema(session, table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                   ConnectorTableHandle table) {
        LOGGER.info("Calling getTableMetadata([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getTableMetadata(session, table);
    }

    @Override
    @Deprecated
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle) {
        LOGGER.info("Calling getInfo([io.trino.spi.connector.ConnectorTableLayoutHandle])");
        return delegate.getInfo(layoutHandle);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table) {
        LOGGER.info("Calling getInfo([io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getInfo(table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session,
                                            Optional<String> schemaName) {
        LOGGER.info("Calling listTables([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.lang.String>])");
        return delegate.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling getColumnHandles([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        LOGGER.info("Calling getColumnMetadata([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ColumnHandle])");
        return delegate.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    @Deprecated
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
        ConnectorSession session, SchemaTablePrefix prefix) {
        LOGGER.info("Calling listTableColumns([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTablePrefix])");
        return delegate.listTableColumns(session, prefix);
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(
        ConnectorSession session, SchemaTablePrefix prefix) {
        LOGGER.info("Calling streamTableColumns([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTablePrefix])");
        return delegate.streamTableColumns(session, prefix);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session,
                                              ConnectorTableHandle tableHandle,
                                              Constraint constraint) {
        LOGGER.info("Calling getTableStatistics([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.Constraint])");
        return delegate.getTableStatistics(session, tableHandle, constraint);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName,
                             Map<String, Object> properties, TrinoPrincipal owner) {
        LOGGER.info("Calling createSchema([io.trino.spi.connector.ConnectorSession, java.lang.String, java.util.Map<java.lang.String,java.lang.Object>, io.trino.spi.security.TrinoPrincipal])");
        delegate.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        LOGGER.info("Calling dropSchema([io.trino.spi.connector.ConnectorSession, java.lang.String])");
        delegate.dropSchema(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target) {
        LOGGER.info("Calling renameSchema([io.trino.spi.connector.ConnectorSession, java.lang.String, java.lang.String])");
        delegate.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source,
                                       TrinoPrincipal principal) {
        LOGGER.info("Calling setSchemaAuthorization([io.trino.spi.connector.ConnectorSession, java.lang.String, io.trino.spi.security.TrinoPrincipal])");
        delegate.setSchemaAuthorization(session, source, principal);
    }

    @Override
    public void createTable(ConnectorSession session,
                            ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
        LOGGER.info("Calling createTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableMetadata, boolean])");
        delegate.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling dropTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        delegate.dropTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle,
                            SchemaTableName newTableName) {
        LOGGER.info("Calling renameTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.SchemaTableName])");
        delegate.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session,
                                   ConnectorTableHandle tableHandle,
                                   Map<String, Object> properties) {
        LOGGER.info("Calling setTableProperties([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Map<java.lang.String,java.lang.Object>])");
        delegate.setTableProperties(session, tableHandle, properties);
    }

    @Override
    public void setTableComment(ConnectorSession session,
                                ConnectorTableHandle tableHandle, Optional<String> comment) {
        LOGGER.info("Calling setTableComment([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Optional<java.lang.String>])");
        delegate.setTableComment(session, tableHandle, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session,
                                 ConnectorTableHandle tableHandle, ColumnHandle column,
                                 Optional<String> comment) {
        LOGGER.info("Calling setColumnComment([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ColumnHandle, java.util.Optional<java.lang.String>])");
        delegate.setColumnComment(session, tableHandle, column, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
                          ColumnMetadata column) {
        LOGGER.info("Calling addColumn([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ColumnMetadata])");
        delegate.addColumn(session, tableHandle, column);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session,
                                      SchemaTableName tableName,
                                      TrinoPrincipal principal) {
        LOGGER.info("Calling setTableAuthorization([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.security.TrinoPrincipal])");
        delegate.setTableAuthorization(session, tableName, principal);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
                             ColumnHandle source, String target) {
        LOGGER.info("Calling renameColumn([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ColumnHandle, java.lang.String])");
        delegate.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle,
                           ColumnHandle column) {
        LOGGER.info("Calling dropColumn([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ColumnHandle])");
        delegate.dropColumn(session, tableHandle, column);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(
        ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        LOGGER.info("Calling getNewTableLayout([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableMetadata])");
        return delegate.getNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(
        ConnectorSession session, ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling getInsertLayout([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getInsertLayout(session, tableHandle);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(
        ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        LOGGER.info("Calling getStatisticsCollectionMetadataForWrite([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableMetadata])");
        return delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(
        ConnectorSession session, ConnectorTableMetadata tableMetadata) {
        LOGGER.info("Calling getStatisticsCollectionMetadata([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableMetadata])");
        return delegate.getStatisticsCollectionMetadata(session, tableMetadata);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling beginStatisticsCollection([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session,
                                           ConnectorTableHandle tableHandle,
                                           Collection<ComputedStatistics> computedStatistics) {
        LOGGER.info("Calling finishStatisticsCollection([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Collection<io.trino.spi.statistics.ComputedStatistics>])");
        delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
                                                       ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout) {
        LOGGER.info("Calling beginCreateTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableMetadata, java.util.Optional<io.trino.spi.connector.ConnectorNewTableLayout>])");
        return delegate.beginCreateTable(session, tableMetadata, layout);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
        ConnectorSession session, ConnectorOutputTableHandle tableHandle,
        Collection<Slice> fragments,
        Collection<ComputedStatistics> computedStatistics) {
        LOGGER.info("Calling finishCreateTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorOutputTableHandle, java.util.Collection<io.airlift.slice.Slice>, java.util.Collection<io.trino.spi.statistics.ComputedStatistics>])");
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session) {
        LOGGER.info("Calling beginQuery([io.trino.spi.connector.ConnectorSession])");
        delegate.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session) {
        LOGGER.info("Calling cleanupQuery([io.trino.spi.connector.ConnectorSession])");
        delegate.cleanupQuery(session);
    }

    @Override
    @Deprecated
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
                                                  ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling beginInsert([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.beginInsert(session, tableHandle);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
                                                  ConnectorTableHandle tableHandle,
                                                  List<ColumnHandle> columns) {
        LOGGER.info("Calling beginInsert([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.ColumnHandle>])");
        return delegate.beginInsert(session, tableHandle, columns);
    }

    @Override
    public boolean supportsMissingColumnsOnInsert() {
        LOGGER.info("Calling supportsMissingColumnsOnInsert([])");
        return delegate.supportsMissingColumnsOnInsert();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
        ConnectorSession session, ConnectorInsertTableHandle insertHandle,
        Collection<Slice> fragments,
        Collection<ComputedStatistics> computedStatistics) {
        LOGGER.info("Calling finishInsert([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorInsertTableHandle, java.util.Collection<io.airlift.slice.Slice>, java.util.Collection<io.trino.spi.statistics.ComputedStatistics>])");
        return delegate.finishInsert(session, insertHandle, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session,
                                                              SchemaTableName viewName) {
        LOGGER.info("Calling delegateMaterializedViewRefreshToConnector([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session,
                                                        SchemaTableName viewName) {
        LOGGER.info("Calling refreshMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.refreshMaterializedView(session, viewName);
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(
        ConnectorSession session, ConnectorTableHandle tableHandle,
        List<ConnectorTableHandle> sourceTableHandles) {
        LOGGER.info("Calling beginRefreshMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.ConnectorTableHandle>])");
        return delegate.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
        ConnectorSession session, ConnectorTableHandle tableHandle,
        ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
        Collection<ComputedStatistics> computedStatistics,
        List<ConnectorTableHandle> sourceTableHandles) {
        LOGGER.info("Calling finishRefreshMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ConnectorInsertTableHandle, java.util.Collection<io.airlift.slice.Slice>, java.util.Collection<io.trino.spi.statistics.ComputedStatistics>, java.util.List<io.trino.spi.connector.ConnectorTableHandle>])");
        return delegate
            .finishRefreshMaterializedView(session, tableHandle, insertHandle, fragments, computedStatistics, sourceTableHandles);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session,
                                                   ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling getDeleteRowIdColumnHandle([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getDeleteRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session,
                                                   ConnectorTableHandle tableHandle,
                                                   List<ColumnHandle> updatedColumns) {
        LOGGER.info("Calling getUpdateRowIdColumnHandle([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.ColumnHandle>])");
        return delegate.getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session,
                                            ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling beginDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle,
                             Collection<Slice> fragments) {
        LOGGER.info("Calling finishDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Collection<io.airlift.slice.Slice>])");
        delegate.finishDelete(session, tableHandle, fragments);
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            List<ColumnHandle> updatedColumns) {
        LOGGER.info("Calling beginUpdate([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.ColumnHandle>])");
        return delegate.beginUpdate(session, tableHandle, updatedColumns);
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle,
                             Collection<Slice> fragments) {
        LOGGER.info("Calling finishUpdate([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Collection<io.airlift.slice.Slice>])");
        delegate.finishUpdate(session, tableHandle, fragments);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName,
                           ConnectorViewDefinition definition, boolean replace) {
        LOGGER.info("Calling createView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.connector.ConnectorViewDefinition, boolean])");
        delegate.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source,
                           SchemaTableName target) {
        LOGGER.info("Calling renameView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.connector.SchemaTableName])");
        delegate.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName,
                                     TrinoPrincipal principal) {
        LOGGER.info("Calling setViewAuthorization([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.security.TrinoPrincipal])");
        delegate.setViewAuthorization(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        LOGGER.info("Calling dropView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        delegate.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session,
                                           Optional<String> schemaName) {
        LOGGER.info("Calling listViews([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.lang.String>])");
        return delegate.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(
        ConnectorSession session, Optional<String> schemaName) {
        LOGGER.info("Calling getViews([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.lang.String>])");
        return delegate.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session,
                                                     SchemaTableName viewName) {
        LOGGER.info("Calling getView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.getView(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session,
                                                   CatalogSchemaName schemaName) {
        LOGGER.info("Calling getSchemaProperties([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.CatalogSchemaName])");
        return delegate.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session,
                                                   CatalogSchemaName schemaName) {
        LOGGER.info("Calling getSchemaOwner([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.CatalogSchemaName])");
        return delegate.getSchemaOwner(session, schemaName);
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session,
                                          ConnectorTableHandle tableHandle,
                                          ConnectorTableLayoutHandle tableLayoutHandle) {
        LOGGER.info("Calling supportsMetadataDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ConnectorTableLayoutHandle])");
        return delegate.supportsMetadataDelete(session, tableHandle, tableLayoutHandle);
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session,
                                       ConnectorTableHandle tableHandle,
                                       ConnectorTableLayoutHandle tableLayoutHandle) {
        LOGGER.info("Calling metadataDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ConnectorTableLayoutHandle])");
        return delegate.metadataDelete(session, tableHandle, tableLayoutHandle);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(
        ConnectorSession session, ConnectorTableHandle handle) {
        LOGGER.info("Calling applyDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session,
                                      ConnectorTableHandle handle) {
        LOGGER.info("Calling executeDelete([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.executeDelete(session, handle);
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(
        ConnectorSession session, ConnectorTableHandle tableHandle,
        Set<ColumnHandle> indexableColumns,
        Set<ColumnHandle> outputColumns,
        TupleDomain<ColumnHandle> tupleDomain) {
        LOGGER.info("Calling resolveIndex([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.Set<io.trino.spi.connector.ColumnHandle>, java.util.Set<io.trino.spi.connector.ColumnHandle>, io.trino.spi.predicate.TupleDomain<io.trino.spi.connector.ColumnHandle>])");
        return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role) {
        LOGGER.info("Calling roleExists([io.trino.spi.connector.ConnectorSession, java.lang.String])");
        return delegate.roleExists(session, role);
    }

    @Override
    public void createRole(ConnectorSession session, String role,
                           Optional<TrinoPrincipal> grantor) {
        LOGGER.info("Calling createRole([io.trino.spi.connector.ConnectorSession, java.lang.String, java.util.Optional<io.trino.spi.security.TrinoPrincipal>])");
        delegate.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role) {
        LOGGER.info("Calling dropRole([io.trino.spi.connector.ConnectorSession, java.lang.String])");
        delegate.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session) {
        LOGGER.info("Calling listRoles([io.trino.spi.connector.ConnectorSession])");
        return delegate.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session,
                                         TrinoPrincipal principal) {
        LOGGER.info("Calling listRoleGrants([io.trino.spi.connector.ConnectorSession, io.trino.spi.security.TrinoPrincipal])");
        return delegate.listRoleGrants(session, principal);
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(ConnectorSession session,
                                            Optional<Set<String>> roles,
                                            Optional<Set<String>> grantees,
                                            OptionalLong limit) {
        LOGGER.info("Calling listAllRoleGrants([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.util.Set<java.lang.String>>, java.util.Optional<java.util.Set<java.lang.String>>, java.util.OptionalLong])");
        return delegate.listAllRoleGrants(session, roles, grantees, limit);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles,
                           Set<TrinoPrincipal> grantees, boolean adminOption,
                           Optional<TrinoPrincipal> grantor) {
        LOGGER.info("Calling grantRoles([io.trino.spi.connector.ConnectorSession, java.util.Set<java.lang.String>, java.util.Set<io.trino.spi.security.TrinoPrincipal>, boolean, java.util.Optional<io.trino.spi.security.TrinoPrincipal>])");
        delegate.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles,
                            Set<TrinoPrincipal> grantees, boolean adminOption,
                            Optional<TrinoPrincipal> grantor) {
        LOGGER.info("Calling revokeRoles([io.trino.spi.connector.ConnectorSession, java.util.Set<java.lang.String>, java.util.Set<io.trino.spi.security.TrinoPrincipal>, boolean, java.util.Optional<io.trino.spi.security.TrinoPrincipal>])");
        delegate.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session,
                                              TrinoPrincipal principal) {
        LOGGER.info("Calling listApplicableRoles([io.trino.spi.connector.ConnectorSession, io.trino.spi.security.TrinoPrincipal])");
        return delegate.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session) {
        LOGGER.info("Calling listEnabledRoles([io.trino.spi.connector.ConnectorSession])");
        return delegate.listEnabledRoles(session);
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName,
                                      Set<Privilege> privileges,
                                      TrinoPrincipal grantee, boolean grantOption) {
        LOGGER.info("Calling grantSchemaPrivileges([io.trino.spi.connector.ConnectorSession, java.lang.String, java.util.Set<io.trino.spi.security.Privilege>, io.trino.spi.security.TrinoPrincipal, boolean])");
        delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName,
                                       Set<Privilege> privileges,
                                       TrinoPrincipal grantee, boolean grantOption) {
        LOGGER.info("Calling revokeSchemaPrivileges([io.trino.spi.connector.ConnectorSession, java.lang.String, java.util.Set<io.trino.spi.security.Privilege>, io.trino.spi.security.TrinoPrincipal, boolean])");
        delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session,
                                     SchemaTableName tableName,
                                     Set<Privilege> privileges,
                                     TrinoPrincipal grantee, boolean grantOption) {
        LOGGER.info("Calling grantTablePrivileges([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, java.util.Set<io.trino.spi.security.Privilege>, io.trino.spi.security.TrinoPrincipal, boolean])");
        delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session,
                                      SchemaTableName tableName,
                                      Set<Privilege> privileges,
                                      TrinoPrincipal grantee, boolean grantOption) {
        LOGGER.info("Calling revokeTablePrivileges([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, java.util.Set<io.trino.spi.security.Privilege>, io.trino.spi.security.TrinoPrincipal, boolean])");
        delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session,
                                               SchemaTablePrefix prefix) {
        LOGGER.info("Calling listTablePrivileges([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTablePrefix])");
        return delegate.listTablePrivileges(session, prefix);
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        LOGGER.info("Calling usesLegacyTableLayouts([])");
        return delegate.usesLegacyTableLayouts();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session,
                                                       ConnectorTableHandle table) {
        LOGGER.info("Calling getTableProperties([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.getTableProperties(session, table);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
        ConnectorSession session, ConnectorTableHandle handle, long limit) {
        LOGGER.info("Calling applyLimit([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, long])");
        return delegate.applyLimit(session, handle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session, ConnectorTableHandle handle,
        Constraint constraint) {
        LOGGER.info("Calling applyFilter([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.Constraint])");
        return delegate.applyFilter(session, handle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
        ConnectorSession session, ConnectorTableHandle handle,
        List<ConnectorExpression> projections,
        Map<String, ColumnHandle> assignments) {
        LOGGER.info("Calling applyProjection([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.expression.ConnectorExpression>, java.util.Map<java.lang.String,io.trino.spi.connector.ColumnHandle>])");
        return delegate.applyProjection(session, handle, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(
        ConnectorSession session, ConnectorTableHandle handle,
        SampleType sampleType, double sampleRatio) {
        LOGGER.info("Calling applySample([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.SampleType, double])");
        return delegate.applySample(session, handle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
        ConnectorSession session, ConnectorTableHandle handle,
        List<AggregateFunction> aggregates,
        Map<String, ColumnHandle> assignments,
        List<List<ColumnHandle>> groupingSets) {
        LOGGER.info("Calling applyAggregation([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.AggregateFunction>, java.util.Map<java.lang.String,io.trino.spi.connector.ColumnHandle>, java.util.List<java.util.List<io.trino.spi.connector.ColumnHandle>>])");
        return delegate.applyAggregation(session, handle, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(
        ConnectorSession session, JoinType joinType,
        ConnectorTableHandle left, ConnectorTableHandle right,
        List<JoinCondition> joinConditions,
        Map<String, ColumnHandle> leftAssignments,
        Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics) {
        LOGGER.info("Calling applyJoin([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.JoinType, io.trino.spi.connector.ConnectorTableHandle, io.trino.spi.connector.ConnectorTableHandle, java.util.List<io.trino.spi.connector.JoinCondition>, java.util.Map<java.lang.String,io.trino.spi.connector.ColumnHandle>, java.util.Map<java.lang.String,io.trino.spi.connector.ColumnHandle>, io.trino.spi.connector.JoinStatistics])");
        return delegate.applyJoin(session, joinType, left, right, joinConditions, leftAssignments, rightAssignments, statistics);
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
        ConnectorSession session, ConnectorTableHandle handle, long topNCount,
        List<SortItem> sortItems,
        Map<String, ColumnHandle> assignments) {
        LOGGER.info("Calling applyTopN([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle, long, java.util.List<io.trino.spi.connector.SortItem>, java.util.Map<java.lang.String,io.trino.spi.connector.ColumnHandle>])");
        return delegate.applyTopN(session, handle, topNCount, sortItems, assignments);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle) {
        LOGGER.info("Calling validateScan([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        delegate.validateScan(session, handle);
    }

    @Override
    public void createMaterializedView(ConnectorSession session,
                                       SchemaTableName viewName,
                                       ConnectorMaterializedViewDefinition definition, boolean replace,
                                       boolean ignoreExisting) {
        LOGGER.info("Calling createMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.connector.ConnectorMaterializedViewDefinition, boolean, boolean])");
        delegate.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName) {
        LOGGER.info("Calling dropMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(
        ConnectorSession session, Optional<String> schemaName) {
        LOGGER.info("Calling listMaterializedViews([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.lang.String>])");
        return delegate.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(
        ConnectorSession session, Optional<String> schemaName) {
        LOGGER.info("Calling getMaterializedViews([io.trino.spi.connector.ConnectorSession, java.util.Optional<java.lang.String>])");
        return delegate.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(
        ConnectorSession session, SchemaTableName viewName) {
        LOGGER.info("Calling getMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(
        ConnectorSession session, SchemaTableName name) {
        LOGGER.info("Calling getMaterializedViewFreshness([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source,
                                       SchemaTableName target) {
        LOGGER.info("Calling renameMaterializedView([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName, io.trino.spi.connector.SchemaTableName])");
        delegate.renameMaterializedView(session, source, target);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(
        ConnectorSession session, ConnectorTableHandle tableHandle) {
        LOGGER.info("Calling applyTableScanRedirect([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.ConnectorTableHandle])");
        return delegate.applyTableScanRedirect(session, tableHandle);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(
        ConnectorSession session, SchemaTableName tableName) {
        LOGGER.info("Calling redirectTable([io.trino.spi.connector.ConnectorSession, io.trino.spi.connector.SchemaTableName])");
        return delegate.redirectTable(session, tableName);
    }
}
