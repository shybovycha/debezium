/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.custom.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

public class CustomJdbcSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<CustomJdbcPartition, CustomJdbcOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcSnapshotChangeEventSource.class);

    private final CustomJdbcConnectorConfig connectorConfig;
    private final CustomJdbcConnection jdbcConnection;

    protected SnapshotProgressListener<CustomJdbcPartition> snapshotProgressListener;

    public CustomJdbcSnapshotChangeEventSource(CustomJdbcConnectorConfig connectorConfig,
                                               MainConnectionProvidingConnectionFactory<CustomJdbcConnection> connectionFactory,
                                               CustomJdbcDatabaseSchema schema, EventDispatcher<CustomJdbcPartition, TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener<CustomJdbcPartition> snapshotProgressListener,
                                               NotificationService<CustomJdbcPartition, CustomJdbcOffsetContext> notificationService,
                                               SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(CustomJdbcPartition partition, CustomJdbcOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> CustomJdbcDataCollectionId.parse(e.getKey().identifier()), Map.Entry::getValue));

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            if (connectorConfig.getSnapshotMode().includeData()) {
                LOGGER.info("According to the connector configuration both schema and data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration only schema will be snapshotted");
            }
            snapshotData = connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> prepare(CustomJdbcPartition partition, boolean onDemand) throws Exception {
        return new CustomJdbcSnapshotContext(partition, jdbcConnection.getRealDatabaseName(), onDemand);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) throws Exception {
        ((CustomJdbcSnapshotContext) snapshotContext).isolationLevelBeforeStart = jdbcConnection.connection().getTransactionIsolation();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> ctx) throws Exception {
        return jdbcConnection.readTableNames(null, null, null, new String[]{ "TABLE" });
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext)
            throws SQLException {

        // jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        // LOGGER.info("Schema locking was disabled in connector configuration");

        // TODO?
        // SnapshotIsolationMode isolationMode = connectorConfig.getSnapshotLockingMode()
        // .orElseThrow(() -> new IllegalStateException("No locking mode specified."));
        //
        // if (isolationMode == SnapshotIsolationMode.READ_UNCOMMITTED) {
        // jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        // LOGGER.info("Schema locking was disabled in connector configuration");
        // }
        // else if (isolationMode == SnapshotIsolationMode.READ_COMMITTED) {
        // jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        // LOGGER.info("Schema locking was disabled in connector configuration");
        // }
        // else if (isolationMode == SnapshotIsolationMode.SERIALIZABLE
        // || isolationMode == SnapshotIsolationMode.REPEATABLE_READ) {
        // // TODO: Not supported at the moment as there's a bug with the JTDS 1.3.1 driver
        // // https://github.com/milesibastos/jTDS/commit/6d642c467b3339c53c3c746d14a1641565ed7d2d
        // LOGGER.error("Serializable or repeatable read snapshot transaction isolation not supported.");
        // jdbcConnection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // ((CustomJdbcSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("sybase_schema_snapshot");
        // }
        // else {
        // throw new IllegalStateException("Unknown locking mode specified.");
        // }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) throws SQLException {
        // Exclusive mode: locks should be kept until the end of transaction.
        // read_uncommitted mode; read_committed mode: no locks have been acquired.
        // boolean useSnapshotLocking = connectorConfig.getSnapshotLockingMode()
        // .filter(mode -> mode == CustomJdbcConnectorConfig.SnapshotIsolationMode.REPEATABLE_READ)
        // .isPresent();
        //
        // if (useSnapshotLocking) {
        // jdbcConnection.connection()
        // .rollback(((CustomJdbcSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        // LOGGER.info("Schema locks released.");
        // }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> ctx, CustomJdbcOffsetContext previousOffset)
            throws Exception {
        ctx.offset = new CustomJdbcOffsetContext(
                connectorConfig,
                TxLogPosition.valueOf(jdbcConnection.getMaxLsn()),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext,
                                      CustomJdbcOffsetContext previousOffset, SnapshottingTask snapshottingTask)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", schema);

            Tables.TableFilter tableFilter = snapshottingTask.isOnDemand() ? Tables.TableFilter.fromPredicate(snapshotContext.capturedTables::contains)
                    : connectorConfig.getTableFilters().dataCollectionFilter();

            // CustomJdbc cannot run schema discovery SPs within a transaction
            // TODO: Toggling autocommit will commit any pending transactions, investigate if OK to do this
            final boolean oldAutoCommit = jdbcConnection.connection().getAutoCommit();
            jdbcConnection.connection().setAutoCommit(true);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
            jdbcConnection.connection().setAutoCommit(oldAutoCommit);
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected void completed(SnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    @Override
    protected void aborted(SnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) {
        close(snapshotContext);
    }

    private void close(SnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) {
        try {
            jdbcConnection.connection().setTransactionIsolation(((CustomJdbcSnapshotContext) snapshotContext).isolationLevelBeforeStart);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to set transaction isolation level.", e);
        }
    }

    /**
     * Generate a valid CustomJdbc query string for the specified table
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext, TableId tableId,
                                                 List<String> columns) {
        String snapshotSelectColumns = String.join(", ", columns);
        String queryTemplate = connectorConfig.getQuery_selectFieldsFromCollection();
        return Optional.of(
                queryTemplate
                        .replace("${fields}", snapshotSelectColumns)
                        .replace("${schema}", CustomJdbcObjectNameQuoter.create(connectorConfig).quoteNameIfNecessary(tableId.schema()))
                        .replace("${collection}", CustomJdbcObjectNameQuoter.create(connectorConfig).quoteNameIfNecessary(tableId.table())));
    }

    @Override
    protected void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext,
                                              RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext,
                                              CustomJdbcOffsetContext offset,
                                              EventDispatcher.SnapshotReceiver<CustomJdbcPartition> snapshotReceiver, Table table,
                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount, String selectStatement, OptionalLong rowCount,
                                              JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {

        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());

        try (Statement statement = readTableStatement(jdbcConnection, rowCount);
                ResultSet rs = resultSetForDataEvents(selectStatement, statement, table)) {

            ColumnUtils.ColumnArray columnArray = columnsToArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();
            boolean hasNext = rs.next();

            if (hasNext) {
                while (hasNext) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                    }

                    rows++;
                    final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);

                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        if (rowCount.isPresent()) {
                            LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                    table.id(), Strings.duration(stop - exportStart));
                        }
                        else {
                            LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                    Strings.duration(stop - exportStart));
                        }
                        snapshotProgressListener.rowsScanned(snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }

                    hasNext = rs.next();
                    setSnapshotMarker(offset, firstTable, lastTable, rows == 1, !hasNext);

                    dispatcher.dispatchSnapshotEvent(snapshotContext.partition, table.id(),
                            getChangeRecordEmitter(snapshotContext.partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                }
            }
            else {
                setSnapshotMarker(offset, firstTable, lastTable, false, true);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                    rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, table.id(), rows);
            notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(snapshotContext.partition,
                    snapshotContext.offset, table.id().identifier(), rows, snapshotContext.capturedTables);
        }
    }

    private ColumnUtils.ColumnArray columnsToArray(ResultSet resultSet, Table table) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        Column[] columns = new Column[metaData.getColumnCount()];

        int greatestColumnPosition = 0;

        for (int i = 0; i < columns.length; i++) {
            final String columnName = metaData.getColumnName(i + 1);

            columns[i] = table.columns().get(i);

            if (columns[i] == null) {
                // This situation can happen when SQL Server and Db2 schema is changed before
                // an incremental snapshot is started and no event with the new schema has been
                // streamed yet.
                // This warning will help to identify the issue in case of a support request.

                final String[] resultSetColumns = new String[metaData.getColumnCount()];

                for (int j = 0; j < metaData.getColumnCount(); j++) {
                    resultSetColumns[j] = metaData.getColumnName(j + 1);
                }

                throw new IllegalArgumentException("Column '"
                        + columnName
                        + "' not found in result set '"
                        + String.join(", ", resultSetColumns)
                        + "' for table '"
                        + table.id()
                        + "', "
                        + table
                        + ". This might be caused by DBZ-4350");
            }

            greatestColumnPosition = Math.max(greatestColumnPosition, columns[i].position());
        }

        return new ColumnUtils.ColumnArray(columns, greatestColumnPosition);
    }

    protected Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    protected void setSnapshotMarker(OffsetContext offset, boolean firstTable, boolean lastTable, boolean firstRecordInTable,
                                     boolean lastRecordInTable) {
        if (lastRecordInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        }
        else if (firstRecordInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        }
        else if (lastRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        else if (firstRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        }
        else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
    }

    protected ResultSet resultSetForDataEvents(String selectStatement, Statement statement, Table table)
            throws SQLException {
        return CustomJdbcResultSet.from(statement.executeQuery(selectStatement), table);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class CustomJdbcSnapshotContext extends RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> {

        private int isolationLevelBeforeStart;
        private Savepoint preSchemaSnapshotSavepoint;

        CustomJdbcSnapshotContext(CustomJdbcPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }

    @Override
    protected CustomJdbcOffsetContext copyOffset(RelationalSnapshotContext<CustomJdbcPartition, CustomJdbcOffsetContext> snapshotContext) {
        return new CustomJdbcOffsetContext.Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

}
