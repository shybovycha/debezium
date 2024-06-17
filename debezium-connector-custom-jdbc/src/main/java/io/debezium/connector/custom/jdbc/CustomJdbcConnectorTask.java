package io.debezium.connector.custom.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

public class CustomJdbcConnectorTask extends BaseSourceTask<CustomJdbcPartition, CustomJdbcOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcConnectorTask.class);
    private static final String CONTEXT_NAME = "sybase-server-connector-task";

    private volatile CustomJdbcTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile CustomJdbcConnection dataConnection;
    private volatile CustomJdbcConnection metadataConnection;
    private volatile ErrorHandler errorHandler;
    private volatile CustomJdbcDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<CustomJdbcPartition, CustomJdbcOffsetContext> start(Configuration config) {
        final CustomJdbcConnectorConfig connectorConfig = new CustomJdbcConnectorConfig(applyFetchSizeToJdbcConfig(config));
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        MainConnectionProvidingConnectionFactory<CustomJdbcConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new CustomJdbcConnection(connectorConfig.getJdbcConfig()));
        dataConnection = connectionFactory.mainConnection();
        metadataConnection = connectionFactory.newConnection();

        final CustomJdbcValueConverters valueConverters = new CustomJdbcValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode());
        this.schema = new CustomJdbcDatabaseSchema(connectorConfig, valueConverters, schemaNameAdjuster, topicNamingStrategy, dataConnection);
        this.schema.initializeStorage();

        Offsets<CustomJdbcPartition, CustomJdbcOffsetContext> previousOffsets = getPreviousOffsets(new CustomJdbcPartition.Provider(connectorConfig),
                new CustomJdbcOffsetContext.Loader(connectorConfig));
        final CustomJdbcPartition partition = previousOffsets.getTheOnlyPartition();
        final CustomJdbcOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        if (previousOffset != null) {
            schema.recover(partition, previousOffset);
        }

        taskContext = new CustomJdbcTaskContext(connectorConfig, schema);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(CustomJdbcConnector.class, connectorConfig, queue, errorHandler);

        final CustomJdbcEventMetadataProvider metadataProvider = new CustomJdbcEventMetadataProvider();

        SignalProcessor<CustomJdbcPartition, CustomJdbcOffsetContext> signalProcessor = new SignalProcessor<>(
                CustomJdbcConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<CustomJdbcPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor);

        NotificationService<CustomJdbcPartition, CustomJdbcOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        ChangeEventSourceCoordinator<CustomJdbcPartition, CustomJdbcOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                CustomJdbcConnector.class,
                connectorConfig,
                new CustomJdbcChangeEventSourceFactory(connectorConfig, metadataConnection, connectionFactory, errorHandler, dispatcher, clock, schema,
                        snapshotterService),
                new DefaultChangeEventSourceMetricsFactory<>(),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (dataConnection != null) {
                // May have an active in-progress transaction associated with the connection and if so,
                // it will throw an exception during shutdown because the active transaction exists. This
                // is meant to help avoid this by rolling back the current active transaction, if exists.
                if (dataConnection.isConnected()) {
                    try {
                        dataConnection.rollback();
                    }
                    catch (SQLException e) {
                        // ignore
                    }
                }
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (metadataConnection != null) {
                metadataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC metadata connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return CustomJdbcConnectorConfig.ALL_FIELDS;
    }

    /**
     * Applies the fetch size to the driver/jdbc configuration from the connector configuration.
     *
     * @param config the connector configuration
     * @return the potentially modified configuration, never null
     */
    private static Configuration applyFetchSizeToJdbcConfig(Configuration config) {
        // By default, do not load whole result sets into memory
        if (config.getInteger(CustomJdbcConnectorConfig.QUERY_FETCH_SIZE) > 0) {
            final String driverPrefix = CommonConnectorConfig.DRIVER_CONFIG_PREFIX;
            return config.edit()
                    .withDefault(driverPrefix + "responseBuffering", "adaptive")
                    .withDefault(driverPrefix + "fetchSize", config.getInteger(CustomJdbcConnectorConfig.QUERY_FETCH_SIZE))
                    .build();
        }
        return config;
    }
}
