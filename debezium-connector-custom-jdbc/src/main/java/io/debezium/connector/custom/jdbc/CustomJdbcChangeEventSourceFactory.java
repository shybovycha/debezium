package io.debezium.connector.custom.jdbc;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class CustomJdbcChangeEventSourceFactory implements ChangeEventSourceFactory<CustomJdbcPartition, CustomJdbcOffsetContext> {

    private final CustomJdbcConnectorConfig configuration;
    private final CustomJdbcConnection metadataConnection;
    private final MainConnectionProvidingConnectionFactory<CustomJdbcConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<CustomJdbcPartition, TableId> dispatcher;
    private final Clock clock;
    private final CustomJdbcDatabaseSchema schema;
    private final SnapshotterService snapshotterService;

    public CustomJdbcChangeEventSourceFactory(CustomJdbcConnectorConfig configuration, CustomJdbcConnection metadataConnection,
                                              MainConnectionProvidingConnectionFactory<CustomJdbcConnection> connectionFactory, ErrorHandler errorHandler,
                                              EventDispatcher<CustomJdbcPartition, TableId> dispatcher, Clock clock, CustomJdbcDatabaseSchema schema,
                                              SnapshotterService snapshotterService) {
        this.configuration = configuration;
        this.metadataConnection = metadataConnection;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<CustomJdbcPartition, CustomJdbcOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<CustomJdbcPartition> snapshotProgressListener,
                                                                                                                NotificationService<CustomJdbcPartition, CustomJdbcOffsetContext> notificationService) {
        return new CustomJdbcSnapshotChangeEventSource(configuration, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<CustomJdbcPartition, CustomJdbcOffsetContext> getStreamingChangeEventSource() {
        return new CustomJdbcStreamingChangeEventSource(
                configuration,
                connectionFactory.mainConnection(),
                metadataConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<CustomJdbcPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                   CustomJdbcOffsetContext offsetContext,
                                                                                                                                                   SnapshotProgressListener<CustomJdbcPartition> snapshotProgressListener,
                                                                                                                                                   DataChangeEventListener<CustomJdbcPartition> dataChangeEventListener,
                                                                                                                                                   NotificationService<CustomJdbcPartition, CustomJdbcOffsetContext> notificationService) {
        // If no data collection id is provided, don't return an instance as the implementation requires
        // that a signal data collection id be provided to work.
        if (Strings.isNullOrEmpty(configuration.getSignalingDataCollectionId())) {
            return Optional.empty();
        }

        final SignalBasedIncrementalSnapshotChangeEventSource<CustomJdbcPartition, TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<>(
                configuration,
                connectionFactory.mainConnection(),
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
