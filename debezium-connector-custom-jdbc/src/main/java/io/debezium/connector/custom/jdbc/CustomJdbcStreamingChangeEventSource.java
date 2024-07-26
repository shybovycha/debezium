/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

// TODO: This needs to be implemented
public class CustomJdbcStreamingChangeEventSource implements StreamingChangeEventSource<CustomJdbcPartition, CustomJdbcOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcStreamingChangeEventSource.class);

    /**
     * Connection used for reading CDC tables.
     */
    private final CustomJdbcConnection dataConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive
     * buffering will not work.
     */
    private final CustomJdbcConnection metadataConnection;

    private final EventDispatcher<CustomJdbcPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final CustomJdbcDatabaseSchema schema;
    private final Duration pollInterval;
    private final CustomJdbcConnectorConfig connectorConfig;
    private CustomJdbcOffsetContext effectiveOffsetContext;

    public CustomJdbcStreamingChangeEventSource(CustomJdbcConnectorConfig connectorConfig, CustomJdbcConnection dataConnection,
                                                CustomJdbcConnection metadataConnection,
                                                EventDispatcher<CustomJdbcPartition, TableId> dispatcher, ErrorHandler errorHandler,
                                                Clock clock, CustomJdbcDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.metadataConnection = metadataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
    }

    public void init(CustomJdbcOffsetContext offsetContext) {

        this.effectiveOffsetContext = offsetContext != null
                ? offsetContext
                : new CustomJdbcOffsetContext(connectorConfig, TxLogPosition.NULL, false, false);
    }

    @Override
    public void execute(ChangeEventSourceContext context, CustomJdbcPartition partition, CustomJdbcOffsetContext offsetContext)
            throws InterruptedException {
        if (!connectorConfig.getSnapshotMode().shouldStream()) {
            LOGGER.info("Streaming is not enabled in current configuration");
        }
        else {
            LOGGER.error("Streaming changes requested but CustomJdbc connector doesn't support those yet");
        }
    }

    @Override
    public CustomJdbcOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }
}
