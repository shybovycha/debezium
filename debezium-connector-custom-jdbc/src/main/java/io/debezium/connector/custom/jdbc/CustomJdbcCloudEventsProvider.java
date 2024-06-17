package io.debezium.connector.custom.jdbc;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for CustomJdbc.
 */
public class CustomJdbcCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                        String cloudEventsSchemaName) {
        return new CustomJdbcCloudEventsMaker(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
