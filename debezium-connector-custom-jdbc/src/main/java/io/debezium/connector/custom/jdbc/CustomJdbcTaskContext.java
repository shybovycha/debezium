package io.debezium.connector.custom.jdbc;

import io.debezium.connector.common.CdcSourceTaskContext;

public class CustomJdbcTaskContext extends CdcSourceTaskContext {

    public CustomJdbcTaskContext(CustomJdbcConnectorConfig config, CustomJdbcDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), config.getCustomMetricTags(), schema::tableIds);
    }
}
