/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;

/**
 * CloudEvents maker for records produced by the CustomJdbc connector.
 */
public class CustomJdbcCloudEventsMaker extends CloudEventsMaker {

    static final String CHANGE_LSN_KEY = "change_lsn";
    static final String COMMIT_LSN_KEY = "commit_lsn";
    static final String EVENT_SERIAL_NO_KEY = "event_serial_no";

    public CustomJdbcCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        super(recordAndMetadata, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";change_lsn:" + sourceField(CHANGE_LSN_KEY)
                + ";commit_lsn:" + sourceField(COMMIT_LSN_KEY)
                + ";event_serial_no:" + sourceField(EVENT_SERIAL_NO_KEY);
    }

    @Override
    protected Set<String> connectorSpecificSourceFields() {
        return Set.of();
    }
}
