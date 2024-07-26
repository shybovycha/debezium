/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class CustomJdbcPartition implements Partition {
    private static final String DATABASE_PARTITION_KEY = "database";

    private final String databaseName;

    public CustomJdbcPartition(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(DATABASE_PARTITION_KEY, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final CustomJdbcPartition other = (CustomJdbcPartition) obj;
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return databaseName.hashCode();
    }

    @Override
    public String toString() {
        return "CustomJdbcPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<CustomJdbcPartition> {
        private final CustomJdbcConnectorConfig connectorConfig;

        Provider(CustomJdbcConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<CustomJdbcPartition> getPartitions() {
            return Collections.singleton(new CustomJdbcPartition(connectorConfig.getLogicalName()));
        }
    }
}
