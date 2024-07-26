/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import io.debezium.schema.SchemaFactory;

public class CustomJdbcSchemaFactory extends SchemaFactory {

    public CustomJdbcSchemaFactory() {
        super();
    }

    private static final CustomJdbcSchemaFactory SYBASE_SCHEMA_FACTORY_OBJECT = new CustomJdbcSchemaFactory();

    public static CustomJdbcSchemaFactory get() {
        return SYBASE_SCHEMA_FACTORY_OBJECT;
    }
}
