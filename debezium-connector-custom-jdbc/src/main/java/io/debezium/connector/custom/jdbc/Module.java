/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.util.Properties;

import io.debezium.util.IoUtil;

public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/jdbc/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "jdbc";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "CustomJdbc_Server";
    }
}
