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
