/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.TableIdPredicates;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.spi.schema.DataCollectionId;

public class CustomJdbcConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    public static final int DEFAULT_QUERY_FETCH_SIZE = 10_000;

    protected static final int DEFAULT_PORT = 5000;

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true, true),

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector and stop after initial consistent snapshot.
         */
        INITIAL_ONLY("initial_only", true, false),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false, true);

        private final String value;
        private final boolean includeData;
        private final boolean shouldStream;

        SnapshotMode(String value, boolean includeData, boolean shouldStream) {
            this.value = value;
            this.includeData = includeData;
            this.shouldStream = shouldStream;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Whether the snapshot mode is followed by streaming.
         */
        public boolean shouldStream() {
            return shouldStream;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    /**
     * The set of predefined snapshot isolation mode options.
     *
     * https://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc32300.1600/doc/html/san1390612541323.html
     */
    public enum SnapshotIsolationMode implements EnumeratedValue {

        /**
         * This mode will block all reads and writes for the entire duration of the snapshot.
         */
        SERIALIZABLE("serializable"),

        /**
         * This mode uses REPEATABLE READ isolation level. This mode will avoid taking any table
         * locks during the snapshot process, except schema snapshot phase where serializable table
         * locks are acquired for a short period.  Since phantom reads can occur, it does not fully
         * guarantee consistency.
         */
        REPEATABLE_READ("repeatable_read"),

        /**
         * This mode uses READ COMMITTED isolation level. This mode does not take any table locks during
         * the snapshot process. In addition, it does not take any long-lasting row-level locks, like
         * in repeatable read isolation level. Snapshot consistency is not guaranteed.
         */
        READ_COMMITTED("read_committed"),

        /**
         * This mode uses READ UNCOMMITTED isolation level. This mode takes neither table locks nor row-level locks
         * during the snapshot process.  This way other transactions are not affected by initial snapshot process.
         * However, snapshot consistency is not guaranteed.
         */
        READ_UNCOMMITTED("read_uncommitted");

        private final String value;

        SnapshotIsolationMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotIsolationMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotIsolationMode option : SnapshotIsolationMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotIsolationMode parse(String value, String defaultValue) {
            SnapshotIsolationMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL_ONLY)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial_only' (the default) to specify the connector should run a snapshot only; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema only. ");

    public static final Field SNAPSHOT_ISOLATION_MODE = Field.create("snapshot.isolation.mode")
            .withDisplayName("Snapshot isolation mode")
            .withEnum(SnapshotIsolationMode.class, SnapshotIsolationMode.READ_COMMITTED)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Controls which transaction isolation level is used and how long the connector locks the monitored tables. "
                    + "Using a value of '" + SnapshotIsolationMode.SERIALIZABLE.getValue()
                    + "' ensures that the connector holds the exclusive lock (and thus prevents any reads and updates) for all monitored tables during the entire snapshot duration. "
                    + "In '" + SnapshotIsolationMode.READ_COMMITTED.getValue()
                    + "' mode no table locks or any *long-lasting* row-level locks are acquired, but connector does not guarantee snapshot consistency."
                    + "In '" + SnapshotIsolationMode.READ_UNCOMMITTED.getValue()
                    + "' mode neither table nor row-level locks are acquired, but connector does not guarantee snapshot consistency.");

    public static final Field QUERY_FETCH_SIZE = CommonConnectorConfig.QUERY_FETCH_SIZE
            .withDescription(
                    "The maximum number of records that should be loaded into memory while streaming. A value of '0' uses the default JDBC fetch size. The default value is '10000'.")
            .withDefault(DEFAULT_QUERY_FETCH_SIZE);

    public static final Field JDBC_DRIVER_CLASS = Field.create("jdbc.driver.class")
            .withDisplayName("JDBC driver class")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .required()
            // .withValidation(RelationalDatabaseConnectorConfig::validateHostname)
            .withDescription("JDBC driver class name. Class must be available on the classpath.");

    public static final Field JDBC_URI_TEMPLATE = Field.create("jdbc.uri.template")
            .withDisplayName("JDBC URI template")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .required()
            // .withValidation(RelationalDatabaseConnectorConfig::validateHostname)
            .withDescription("JDBC URI template. Driver-specific.");

    public static final Field QUERYING_ESCAPE_CHAR_OPENING = Field.create("querying.escape.char.opening")
            .withDisplayName("Opening escape character")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 2))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERYING_ESCAPE_CHAR_CLOSING = Field.create("querying.escape.char.closing")
            .withDisplayName("Closing escape character")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERYING_BOOLEAN_TRUE_VALUE = Field.create("querying.boolean.true.value")
            .withDisplayName("Value conforming to boolean True")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 4))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERYING_BOOLEAN_FALSE_VALUE = Field.create("querying.boolean.false.value")
            .withDisplayName("Value conforming to boolean False")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 5))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERYING_SPECIAL_CHARACTERS = Field.create("querying.special.characters")
            .withDisplayName("Special characters which need escaping")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 6))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERIES_GET_DATABASE_NAME = Field.create("queries.get.database.name")
            .withDisplayName("Query to retrieve DB name")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 7))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERIES_GET_TIMESTAMP = Field.create("queries.get.date")
            .withDisplayName("Query to get current timestamp")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 8))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERIES_PING = Field.create("queries.ping")
            .withDisplayName("Query to check if a connection succeeded")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 9))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field QUERIES_SELECT_FIELDS_FROM_COLLECTION = Field.create("queries.select.fields.from.collection")
            .withDisplayName("Query to select specified fields from collection")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 10))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Query template to select specified fields from collection. Use ${fields}, ${collection}, ${schema} to interpolate corresponding values.")
            .required();

    public static final Field USES_DATABASE_CATALOG = Field.create("tableid.use.catalog")
            .withDisplayName("Use catalog (DB name) when referring to tables")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 11))
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field USES_DATABASE_SCHEMA = Field.create("tableid.use.schema")
            .withDisplayName("Use schema when referring to tables")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 12))
            .withWidth(Width.SHORT)
            .withDefault(true)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(CustomJdbcSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("CustomJdbc")
            .excluding(CommonConnectorConfig.QUERY_FETCH_SIZE,
                    CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER)
            .type(
                    JDBC_DRIVER_CLASS,
                    JDBC_URI_TEMPLATE,
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME)
            .connector(
                    SNAPSHOT_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    QUERY_FETCH_SIZE,
                    QUERYING_ESCAPE_CHAR_OPENING,
                    QUERYING_ESCAPE_CHAR_CLOSING,
                    QUERYING_BOOLEAN_TRUE_VALUE,
                    QUERYING_BOOLEAN_FALSE_VALUE,
                    QUERYING_SPECIAL_CHARACTERS,
                    QUERIES_GET_DATABASE_NAME,
                    QUERIES_GET_TIMESTAMP,
                    QUERIES_PING,
                    QUERIES_SELECT_FIELDS_FROM_COLLECTION,
                    USES_DATABASE_CATALOG,
                    USES_DATABASE_SCHEMA)
            .events(SOURCE_INFO_STRUCT_MAKER)
            .excluding(
                    SCHEMA_INCLUDE_LIST,
                    SCHEMA_EXCLUDE_LIST,
                    // additional fields
                    BINARY_HANDLING_MODE,
                    INCLUDE_SCHEMA_COMMENTS,
                    INCREMENTAL_SNAPSHOT_ALLOW_SCHEMA_CHANGES,
                    SNAPSHOT_MAX_THREADS,
                    DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY)
            .create();

    protected static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    private final String databaseName;
    private final SnapshotMode snapshotMode;
    private final SnapshotIsolationMode snapshotIsolationMode;
    private final boolean usesCatalog;
    private final boolean usesSchema;

    public CustomJdbcConnectorConfig(Configuration config) {
        super(
                CustomJdbcConnector.class,
                config,
                new IncludeAllTablesPredicate(),
                x -> {
                    if (x.schema() == null) {
                        if (x.catalog() != null) {
                            return x.catalog() + "." + x.table();
                        }

                        return x.schema() + "." + x.table();
                    }

                    return x.catalog() + "." + x.schema() + "." + x.table();
                },
                config.getBoolean(USES_DATABASE_CATALOG),
                ColumnFilterMode.SCHEMA,
                false);

        this.databaseName = config.getString(DATABASE_NAME);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.snapshotIsolationMode = SnapshotIsolationMode.parse(config.getString(SNAPSHOT_ISOLATION_MODE), SNAPSHOT_ISOLATION_MODE.defaultValueAsString());
        this.usesSchema = config.getBoolean(USES_DATABASE_SCHEMA);
        this.usesCatalog = config.getBoolean(USES_DATABASE_CATALOG);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public boolean usesCatalog() {
        return usesCatalog;
    }

    public boolean usesSchema() {
        return usesSchema;
    }

    @Override
    public Optional<SnapshotIsolationMode> getSnapshotLockingMode() {
        return Optional.of(this.snapshotIsolationMode);
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    private static class IncludeAllTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return true;
        }
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Lsn.valueOf(recorded.getString(SourceInfo.CHANGE_LSN_KEY))
                        .compareTo(Lsn.valueOf(desired.getString(SourceInfo.CHANGE_LSN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    @Override
    public Map<DataCollectionId, String> getSnapshotSelectOverridesByTable() {
        String tableList = getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);

        if (tableList == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        TableIdPredicates tableSeparatorPredicate = new CustomJdbcTableIdPredicate(
                getEscapeCharOpening().charAt(0),
                getEscapeCharClosing().charAt(0));

        for (String table : tableList.split(",")) {
            snapshotSelectOverridesByTable.put(
                    TableId.parse(table, tableSeparatorPredicate),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    public String getJdbcDriverClassName() {
        return getConfig().getString(JDBC_DRIVER_CLASS);
    }

    public String getJdbcUriTemplate() {
        return getConfig().getString(JDBC_URI_TEMPLATE);
    }

    public String getEscapeCharOpening() {
        return getConfig().getString(QUERYING_ESCAPE_CHAR_OPENING);
    }

    public String getEscapeCharClosing() {
        return getConfig().getString(QUERYING_ESCAPE_CHAR_CLOSING);
    }

    public String getQuery_getDatabaseName() {
        return getConfig().getString(QUERIES_GET_DATABASE_NAME);
    }

    public String getQuery_getTimestamp() {
        return getConfig().getString(QUERIES_GET_TIMESTAMP);
    }

    public String getBooleanTrueValue() {
        return getConfig().getString(QUERYING_BOOLEAN_TRUE_VALUE);
    }

    public String getBooleanFalseValue() {
        return getConfig().getString(QUERYING_BOOLEAN_FALSE_VALUE);
    }

    public Set<String> getSpecialCharacters() {
        return getConfig().getString(QUERYING_SPECIAL_CHARACTERS)
                .chars()
                .mapToObj(code -> String.valueOf((char) code))
                .collect(Collectors.toSet());
    }

    public String getQuery_selectFieldsFromCollection() {
        return getConfig().getString(QUERIES_SELECT_FIELDS_FROM_COLLECTION);
    }
}
