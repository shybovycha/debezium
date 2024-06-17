package io.debezium.connector.custom.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

// TODO: This needs a second look
public class CustomJdbcDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcDatabaseSchema.class);

    public CustomJdbcDatabaseSchema(CustomJdbcConnectorConfig connectorConfig, CustomJdbcValueConverters valueConverters, SchemaNameAdjuster schemaNameAdjuster,
                                    TopicNamingStrategy<TableId> topicNamingStrategy,
                                    CustomJdbcConnection connection) {
        super(connectorConfig, topicNamingStrategy, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        new CustomJdbcDefaultValueConverter(valueConverters, connection),
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false),
                false, connectorConfig.getKeyMapper());
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        }
        else if (schemaChange.getType() == SchemaChangeEventType.ALTER) {
            tableChanges = new TableChanges();
            tableChanges.alter(table);
        }

        record(schemaChange, tableChanges);
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }
}
