package io.debezium.connector.custom.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class CustomJdbcSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.sybase.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.CHANGE_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);

        if (sourceInfo.getTableId() != null) {
            ret
                    .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
                    .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());
        }
        if (sourceInfo.getChangeLsn() != null && sourceInfo.getChangeLsn().isAvailable()) {
            ret.put(SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn().toString());
        }
        if (sourceInfo.getCommitLsn() != null && sourceInfo.getCommitLsn().isAvailable()) {
            ret.put(SourceInfo.COMMIT_LSN_KEY, sourceInfo.getCommitLsn().toString());
        }
        return ret;
    }
}
