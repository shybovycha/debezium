/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

// TODO: Important - need to reimplement this to suit CustomJdbc
public class CustomJdbcValueConverters extends JdbcValueConverters {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcValueConverters.class);

    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * Create a new instance that always uses UTC for the default time zone when
     * converting values without timezone information to values that require
     * timezones.
     * <p>
     *
     * @param decimalMode
     *            how {@code DECIMAL} and {@code NUMERIC} values should be
     *            treated; may be null if
     *            {@link DecimalMode#PRECISE}
     *            is to be used
     * @param temporalPrecisionMode
     *            date/time value will be represented either as Connect datatypes or Debezium specific datatypes
     */
    public CustomJdbcValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        LOGGER.info("Converting column {} ({}) into schema", column.name(), column.jdbcType());

        // TODO: configure these type conversions
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.NUMERIC:
                return numericSchema(column);
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return SchemaBuilder.int16();
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        LOGGER.info("Converting column {} ({}) value", column.name(), column.jdbcType());

        // TODO: configure these type conversions
        switch (column.jdbcType()) {
            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, we thus need to store it in short int
                return (data) -> convertSmallInt(column, fieldDefn, data);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            try {
                return super.convertString(column, fieldDefn, ((Clob) data).getSubString(1L, (int) ((Clob) data).length()));
            }
            catch (SQLException e) {
                throw new DataException("Cannot convert clob column due to " + e.getMessage());
            }
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(Column column, Field fieldDefn, Object data, CommonConnectorConfig.BinaryHandlingMode mode) {
        if (data instanceof Blob) {
            try {
                return super.convertBinary(column, fieldDefn, ((Blob) data).getBytes(1L, (int) ((Blob) data).length()), mode);
            }
            catch (SQLException e) {
                throw new DataException("Cannot convert blob column due to " + e.getMessage());
            }
        }

        return super.convertBinary(column, fieldDefn, data, mode);
    }

    // TODO: configure the timestamp format?
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        // dummy return
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

    private SchemaBuilder numericSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE && isVariableScaleDecimal(column)) {
            return VariableScaleDecimal.builder();
        }

        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(0));
    }

    private boolean isVariableScaleDecimal(Column column) {
        return (column.length() == 0 || column.length() == VARIABLE_SCALE_DECIMAL_LENGTH) &&
                column.scale().orElseGet(() -> 0) == 0;
    }
}
