/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.util.BoundedConcurrentHashMap;

/**
 * {@link JdbcConnection} extension to be used with CustomJdbc.
 *
 */
public class CustomJdbcConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomJdbcConnection.class);

    private final String GET_DATABASE_NAME;
    private final String GET_DATE;

    private final String URL_PATTERN;

    private final CustomJdbcConnectorConfig connectorConfig;

    /**
     * actual name of the database, which could differ in casing from the database name given in the connector config.
     */
    private final String realDatabaseName;

    private final BoundedConcurrentHashMap<Lsn, Instant> lsnToInstantCache;

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public CustomJdbcConnection(CustomJdbcConnectorConfig config) {
        super(
                config.getJdbcConfig(),
                JdbcConnection.patternBasedFactory(
                        config.getJdbcUriTemplate(),
                        config.getJdbcDriverClassName(),
                        CustomJdbcConnection.class.getClassLoader(),
                        JdbcConfiguration.PORT.withDefault(CustomJdbcConnectorConfig.PORT.defaultValueAsString())),
                config.getEscapeCharOpening(),
                config.getEscapeCharClosing());

        URL_PATTERN = config.getJdbcUriTemplate();

        GET_DATABASE_NAME = config.getQuery_getDatabaseName();
        GET_DATE = config.getQuery_getTimestamp();

        connectorConfig = config;

        lsnToInstantCache = new BoundedConcurrentHashMap<>(100);
        realDatabaseName = retrieveRealDatabaseName();
    }

    /**
     * @return the current largest log sequence number
     */
    public Lsn getMaxLsn() throws SQLException {
        return Lsn.NULL;
    }

    @Override
    public Optional<Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap(GET_DATE,
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    @Override
    protected boolean isTableUniqueIndexIncluded(String indexName, String columnName) {
        // ignore indices with no name;
        return indexName != null;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(
                    GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        StringBuilder quoted = new StringBuilder();
        if (tableId.schema() != null && !tableId.schema().isEmpty()) {
            quoted.append(CustomJdbcObjectNameQuoter.create(connectorConfig).quoteNameIfNecessary(tableId.schema())).append(".");
        }
        quoted.append(CustomJdbcObjectNameQuoter.create(connectorConfig).quoteNameIfNecessary(tableId.table()));
        return quoted.toString();
    }

    @Override
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer[] preparers, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final ResultSet[] resultSets = new ResultSet[multiQuery.length];
        final PreparedStatement[] preparedStatements = new PreparedStatement[multiQuery.length];

        try {
            for (int i = 0; i < multiQuery.length; i++) {
                final String query = multiQuery[i];
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("running '{}'", query);
                }
                // Purposely create the statement this way
                final PreparedStatement statement = createPreparedStatement(query);
                preparedStatements[i] = statement;
                preparers[i].accept(statement);
                resultSets[i] = statement.executeQuery();
            }
            if (resultConsumer != null) {
                resultConsumer.accept(resultSets);
            }
        }
        finally {
            for (ResultSet rs : resultSets) {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (Exception ei) {
                    }
                }
            }

            for (PreparedStatement ps : preparedStatements) {
                closePreparedStatement(ps);
            }
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQueryWithBlockingConsumer(String preparedQueryString, StatementPreparer preparer, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString) throws SQLException {
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            statement.executeQuery();
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString, StatementPreparer preparer, ResultSetConsumer resultConsumer)
            throws SQLException {
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    @Override
    public <T> T prepareQueryAndMap(String preparedQueryString, StatementPreparer preparer, ResultSetMapper<T> mapper)
            throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            preparer.accept(statement);
            try (ResultSet resultSet = statement.executeQuery();) {
                return mapper.apply(resultSet);
            }
        }
    }

    @Override
    public JdbcConnection prepareUpdate(String stmt, StatementPreparer preparer) throws SQLException {
        try (PreparedStatement statement = createPreparedStatement(stmt)) {
            if (preparer != null) {
                preparer.accept(statement);
            }
            LOGGER.trace("Executing statement '{}'", stmt);
            statement.execute();
        }
        return this;
    }

    @Override
    public JdbcConnection prepareQuery(String preparedQueryString, List<?> parameters,
                                       ParameterResultSetConsumer resultConsumer)
            throws SQLException {
        try (PreparedStatement statement = createPreparedStatement(preparedQueryString)) {
            int index = 1;
            for (final Object parameter : parameters) {
                statement.setObject(index++, parameter);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultConsumer != null) {
                    resultConsumer.accept(parameters, resultSet);
                }
            }
        }
        return this;
    }

    private PreparedStatement createPreparedStatement(String query) {
        try {
            LOGGER.trace("Creating prepared statement '{}'", query);
            return connection().prepareStatement(query);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private void closePreparedStatement(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            }
            catch (SQLException e) {
                // ignored
            }
        }
    }
}
