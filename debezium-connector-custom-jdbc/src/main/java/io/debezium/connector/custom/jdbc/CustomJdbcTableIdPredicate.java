package io.debezium.connector.custom.jdbc;

import io.debezium.relational.TableIdPredicates;

public class CustomJdbcTableIdPredicate implements TableIdPredicates {

    // TODO: configure these symbols
    @Override
    public boolean isStartDelimiter(char c) {
        return c == '[';
    }

    // TODO: configure these symbols
    @Override
    public boolean isEndDelimiter(char c) {
        return c == ']';
    }
}
