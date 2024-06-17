package io.debezium.connector.custom.jdbc;

import io.debezium.relational.TableIdPredicates;

public class CustomJdbcTableIdPredicate implements TableIdPredicates {

    @Override
    public boolean isStartDelimiter(char c) {
        return c == '[';
    }

    @Override
    public boolean isEndDelimiter(char c) {
        return c == ']';
    }
}
