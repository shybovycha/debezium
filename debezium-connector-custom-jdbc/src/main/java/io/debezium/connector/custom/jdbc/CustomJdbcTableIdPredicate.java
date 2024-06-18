/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.custom.jdbc;

import io.debezium.relational.TableIdPredicates;

public class CustomJdbcTableIdPredicate implements TableIdPredicates {
    private final char openingEscapeChar;
    private final char closingEscapeChar;

    public CustomJdbcTableIdPredicate(char openingEscapeChar, char closingEscapeChar) {
        this.openingEscapeChar = openingEscapeChar;
        this.closingEscapeChar = closingEscapeChar;
    }

    @Override
    public boolean isStartDelimiter(char c) {
        return c == openingEscapeChar;
    }

    @Override
    public boolean isEndDelimiter(char c) {
        return c == closingEscapeChar;
    }
}
