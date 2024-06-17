package io.debezium.connector.custom.jdbc;

import java.util.Collections;
import java.util.List;

import io.debezium.annotation.Immutable;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

@Immutable
public final class CustomJdbcDataCollectionId implements DataCollectionId {

    private final String dbName;
    private final String name;

    /**
     * Parse the supplied {@code <database_name>.<collection_name>} string.
     * The {@code collection_name} can also contain dots in its value.
     *
     * @param str the string representation of the collection identifier; may not be null
     * @return the collection ID, or null if it could not be parsed
     */
    public static CustomJdbcDataCollectionId parse(String str) {
        final int dotPosition = str.indexOf('.');
        if (dotPosition == -1 || (dotPosition + 1) == str.length() || dotPosition == 0) {
            return null;
        }

        return new CustomJdbcDataCollectionId(str.substring(0, dotPosition), str.substring(dotPosition + 1));
    }

    public CustomJdbcDataCollectionId(String dbName, String collectionName) {
        this.dbName = dbName;
        this.name = collectionName;
        assert this.dbName != null;
        assert this.name != null;
    }

    public String dbName() {
        return dbName;
    }

    public String name() {
        return name;
    }

    @Override
    public String identifier() {
        return dbName + "." + name;
    }

    @Override
    public List<String> parts() {
        return Collect.arrayListOf(dbName, name);
    }

    @Override
    public List<String> databaseParts() {
        return Collect.arrayListOf(dbName, name);
    }

    @Override
    public List<String> schemaParts() {
        return Collections.emptyList();
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof CustomJdbcDataCollectionId) {
            CustomJdbcDataCollectionId that = (CustomJdbcDataCollectionId) obj;
            return this.dbName.equals(that.dbName) && this.name.equals(that.name);
        }
        return false;
    }

    /**
     * Get the namespace of this collection, which is composed of the {@link #dbName database name} and {@link #name collection
     * name}.
     *
     * @return the namespace for this collection; never null
     */
    public String namespace() {
        return identifier();
    }

    @Override
    public String toString() {
        return identifier();
    }
}
