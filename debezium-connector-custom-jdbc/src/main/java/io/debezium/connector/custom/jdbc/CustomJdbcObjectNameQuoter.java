package io.debezium.connector.custom.jdbc;

public class CustomJdbcObjectNameQuoter {
    private final CustomJdbcConnectorConfig config;

    public CustomJdbcObjectNameQuoter(CustomJdbcConnectorConfig config) {
        this.config = config;
    }

    public static CustomJdbcObjectNameQuoter create(CustomJdbcConnectorConfig config) {
        return new CustomJdbcObjectNameQuoter(config);
    }

    /**
     * This function quotes a table or schema name in CustomJdbc if the name contains
     * at least one forbidden identifier character.
     *
     * @param name The name of the object.
     * @return The name of object between square brackets if not allowed as-is.
     */
    public String quoteNameIfNecessary(String name) {
        if (config.getSpecialCharacters().stream().anyMatch(name::contains)) {
            return "[" + name + "]";
        }

        return name;
    }
}
