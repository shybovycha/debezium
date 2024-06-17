package io.debezium.connector.custom.jdbc;

public class CustomJdbcObjectNameQuoter {
    /**
     * This function quotes a table or schema name in CustomJdbc if the name contains
     * at least one forbidden identifier character.
     *
     * @param name The name of the object.
     * @return The name of object between square brackets if not allowed as-is.
     */
    public static String quoteNameIfNecessary(String name) {
        if (name.contains("!") ||
                name.contains("%") ||
                name.contains("^") ||
                name.contains("&") ||
                name.contains("*") ||
                name.contains(".")) {
            return "[" + name + "]";
        }

        return name;
    }
}
