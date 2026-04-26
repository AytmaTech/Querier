package com.aytmatech.querier;

/**
 * Defines quoting strategies for database identifiers (table names, column names, etc.). Different
 * databases use different quoting mechanisms for identifiers to handle reserved words and special
 * characters.
 */
public enum QuoteStrategy {
  /**
   * No quoting - bare identifiers. This is the default for backward compatibility. Example:
   * orders.id
   */
  NONE {
    @Override
    public String quote(String identifier) {
      return identifier;
    }
  },

  /**
   * ANSI SQL standard double quotes. Used by PostgreSQL, Oracle, SQLite, and most ANSI-compliant
   * databases. Example: "orders"."id"
   */
  ANSI {
    @Override
    public String quote(String identifier) {
      String escaped = identifier.replace("\"", "\"\"");
      return "\"" + escaped + "\"";
    }
  },

  /** MySQL-style backticks. Used by MySQL and MariaDB. Example: `orders`.`id` */
  MYSQL {
    @Override
    public String quote(String identifier) {
      String escaped = identifier.replace("`", "``");
      return "`" + escaped + "`";
    }
  },

  /**
   * SQL Server square brackets. Used by Microsoft SQL Server and Azure SQL. Example: [orders].[id]
   */
  SQL_SERVER {
    @Override
    public String quote(String identifier) {
      String escaped = identifier.replace("]", "]]");
      return "[" + escaped + "]";
    }
  };

  /**
   * Quotes a single identifier segment according to this strategy.
   *
   * @param identifier the identifier to quote (e.g., "orders", "id")
   * @return the quoted identifier
   */
  public abstract String quote(String identifier);

  /**
   * Quotes each segment of a qualified name (catalog.schema.table or table.column).
   *
   * @param qualifiedName the qualified name (e.g., "sales.orders" or "mydb.sales.orders")
   * @return the quoted qualified name (e.g., "sales"."orders" for ANSI)
   */
  public String quoteQualified(String qualifiedName) {
    if (qualifiedName == null || qualifiedName.isEmpty()) {
      return qualifiedName;
    }

    String[] parts = qualifiedName.split("\\.");
    StringBuilder result = new StringBuilder();

    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      result.append(quote(parts[i]));
    }

    return result.toString();
  }
}
