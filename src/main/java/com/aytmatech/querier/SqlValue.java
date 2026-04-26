package com.aytmatech.querier;

/**
 * Enum representing special SQL values that can be used in queries, such as ALL, TRUE, FALSE, and
 * NULL.
 */
public enum SqlValue {
  /** Represents the SQL wildcard value/ */
  ALL("*"),
  /** Represents the SQL boolean value TRUE. */
  TRUE("TRUE"),
  /** Represents the SQL boolean value FALSE. */
  FALSE("FALSE"),
  /** Represents the SQL NULL value. */
  NULL("NULL");

  private final String sql;

  SqlValue(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this value (e.g., "*", "TRUE", "FALSE", "NULL").
   *
   * @return The SQL string for this value
   */
  public String getSql() {
    return sql;
  }
}
