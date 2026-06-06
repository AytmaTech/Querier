package com.aytmatech.querier;

/** SQL JOIN types. */
public enum JoinType {
  /** Represents an INNER JOIN. */
  INNER("INNER JOIN"),

  /** Represents a LEFT JOIN (or LEFT OUTER JOIN). */
  LEFT("LEFT JOIN"),
  /** Represents a RIGHT JOIN (or RIGHT OUTER JOIN). */
  RIGHT("RIGHT JOIN"),
  /** Represents a FULL OUTER JOIN. */
  FULL("FULL OUTER JOIN"),
  /** Represents a CROSS JOIN. */
  CROSS("CROSS JOIN");

  private final String sql;

  JoinType(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this JOIN type (e.g., "INNER JOIN", "LEFT JOIN").
   *
   * @return The SQL string for this JOIN type
   */
  public String getSql() {
    return sql;
  }
}
