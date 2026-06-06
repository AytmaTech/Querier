package com.aytmatech.querier;

/** SQL set operation keywords. */
public enum SetOperationType {
  /** Represents the UNION set operation. */
  UNION("UNION"),
  /** Represents the UNION ALL set operation. */
  UNION_ALL("UNION ALL"),
  /** Represents the INTERSECT set operation. */
  INTERSECT("INTERSECT"),
  /** Represents the EXCEPT set operation. */
  EXCEPT("EXCEPT");

  private final String sql;

  SetOperationType(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this set operation (e.g., "UNION", "INTERSECT").
   *
   * @return The SQL string for this set operation
   */
  public String getSql() {
    return sql;
  }
}
