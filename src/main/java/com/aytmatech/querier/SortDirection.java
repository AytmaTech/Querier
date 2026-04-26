package com.aytmatech.querier;

/** SQL sort direction for ORDER BY clauses. */
public enum SortDirection {
  /** Ascending sort direction, rendered as "ASC". */
  ASC("ASC"),
  /** Descending sort direction, rendered as "DESC". */
  DESC("DESC");

  private final String sql;

  SortDirection(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this sort direction (e.g., "ASC" or "DESC").
   *
   * @return The SQL string for this sort direction
   */
  public String getSql() {
    return sql;
  }
}
