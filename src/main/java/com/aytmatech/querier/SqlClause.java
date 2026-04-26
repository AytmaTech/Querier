package com.aytmatech.querier;

/** SQL clause keywords used in SELECT statements. */
public enum SqlClause {
  /** SELECT clause keyword. */
  SELECT("SELECT"),
  /** DISTINCT keyword used in SELECT statements. */
  DISTINCT("DISTINCT"),
  /** FROM clause keyword. */
  FROM("FROM"),
  /** WHERE clause keyword. */
  WHERE("WHERE"),
  /** GROUP BY clause keyword. */
  GROUP_BY("GROUP BY"),
  /** HAVING clause keyword. */
  HAVING("HAVING"),
  /** ORDER BY clause keyword. */
  ORDER_BY("ORDER BY"),
  /** LIMIT clause keyword. */
  LIMIT("LIMIT"),
  /** OFFSET clause keyword. */
  OFFSET("OFFSET"),
  /** JOIN clause keywords. */
  WITH("WITH"),
  /** AS keyword used for aliasing in SQL expressions. */
  AS("AS"),
  /** ON keyword used in JOIN conditions. */
  ON("ON"),
  /** OVER clause keyword used for window functions. */
  OVER("OVER"),
  /** PARTITION BY clause keyword used in window functions. */
  PARTITION_BY("PARTITION BY");

  private final String sql;

  SqlClause(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this clause keyword (e.g., "SELECT", "WHERE").
   *
   * @return The SQL string for this clause keyword
   */
  public String getSql() {
    return sql;
  }
}
