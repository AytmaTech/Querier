package com.aytmatech.querier;

/** SQL aggregate function names. */
public enum AggregateFunction {
  /** COUNT aggregate function. */
  COUNT("COUNT"),
  /**
   * Special variant of COUNT that renders as {@code COUNT(DISTINCT ...)}. The SQL string is the
   * same as COUNT — the DISTINCT keyword is added separately when building the column expression.
   */
  COUNT_DISTINCT("COUNT"),

  /** SUM aggregate function. */
  SUM("SUM"),

  /** AVG aggregate function. */
  AVG("AVG"),

  /** MIN aggregate function. */
  MIN("MIN"),

  /** MAX aggregate function. */
  MAX("MAX");

  private final String sql;

  AggregateFunction(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL function name (e.g., "COUNT", "SUM"). For COUNT_DISTINCT, this will return "COUNT"
   * — the DISTINCT keyword is handled separately when building the column expression.
   *
   * @return The SQL function name
   */
  public String getSql() {
    return sql;
  }
}
