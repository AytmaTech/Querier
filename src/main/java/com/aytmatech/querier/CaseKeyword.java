package com.aytmatech.querier;

/** SQL keywords used in CASE WHEN expressions. */
public enum CaseKeyword {
  /** Represents the CASE keyword in a CASE WHEN expression. */
  CASE("CASE"),

  /** Represents the WHEN keyword in a CASE WHEN expression. */
  WHEN("WHEN"),

  /** Represents the THEN keyword in a CASE WHEN expression. */
  THEN("THEN"),

  /** Represents the ELSE keyword in a CASE WHEN expression. */
  ELSE("ELSE"),

  /** Represents the END keyword in a CASE WHEN expression. */
  END("END"),

  /** Represents the AS keyword used for aliasing in SQL expressions. */
  AS("AS");

  private final String sql;

  CaseKeyword(String sql) {
    this.sql = sql;
  }

  /**
   * * Gets the SQL string representation of this keyword (e.g., "CASE", "WHEN").
   *
   * @return The SQL string for this keyword
   */
  public String getSql() {
    return sql;
  }
}
