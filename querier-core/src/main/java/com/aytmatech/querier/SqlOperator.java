package com.aytmatech.querier;

/** SQL comparison and logical operator keywords. */
public enum SqlOperator {
  /** Represents the equality operator (=) in SQL. */
  EQ("="),
  /** Represents the inequality operator (!=) in SQL. */
  NE("!="),
  /** Represents the greater than operator (&gt;) in SQL. */
  GT(">"),
  /** Represents the less than operator (&lt;) in SQL. */
  LT("<"),
  /** Represents the greater than or equal to operator (&ge;) in SQL. */
  GTE(">="),
  /** Represents the less than or equal to operator (&le;) in SQL. */
  LTE("<="),
  /** Represents the LIKE operator in SQL. */
  LIKE("LIKE"),
  /** Represents the NOT LIKE operator in SQL. */
  NOT_LIKE("NOT LIKE"),
  /** Represents the IN operator in SQL. */
  IN("IN"),
  /** Represents the NOT IN operator in SQL. */
  NOT_IN("NOT IN"),
  /** Represents the BETWEEN operator in SQL. */
  BETWEEN("BETWEEN"),
  /** Represents the NOT BETWEEN operator in SQL. */
  IS_NULL("IS NULL"),
  /** Represents the IS NOT NULL operator in SQL. */
  IS_NOT_NULL("IS NOT NULL"),
  /** Represents the AND operator in SQL. */
  AND("AND"),
  /** Represents the OR operator in SQL. */
  OR("OR"),
  /** Represents the NOT operator in SQL. */
  NOT("NOT"),
  /** Represents the EXISTS operator in SQL. */
  EXISTS("EXISTS");

  private final String sql;

  SqlOperator(String sql) {
    this.sql = sql;
  }

  /**
   * Gets the SQL string representation of this operator (e.g., "=", "AND").
   *
   * @return The SQL string for this operator
   */
  public String getSql() {
    return sql;
  }
}
