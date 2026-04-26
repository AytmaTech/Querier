package com.aytmatech.querier;

import java.util.*;

/** Represents a raw SQL expression or a computed expression. */
public class Expression {
  private final String sql;
  private final Map<String, Object> params;
  private String alias;

  private Expression(String sql, Map<String, Object> params) {
    this.sql = sql;
    this.params = params != null ? Map.copyOf(params) : new HashMap<>();
  }

  /**
   * Creates a raw SQL expression.
   *
   * @param sql The raw SQL string for this expression, which may contain parameter placeholders
   *     (e.g., ":paramName")
   * @return An Expression representing the raw SQL fragment, with no parameters bound
   */
  public static Expression raw(String sql) {
    return new Expression(sql, new HashMap<>());
  }

  /**
   * Creates a table reference for use in FROM clause (e.g., for CTEs).
   *
   * @param tableName The name of the table to reference
   * @return An Expression representing the table reference, which can be used in FROM clauses and
   *     aliased as needed
   */
  public static Expression tableRef(String tableName) {
    return new Expression(tableName, new HashMap<>());
  }

  /**
   * Sets an alias for this expression.
   *
   * @param alias The alias to set for this expression
   * @return This expression instance with the alias set, allowing for method chaining
   */
  public Expression as(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Gets the raw SQL string for this expression (without alias). This is the base SQL fragment that
   * represents this expression, and may contain parameter placeholders (e.g., ":paramName") if
   * parameters are bound to it.
   *
   * @return The raw SQL string for this expression, without alias
   */
  public String getSql() {
    return sql;
  }

  /**
   * Gets the alias for this expression, or null if no alias is set.
   *
   * @return The alias for this expression, or null if no alias is set
   */
  public String getAlias() {
    return alias;
  }

  /**
   * Gets the parameters bound to this expression. The returned map is unmodifiable.
   *
   * @return An unmodifiable map of parameter names to their bound values for this expression
   */
  public Map<String, Object> getParams() {
    return Map.copyOf(params);
  }

  /**
   * Gets the full SQL representation of this expression, including alias if set (e.g., "column_name
   * AS alias").
   *
   * @return The full SQL string for this expression, including alias if set
   */
  public String toSql() {
    if (alias != null) {
      return sql + " AS " + alias;
    }
    return sql;
  }
}
