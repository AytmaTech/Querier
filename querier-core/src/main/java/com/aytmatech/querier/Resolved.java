package com.aytmatech.querier;

import java.util.Map;

/**
 * Carries a resolved SQL fragment and its bound parameters.
 *
 * @param sql the SQL fragment
 * @param params the bound parameters
 */
public record Resolved(String sql, Map<String, Object> params) {

  /**
   * Creates a new {@code Resolved} instance with the given SQL fragment and bound parameters.
   *
   * @param sql the SQL fragment
   * @param params the bound parameters
   */
  public Resolved(String sql, Map<String, Object> params) {
    this.sql = sql;
    this.params = Map.copyOf(params);
  }
}
