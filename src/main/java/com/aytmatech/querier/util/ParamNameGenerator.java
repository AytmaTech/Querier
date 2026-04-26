package com.aytmatech.querier.util;

/**
 * Generates sequential parameter names for a single query build. Each call to {@link #next()}
 * returns the next name in the sequence: {@code param0}, {@code param1}, {@code param2}, …
 *
 * <p>Create one instance per query build (i.e. per {@code toSqlAndParams()} call) so that the
 * counter naturally resets for each new query. Pass the same instance through all SQL-generation
 * calls within that query to ensure unique, sequential names with no global state.
 */
public class ParamNameGenerator {
  private int counter = 0;

  /**
   * Gets the next parameter name in the sequence, formatted as "param" followed by a unique integer
   * (e.g., "param0", "param1", "param2", …). Each call to this method increments the internal
   * counter to ensure that subsequent calls return unique names.
   *
   * @return The next parameter name in the sequence (e.g., "param0", "param1", "param2", …)
   */
  public String next() {
    return "param" + counter++;
  }
}
