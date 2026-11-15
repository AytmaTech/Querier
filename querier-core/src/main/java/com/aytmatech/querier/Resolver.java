package com.aytmatech.querier;

import com.aytmatech.querier.util.ParamNameGenerator;

/**
 * Functional interface for lazily resolving a Querier SQL Clause to SQL and parameters. This allows
 * for deferred resolution of column references and parameter names until the full query context is
 * available, ensuring that all parameter names are unique and that quote strategies can be applied
 * consistently across the entire query. Usage example:
 *
 * <pre>{@code
 * DbFunction.custom((strategy, gen) -> {
 *     NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
 *     String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
 *     return new DbFunction.Resolved("CAST(" + colSql + " AS TEXT)", Map.of());
 * })
 * }</pre>
 *
 * @see ParamNameGenerator
 * @see QuoteStrategy
 */
@FunctionalInterface
public interface Resolver {

  /**
   * Resolves the SQL fragment and bound parameters for a database function call.
   *
   * @param strategy the quoting strategy to apply to table and column names
   * @param gen the parameter name generator to use for any bound parameters
   * @return a {@code Resolved} instance containing the SQL fragment and bound parameters
   */
  Resolved resolve(QuoteStrategy strategy, ParamNameGenerator gen);
}
