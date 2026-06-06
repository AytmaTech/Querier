package com.aytmatech.querier;

import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a SQL CASE WHEN expression with type-safe conditions.
 *
 * <p>Parameter names are assigned lazily at SQL-generation time. Use {@link #resolve(QuoteStrategy,
 * ParamNameGenerator)} (package-private) when building a full query so that all parameters share
 * one sequential counter. The public {@link #toSql()}, {@link #toSqlWithoutAlias()}, and {@link
 * #getParams()} methods remain available for standalone usage and backward compatibility; they use
 * a fresh {@link ParamNameGenerator} scoped to this instance.
 *
 * <p>Example usage:
 *
 * <pre>
 * CaseWhen caseWhen = CaseWhen.builder()
 *     .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
 *     .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), "In Transit")
 *     .orElse("Pending")
 *     .as("status_label");
 * </pre>
 */
public class CaseWhen {

  private static final Pattern TABLE_COLUMN_PATTERN =
      Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b");

  /** Holds a fully-resolved SQL fragment and its bound parameters. */
  record Resolved(String sql, Map<String, Object> params) {}

  private final List<WhenClause> whenClauses;
  private final AtomicBoolean hasElse = new AtomicBoolean(false);
  private final AtomicReference<Object> elseValue = new AtomicReference<>(null);
  private String alias;

  /** Lazy cache for backward-compatible standalone usage. */
  private final AtomicReference<Resolved> standaloneCache = new AtomicReference<>();

  private CaseWhen() {
    this.whenClauses = new ArrayList<>();
  }

  /**
   * Creates a new CaseWhen builder.
   *
   * @return a new CaseWhen instance for building a CASE WHEN expression
   */
  public static CaseWhen builder() {
    return new CaseWhen();
  }

  /**
   * Adds a WHEN-THEN clause.
   *
   * @param condition the condition to evaluate
   * @param value the value to return when the condition is true
   * @return this instance for chaining
   */
  public CaseWhen when(Condition condition, Object value) {
    if (condition == null) {
      throw new IllegalArgumentException("Condition cannot be null");
    }
    whenClauses.add(new WhenClause(condition, value));
    return this;
  }

  /**
   * Sets the ELSE value.
   *
   * @param value the value to return when no conditions match
   * @return this instance for chaining
   */
  public CaseWhen orElse(Object value) {
    this.elseValue.set(value);
    this.hasElse.set(true);
    return this;
  }

  /**
   * Sets an alias for this CASE WHEN expression.
   *
   * @param alias the alias name
   * @return this instance for chaining
   */
  public CaseWhen as(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Gets the alias for this CASE WHEN expression, or null if no alias is set.
   *
   * @return the alias for this CASE WHEN expression, or null if no alias is set
   */
  public String getAlias() {
    return alias;
  }

  /**
   * Resolves the CASE WHEN SQL <em>without</em> alias, consuming param names from the supplied
   * generator so they are unique within the enclosing query.
   *
   * @param quoteStrategy the quote strategy to apply to identifiers in the generated SQL
   * @param gen the ParamNameGenerator to use for generating unique parameter names for this CASE
   *     WHEN expression within the context of a larger query
   * @return a Resolved object containing the SQL string for this CASE WHEN expression (without
   *     alias) and a map of parameter names to values for
   * @throws IllegalStateException if no WHEN clauses have been added to this CaseWhen instance
   */
  Resolved resolveWithoutAlias(QuoteStrategy quoteStrategy, ParamNameGenerator gen) {
    if (whenClauses.isEmpty()) {
      throw new IllegalStateException("At least one WHEN clause is required");
    }
    StringBuilder sql = new StringBuilder(CaseKeyword.CASE.getSql());
    Map<String, Object> allParams = new HashMap<>();

    for (WhenClause wc : whenClauses) {
      Condition.Resolved condResolved = wc.condition.resolve(quoteStrategy, gen);
      String thenParamName = gen.next();
      sql.append(" ").append(CaseKeyword.WHEN.getSql()).append(" ").append(condResolved.sql());
      sql.append(" ").append(CaseKeyword.THEN.getSql()).append(" :").append(thenParamName);
      allParams.putAll(condResolved.params());
      allParams.put(thenParamName, wc.thenValue);
    }

    if (hasElse.get()) {
      String elseParamName = gen.next();
      sql.append(" ").append(CaseKeyword.ELSE.getSql()).append(" :").append(elseParamName);
      allParams.put(elseParamName, elseValue.get());
    }

    sql.append(" ").append(CaseKeyword.END.getSql());
    return new Resolved(sql.toString(), allParams);
  }

  /**
   * Resolves the full CASE WHEN SQL (including alias if set).
   *
   * @param quoteStrategy the quote strategy to apply to identifiers in the generated SQL
   * @param gen the ParamNameGenerator to use for generating unique parameter names for this CASE
   *     WHEN expression within the context of a larger query
   * @return a Resolved object containing the full SQL string for this CASE WHEN expression
   *     (including alias if set) and a map of parameter names to values for all parameters used in
   *     this expression
   * @throws IllegalStateException if no WHEN clauses have been added to this CaseWhen instance
   */
  Resolved resolve(QuoteStrategy quoteStrategy, ParamNameGenerator gen) {
    Resolved r = resolveWithoutAlias(quoteStrategy, gen);
    String sql = alias != null ? r.sql() + " " + CaseKeyword.AS.getSql() + " " + alias : r.sql();
    return new Resolved(sql, r.params());
  }

  /**
   * Generates SQL for this CASE WHEN expression without quote strategy.
   *
   * @return the SQL string for this CASE WHEN expression, including alias if set
   */
  public String toSql() {
    return toSql(QuoteStrategy.NONE);
  }

  /**
   * Generates SQL for this CASE WHEN expression with the specified quote strategy.
   *
   * @param quoteStrategy the quote strategy to apply to identifiers in the generated SQL
   * @return the SQL string for this CASE WHEN expression, including alias if set, with quoted
   *     identifiers according to the specified strategy.
   */
  public String toSql(QuoteStrategy quoteStrategy) {
    String sql = toSqlWithoutAlias(quoteStrategy);
    if (alias != null) {
      sql += " " + CaseKeyword.AS.getSql() + " " + alias;
    }
    return sql;
  }

  /**
   * Generates SQL without alias — useful when CaseWhen is used inside Aggregate functions or ORDER
   * BY.
   *
   * @return the SQL string for this CASE WHEN expression without alias, with no quote strategy
   *     applied.
   */
  public String toSqlWithoutAlias() {
    return toSqlWithoutAlias(QuoteStrategy.NONE);
  }

  /**
   * Generates SQL without alias with the specified quote strategy.
   *
   * @param quoteStrategy the quote strategy to apply to identifiers in the generated SQL
   * @return the SQL string for this CASE WHEN expression without alias, with quoted identifiers
   *     according to the specified strategy.
   */
  public String toSqlWithoutAlias(QuoteStrategy quoteStrategy) {
    Resolved cached = ensureStandaloneCache();
    if (quoteStrategy == QuoteStrategy.NONE) {
      return cached.sql();
    }
    return applyQuoteStrategy(cached.sql(), quoteStrategy);
  }

  /**
   * Returns all parameters from this CASE WHEN expression.
   *
   * @return a map of parameter names to values for all parameters used in this CASE WHEN
   *     expression.
   */
  public Map<String, Object> getParams() {
    return ensureStandaloneCache().params();
  }

  /**
   * Ensures the standalone cache is populated with a resolved SQL and params for this CaseWhen.
   * This is used for the public toSql() and getParams() methods to provide consistent results
   * without requiring a QuoteStrategy or ParamNameGenerator. The cache is populated on first access
   * and reused for subsequent calls.
   *
   * @return a Resolved object containing the SQL and parameters for this CaseWhen, cached for
   *     standalone usage.
   */
  private Resolved ensureStandaloneCache() {
    if (standaloneCache.get() == null) {
      synchronized (this) {
        if (standaloneCache.get() == null) {
          Resolved r = resolveWithoutAlias(QuoteStrategy.NONE, new ParamNameGenerator());
          standaloneCache.set(new Resolved(r.sql(), Map.copyOf(r.params())));
        }
      }
    }
    return standaloneCache.get();
  }

  /**
   * Applies the specified quote strategy to all table.column patterns in the given SQL string. This
   * is used to support quoting identifiers in the generated SQL for this CaseWhen expression when
   * using the public toSql() method with a QuoteStrategy.
   *
   * @param sql the SQL string to which the quote strategy should be applied. This string is
   *     expected to contain table.column patterns that need to be quoted according to the specified
   *     strategy.
   * @param quoteStrategy the QuoteStrategy to apply to identifiers in the SQL string. This
   *     determines how table and column names will be quoted (e.g., with backticks for MySQL,
   *     double quotes for ANSI, etc.).
   * @return a new SQL string with the quote strategy applied to all table.column patterns. For
   *     example, if the input SQL contains "orders.id" and the quote strategy is ANSI, the output
   *     will contain "\"orders\".\"id\"".
   */
  private static String applyQuoteStrategy(String sql, QuoteStrategy quoteStrategy) {
    Matcher matcher = TABLE_COLUMN_PATTERN.matcher(sql);
    StringBuilder result = new StringBuilder();
    int lastEnd = 0;
    while (matcher.find()) {
      result.append(sql, lastEnd, matcher.start());
      result
          .append(quoteStrategy.quote(matcher.group(1)))
          .append(".")
          .append(quoteStrategy.quote(matcher.group(2)));
      lastEnd = matcher.end();
    }
    result.append(sql.substring(lastEnd));
    return result.toString();
  }

  /** Represents a WHEN-THEN clause. */
  private static class WhenClause {
    final Condition condition;
    final Object thenValue;

    WhenClause(Condition condition, Object thenValue) {
      this.condition = condition;
      this.thenValue = thenValue;
    }
  }
}
