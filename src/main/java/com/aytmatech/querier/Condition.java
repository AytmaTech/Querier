package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents SQL WHERE and JOIN conditions.
 *
 * <p>Parameter names are assigned lazily at SQL-generation time by a {@link ParamNameGenerator}
 * instance that is shared across the whole query, so every parameter within one {@code
 * toSqlAndParams()} call gets a unique, sequential name ({@code param0}, {@code param1}, …).
 */
public class Condition {
  private static final Pattern TABLE_COLUMN_PATTERN =
      Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b");

  /**
   * Functional interface for lazily resolving a Condition to SQL and parameters. This allows for
   * deferred resolution of column references and parameter names until the full query context is
   * available, ensuring that all parameter names are unique and that quote strategies can be
   * applied consistently across the entire query.
   *
   * @see ParamNameGenerator
   * @see QuoteStrategy
   */
  @FunctionalInterface
  interface Resolver {
    Resolved resolve(QuoteStrategy quoteStrategy, ParamNameGenerator gen);
  }

  /** Holds a fully-resolved SQL fragment and its bound parameters. */
  record Resolved(String sql, Map<String, Object> params) {}

  private final Resolver resolver;

  /** Lazy cache used by the backward-compatible {@code getSql()} / {@code getParams()} methods. */
  private final AtomicReference<Resolved> standaloneCache = new AtomicReference<>();

  private Condition(Resolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Resolves this condition to SQL + params using the supplied generator. The generator is shared
   * with the whole query so that all parameter names are unique within a single {@code
   * toSqlAndParams()} call.
   *
   * @param gen the parameter name generator to use for generating unique parameter names during
   *     resolution
   * @param quoteStrategy the quote strategy to apply to table and column names during resolution
   * @return a Resolved object containing the resolved SQL string and its associated parameters,
   *     with all parameter names generated using the provided generator and all identifiers quoted
   *     according to the specified quote strategy
   */
  Resolved resolve(QuoteStrategy quoteStrategy, ParamNameGenerator gen) {
    return resolver.resolve(quoteStrategy, gen);
  }

  /**
   * Gets the SQL string for this condition without applying any quoting to table and column names.
   * This method uses a lazy cache to store the resolved SQL and parameters on the first call, so
   * subsequent calls will return the cached SQL string without re-resolving the condition. Note
   * that this method is not thread-safe due to the lazy caching mechanism, so it should only be
   * used in single-threaded contexts or when external synchronization is applied.
   *
   * @return the SQL string for this condition, with no quoting applied to identifiers. The SQL
   *     string is resolved lazily and cached for subsequent calls, but this method is not
   *     thread-safe due to the lazy caching mechanism. For thread-safe usage with quoting, use
   *     {@code getSql(QuoteStrategy)} instead.
   */
  public String getSql() {
    return getSql(QuoteStrategy.NONE);
  }

  /**
   * Gets the SQL with the specified quote strategy applied to table and column names.
   *
   * @param quoteStrategy the quote strategy to apply
   * @return the SQL with quoted identifiers
   */
  public String getSql(QuoteStrategy quoteStrategy) {
    ensureStandaloneCache();
    if (quoteStrategy == QuoteStrategy.NONE) {
      return standaloneCache.get().sql();
    }
    return applyQuoteStrategy(standaloneCache.get().sql(), quoteStrategy);
  }

  /**
   * Gets the parameters for this condition. This method uses a lazy cache to store the resolved SQL
   * and parameters on the first call, so subsequent calls will return the cached parameters without
   * re-resolving the condition. Note that this method is not thread-safe due to the lazy caching
   * mechanism, so it should only be used in single-threaded contexts or when external
   * synchronization is applied.
   *
   * @return an unmodifiable map of parameter names to their bound values for this condition. The
   *     parameters are resolved lazily and cached for subsequent calls, but this method is not
   *     thread-safe due to the lazy caching mechanism. For thread-safe usage with quoting, use
   *     {@code getSql(QuoteStrategy)} together with {@code resolve(QuoteStrategy,
   *     ParamNameGenerator)} instead.
   */
  public Map<String, Object> getParams() {
    ensureStandaloneCache();
    return standaloneCache.get().params();
  }

  private void ensureStandaloneCache() {
    if (standaloneCache.get() == null) {
      synchronized (this) {
        if (standaloneCache.get() == null) {
          Resolved r = resolver.resolve(QuoteStrategy.NONE, new ParamNameGenerator());
          standaloneCache.set(new Resolved(r.sql(), Map.copyOf(r.params())));
        }
      }
    }
  }

  /**
   * Applies quote strategy to all {@code table.column} patterns in the SQL string.
   *
   * @param sql the SQL string to apply quoting to
   * @param quoteStrategy the quote strategy to apply to table and column names
   * @return the SQL string with the quote strategy applied to all table and column names in {@code
   *     table.column} patterns. This method uses a regular expression to find all occurrences of
   *     qualified identifiers in the form of "table.column" and applies the specified quote
   *     strategy to both the table and column parts, ensuring that all identifiers are properly
   *     quoted according to the chosen strategy.
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

  /**
   * column = value
   *
   * @param columnRef the column reference (e.g., orders.id)
   * @param value the value to compare to (e.g., 123)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column = :param", with the parameter value
   *     bound to the provided value.
   */
  public static <T, R> Condition eq(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.EQ, value);
  }

  /**
   * column1 = column2 (for JOINs)
   *
   * @param columnRef1 the first column reference (e.g., orders.customer_id)
   * @param columnRef2 the second column reference (e.g., customers.id)
   * @param <T1> The entity type of the first column
   * @param <R1> The return type of the first column
   * @param <T2> The entity type of the second column
   * @param <R2> The return type of the second column
   * @return a Condition representing the SQL condition "column1 = column2", used for JOIN
   *     conditions where two columns from different tables are compared for equality.
   */
  public static <T1, R1, T2, R2> Condition eq(
      ColumnRef<T1, R1> columnRef1, ColumnRef<T2, R2> columnRef2) {
    return columnColumnCondition(columnRef1, SqlOperator.EQ, columnRef2);
  }

  /**
   * column1 = column2 (for JOINs)
   *
   * @param expression the first expression (e.g., a raw SQL expression or an aggregate function)
   * @param columnRef the column reference to compare to (e.g., customers.id)
   * @param <T> The entity type of the column reference
   * @param <R> The return type of the column reference
   * @return a Condition representing the SQL condition "expression = column", used for JOIN
   *     conditions where a computed expression is compared to a column for equality.
   */
  public static <T, R> Condition eq(Expression expression, ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          String sql =
              expression.getSql()
                  + " "
                  + SqlOperator.EQ.getSql()
                  + " "
                  + qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName());
          return new Resolved(sql, new HashMap<>());
        });
  }

  /**
   * column1 = column2 (for JOINs)
   *
   * @param expression1 the first expression (e.g., a raw SQL expression or an aggregate function)
   * @param expression2 the second expression to compare to (e.g., another raw SQL expression or
   *     aggregate function)
   * @return a Condition representing the SQL condition "expression1 = expression2", used for JOIN
   *     conditions where two computed expressions are compared for equality.
   */
  public static Condition eq(Expression expression1, Expression expression2) {
    return new Condition(
        (qs, gen) -> {
          String sql =
              expression1.getSql() + " " + SqlOperator.EQ.getSql() + " " + expression2.getSql();
          return new Resolved(sql, new HashMap<>());
        });
  }

  /**
   * column != value
   *
   * @param columnRef the column reference (e.g., orders.id)
   * @param value the value to compare to (e.g., 123)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column != :param", with the parameter value
   *     bound to the provided value.
   */
  public static <T, R> Condition ne(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.NE, value);
  }

  /**
   * column &gt; value
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param value the value to compare to (e.g., 100)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column > :param", with the parameter value
   *     bound to the provided value.
   */
  public static <T, R> Condition gt(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.GT, value);
  }

  /**
   * column &gt; value
   *
   * @param aggregate the aggregate expression (e.g., COUNT(orders.id))
   * @param value the value to compare to (e.g., 5)
   * @return a Condition representing the SQL condition "aggregate > :param", with the parameter
   *     value bound to the provided value. This is used for HAVING conditions where an aggregate
   *     function is compared to a value.
   */
  public static Condition gt(Aggregate aggregate, Object value) {
    return columnValueCondition(aggregate, SqlOperator.GT, value);
  }

  /**
   * raw expression &gt; value (for expressions in WHERE)
   *
   * @param expression the raw SQL expression (e.g., "orders.amount - orders.discount")
   * @param value the value to compare to (e.g., 50)
   * @return a Condition representing the SQL condition "expression > :param", with the parameter
   *     value bound to the provided value. This is used for conditions where a computed expression
   *     is compared to a value for filtering.
   */
  public static Condition gt(Expression expression, Object value) {
    return columnValueCondition(expression, SqlOperator.GT, value);
  }

  /**
   * column &lt; value
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param value the value to compare to (e.g., 100)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column &lt; :param", with the parameter
   *     value bound to the provided value.
   */
  public static <T, R> Condition lt(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.LT, value);
  }

  /**
   * column &ge; value
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param value the value to compare to (e.g., 100)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column >= :param", with the parameter value
   *     bound to the provided value.
   */
  public static <T, R> Condition gte(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.GTE, value);
  }

  /**
   * column &le; value
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param value the value to compare to (e.g., 100)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column &le; :param", with the parameter
   *     value bound to the provided value.
   */
  public static <T, R> Condition lte(ColumnRef<T, R> columnRef, Object value) {
    return columnValueCondition(columnRef, SqlOperator.LTE, value);
  }

  /**
   * column LIKE pattern
   *
   * @param columnRef the column reference (e.g., customers.name)
   * @param pattern the LIKE pattern to match (e.g., "A%")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column LIKE :param", with the parameter
   *     value bound to the provided pattern. This is used for string matching conditions where a
   *     column's value is compared to a pattern using the SQL LIKE operator.
   */
  public static <T, R> Condition like(ColumnRef<T, R> columnRef, String pattern) {
    return columnValueCondition(columnRef, SqlOperator.LIKE, pattern);
  }

  /**
   * column = (subquery)
   *
   * @param columnRef the column reference (e.g., orders.customer_id)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT id FROM
   *     customers WHERE name = 'Alice'")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column = (subquery)", where the column is
   *     compared for equality to the result of the subquery. This is used for conditions where a
   *     column's value is compared to the result of a subquery, such as when filtering for rows
   *     that match a specific value returned by another query.
   */
  public static <T, R> Condition eq(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.EQ, subquery);
  }

  /**
   * column != (subquery)
   *
   * @param columnRef the column reference (e.g., orders.customer_id)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT id FROM
   *     customers WHERE name = 'Alice'")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column != (subquery)", where the column is
   *     compared for inequality to the result of the subquery. This is used for conditions where a
   *     column's value is compared to the result of a subquery for inequality, such as when
   *     filtering for rows that do not match a specific value returned by another query.
   */
  public static <T, R> Condition ne(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.NE, subquery);
  }

  /**
   * column &gt; (subquery)
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT AVG(amount)
   *     FROM orders")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column > (subquery)", where the column is
   *     compared for being greater than the result of the subquery. This is used for conditions
   *     where a column's value is compared to the result of a subquery for being greater than, such
   *     as when filtering for rows where a column's value exceeds a specific value returned by
   *     another query.
   */
  public static <T, R> Condition gt(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.GT, subquery);
  }

  /**
   * column &lt; (subquery)
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT AVG(amount)
   *     FROM orders")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column &lt; (subquery)", where the column
   *     is compared for being less than the result of the subquery. This is used for conditions
   *     where a column's value is compared to the result of a subquery for being less than, such as
   *     when filtering for rows where a column's value is below a specific value returned by
   *     another query.
   */
  public static <T, R> Condition lt(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.LT, subquery);
  }

  /**
   * column &ge; (subquery)
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT AVG(amount)
   *     FROM orders")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column >= (subquery)", where the column is
   *     compared for being greater than or equal to the result of the subquery. This is used for
   *     conditions where a column's value is compared to the result of a subquery for being greater
   *     than or equal to, such as when filtering for rows where a column's value meets or exceeds a
   *     specific value returned by another query.
   */
  public static <T, R> Condition gte(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.GTE, subquery);
  }

  /**
   * column &le; (subquery)
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT AVG(amount)
   *     FROM orders")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column &le; (subquery)", where the column
   *     is compared for being less than or equal to the result of the subquery. This is used for
   *     conditions where a column's value is compared to the result of a subquery for being less
   *     than or equal to, such as when filtering for rows where a column's value meets or falls
   *     below a specific value returned by another query.
   */
  public static <T, R> Condition lte(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.LTE, subquery);
  }

  /**
   * column IN (values)
   *
   * @param columnRef the column reference (e.g., orders.status)
   * @param values the collection of values to compare to (e.g., List.of("PENDING", "SHIPPED"))
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column IN (:param)", where the column is
   *     compared for membership in the provided collection of values. The parameter value is bound
   *     to the collection, allowing for filtering rows where a column's value matches any of the
   *     specified values.
   */
  public static <T, R> Condition in(ColumnRef<T, R> columnRef, Collection<?> values) {
    return columnCollectionCondition(columnRef, SqlOperator.IN, values);
  }

  /**
   * column NOT IN (values)
   *
   * @param columnRef the column reference (e.g., orders.status)
   * @param values the collection of values to compare to (e.g., List.of("PENDING", "SHIPPED"))
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column NOT IN (:param)", where the column
   *     is compared for non-membership in the provided collection of values. The parameter value is
   *     bound to the collection, allowing for filtering rows where a column's value does not match
   *     any of the specified values.
   */
  public static <T, R> Condition notIn(ColumnRef<T, R> columnRef, Collection<?> values) {
    return columnCollectionCondition(columnRef, SqlOperator.NOT_IN, values);
  }

  /**
   * column IN (subquery)
   *
   * @param columnRef the column reference (e.g., orders.customer_id)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT id FROM
   *     customers WHERE name = 'Alice'")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column IN (subquery)", where the column is
   *     compared for membership in the set of values returned by the subquery. This is used for
   *     conditions where a column's value is checked against a list of values produced by another
   *     query, such as when filtering for rows that match any value returned by a subquery.
   */
  public static <T, R> Condition in(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.IN, subquery);
  }

  /**
   * column NOT IN (subquery)
   *
   * @param columnRef the column reference (e.g., orders.customer_id)
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT id FROM
   *     customers WHERE name = 'Alice'")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column NOT IN (subquery)", where the column
   *     is compared for non-membership in the set of values returned by the subquery. This is used
   *     for conditions where a column's value is checked against a list of values produced by
   *     another query for non-membership, such as when filtering for rows that do not match any
   *     value returned by a subquery.
   */
  public static <T, R> Condition notIn(ColumnRef<T, R> columnRef, Select subquery) {
    return columnSubqueryCondition(columnRef, SqlOperator.NOT_IN, subquery);
  }

  /**
   * expression IN (values)
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param values the collection of values to compare to (e.g., List.of(100, 200, 300))
   * @return a Condition representing the SQL condition "expression IN (:param)", where the
   *     expression is compared for membership in the provided collection of values. The parameter
   *     value is bound to the collection, allowing for filtering rows where a computed expression's
   *     value matches any of the specified values.
   */
  public static Condition in(Expression expression, Collection<?> values) {
    return columnCollectionCondition(expression, SqlOperator.IN, values);
  }

  /**
   * expression NOT IN (values)
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param values the collection of values to compare to (e.g., List.of(100, 200, 300))
   * @return a Condition representing the SQL condition "expression NOT IN (:param)", where the
   *     expression is compared for non-membership in the provided collection of values. The
   *     parameter value is bound to the collection, allowing for filtering rows where a computed
   *     expression's value does not match any of the specified values.
   */
  public static Condition notIn(Expression expression, Collection<?> values) {
    return columnCollectionCondition(expression, SqlOperator.NOT_IN, values);
  }

  /**
   * expression IN (subquery)
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT amount FROM
   *     orders WHERE customer_id = 123")
   * @return a Condition representing the SQL condition "expression IN (subquery)", where the
   *     expression is compared for membership in the set of values returned by the subquery. This
   *     is used for conditions where a computed expression's value is checked against a list of
   *     values produced by another query, such as when filtering for rows where a computed
   *     expression's value matches any value returned by a subquery.
   */
  public static Condition in(Expression expression, Select subquery) {
    return columnSubqueryCondition(expression, SqlOperator.IN, subquery);
  }

  /**
   * expression NOT IN (subquery)
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT amount FROM
   *     orders WHERE customer_id = 123")
   * @return a Condition representing the SQL condition "expression NOT IN (subquery)", where the
   *     expression is compared for non-membership in the set of values returned by the subquery.
   *     This is used for conditions where a computed expression's value is checked against a list
   *     of values produced by another query for non-membership, such as when filtering for rows
   *     where a computed expression's value does not match any value returned by a subquery.
   */
  public static Condition notIn(Expression expression, Select subquery) {
    return columnSubqueryCondition(expression, SqlOperator.NOT_IN, subquery);
  }

  /**
   * column IS NULL
   *
   * @param columnRef the column reference (e.g., customers.name)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column IS NULL", used for filtering rows
   *     where a column's value is null.
   */
  public static <T, R> Condition isNull(ColumnRef<T, R> columnRef) {
    return columnSubqueryCondition(columnRef, SqlOperator.IS_NULL);
  }

  /**
   * column IS NOT NULL
   *
   * @param columnRef the column reference (e.g., customers.name)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column IS NOT NULL", used for filtering
   *     rows where a column's value is not null.
   */
  public static <T, R> Condition isNotNull(ColumnRef<T, R> columnRef) {
    return columnSubqueryCondition(columnRef, SqlOperator.IS_NOT_NULL);
  }

  /**
   * column BETWEEN min AND max
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param min the minimum value for the BETWEEN condition (e.g., 100)
   * @param max the maximum value for the BETWEEN condition (e.g., 200)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column BETWEEN :min AND :max", where the
   *     column's value is compared to be within the range defined by the provided minimum and
   *     maximum values
   */
  public static <T, R> Condition between(ColumnRef<T, R> columnRef, Object min, Object max) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          String p1 = gen.next();
          String p2 = gen.next();
          String sql =
              qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName())
                  + " "
                  + SqlOperator.BETWEEN.getSql()
                  + " :"
                  + p1
                  + " "
                  + SqlOperator.AND.getSql()
                  + " :"
                  + p2;
          Map<String, Object> params = new HashMap<>();
          params.put(p1, min);
          params.put(p2, max);
          return new Resolved(sql, params);
        });
  }

  /**
   * EXISTS (subquery)
   *
   * @param subquery the subquery to check for existence (e.g., a Select representing "SELECT 1 FROM
   *     orders WHERE customer_id = customers.id")
   * @return a Condition representing the SQL condition "EXISTS (subquery)", used for filtering rows
   *     based on the existence of related rows in another table or query. This condition evaluates
   *     to true if the subquery returns any rows, allowing for checks like "find customers for whom
   *     there exists an order with a certain condition".
   */
  public static Condition exists(Select subquery) {
    return new Condition(
        (qs, gen) -> {
          Select.SqlAndParams sp = subquery.toSqlAndParams(gen);
          return new Resolved(
              SqlOperator.EXISTS.getSql() + " (" + sp.sql() + ")", new HashMap<>(sp.params()));
        });
  }

  /**
   * condition1 AND condition2 AND ...
   *
   * @param conditions the array of conditions to combine with AND (e.g., List.of(cond1, cond2,
   *     cond3))
   * @return a Condition representing the SQL condition "condition1 AND condition2 AND ...", where
   *     the provided conditions are combined using the AND operator. This allows for building
   *     complex conditions by combining multiple simpler conditions together, such as when
   *     filtering for rows that must satisfy multiple criteria simultaneously.
   * @throws IllegalArgumentException if no conditions are provided (i.e., the array is empty),
   *     since an AND condition requires at least one condition to be meaningful. If only one
   *     condition is provided, it is returned as-is without wrapping in an AND, since a single
   *     condition does not need to be combined with anything else.
   */
  public static Condition and(Condition... conditions) {
    if (conditions.length == 0) {
      throw new IllegalArgumentException("At least one condition required");
    }
    if (conditions.length == 1) {
      return conditions[0];
    }
    return new Condition(
        (qs, gen) -> {
          StringBuilder sql = new StringBuilder("(");
          Map<String, Object> allParams = new HashMap<>();
          for (int i = 0; i < conditions.length; i++) {
            if (i > 0) sql.append(" ").append(SqlOperator.AND.getSql()).append(" ");
            Resolved r = conditions[i].resolve(qs, gen);
            sql.append(r.sql());
            allParams.putAll(r.params());
          }
          sql.append(")");
          return new Resolved(sql.toString(), allParams);
        });
  }

  /**
   * condition1 OR condition2 OR ...
   *
   * @param conditions the array of conditions to combine with OR (e.g., List.of(cond1, cond2,
   *     cond3))
   * @return a Condition representing the SQL condition "condition1 OR condition2 OR ...", where the
   *     provided conditions are combined using the OR operator. This allows for building complex
   *     conditions by combining multiple simpler conditions together with OR logic, such as when
   *     filtering for rows that can satisfy any of several criteria
   * @throws IllegalArgumentException if no conditions are provided (i.e., the array is empty),
   *     since an OR condition requires at least one condition to be meaningful. If only one
   *     condition is provided, it is returned as-is without wrapping in an OR, since a single
   *     condition does not need to be combined with anything else.
   */
  public static Condition or(Condition... conditions) {
    if (conditions.length == 0) {
      throw new IllegalArgumentException("At least one condition required");
    }
    if (conditions.length == 1) {
      return conditions[0];
    }
    return new Condition(
        (qs, gen) -> {
          StringBuilder sql = new StringBuilder("(");
          Map<String, Object> allParams = new HashMap<>();
          for (int i = 0; i < conditions.length; i++) {
            if (i > 0) sql.append(" ").append(SqlOperator.OR.getSql()).append(" ");
            Resolved r = conditions[i].resolve(qs, gen);
            sql.append(r.sql());
            allParams.putAll(r.params());
          }
          sql.append(")");
          return new Resolved(sql.toString(), allParams);
        });
  }

  /**
   * NOT condition
   *
   * @param condition the condition to negate (e.g., a Condition representing "orders.amount > 100")
   * @return a Condition representing the SQL condition "NOT (condition)", where the provided
   *     condition is negated using the NOT operator. This allows for building conditions that
   *     filter for rows that do not satisfy a certain condition, such as when filtering for rows
   *     where a column's value is not greater than a specific threshold.
   */
  public static Condition not(Condition condition) {
    return new Condition(
        (qs, gen) -> {
          Resolved r = condition.resolve(qs, gen);
          return new Resolved(SqlOperator.NOT.getSql() + " (" + r.sql() + ")", r.params());
        });
  }

  /**
   * Builds a {@code column OP (subquery)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param columnRef the column reference (e.g., orders.customer_id)
   * @param op the SQL operator to use (e.g., SqlOperator.EQ for "column = (subquery)")
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT id FROM
   *     customers WHERE name = 'Alice'")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column OP (subquery)", where the column is
   *     compared to the result of the subquery using the specified operator. This is used for
   *     conditions where a column's value is compared to the result of a subquery using various
   *     operators, such as "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", etc
   */
  private static <T, R> Condition columnSubqueryCondition(
      ColumnRef<T, R> columnRef, SqlOperator op, Select subquery) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          Select.SqlAndParams sp = subquery.toSqlAndParams(gen);
          String sql =
              qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName())
                  + " "
                  + op.getSql()
                  + " ("
                  + sp.sql()
                  + ")";
          return new Resolved(sql, new HashMap<>(sp.params()));
        });
  }

  /**
   * Builds a {@code column OP (value)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param columnRef the column reference (e.g., orders.amount)
   * @param op the SQL operator to use (e.g., SqlOperator.GT for "column > :param")
   * @param value the value to compare to (e.g., 100)
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column OP :param", where the column is
   *     compared to the provided value using the specified operator. The parameter value is bound
   *     to the provided value, allowing for filtering rows based on a column's value compared to a
   *     specific value using various operators, such as "=", "!=", ">", "<", ">=", "<=", "LIKE",
   *     etc.
   */
  private static <T, R> Condition columnValueCondition(
      ColumnRef<T, R> columnRef, SqlOperator op, Object value) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          String paramName = gen.next();
          String sql =
              qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName())
                  + " "
                  + op.getSql()
                  + " :"
                  + paramName;
          return new Resolved(sql, Map.of(paramName, value));
        });
  }

  /**
   * Builds a {@code column OP (column)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param columnRef1 the first column reference (e.g., orders.customer_id)
   * @param op the SQL operator to use (e.g., SqlOperator.EQ for "column1 = column2")
   * @param columnRef2 the second column reference (e.g., customers.id)
   * @param <T1> The entity type of the first column
   * @param <R1> The return type of the first column
   * @param <T2> The entity type of the second column
   * @param <R2> The return type of the second column
   * @return a Condition representing the SQL condition "column1 OP column2", where the first column
   *     is compared to the second column using the specified operator. This is used for conditions
   *     where two columns are compared to each other using various operators, such as "=", "!=",
   *     ">", "<", ">=", "<=", etc., often in JOIN conditions or when comparing columns within the
   *     same table.
   */
  private static <T1, R1, T2, R2> Condition columnColumnCondition(
      ColumnRef<T1, R1> columnRef1, SqlOperator op, ColumnRef<T2, R2> columnRef2) {
    NameResolver.TableAndColumnName tableAndColumnName1 = NameResolver.resolve(columnRef1);
    NameResolver.TableAndColumnName tableAndColumnName2 = NameResolver.resolve(columnRef2);
    return new Condition(
        (qs, gen) -> {
          String sql =
              qs.quote(tableAndColumnName1.tableName())
                  + "."
                  + qs.quote(tableAndColumnName1.columnName())
                  + " "
                  + op.getSql()
                  + " "
                  + qs.quote(tableAndColumnName2.tableName())
                  + "."
                  + qs.quote(tableAndColumnName2.columnName());
          return new Resolved(sql, new HashMap<>());
        });
  }

  /**
   * Builds a {@code column OP (:param)} condition for collection values.
   *
   * @param columnRef the column reference (e.g., orders.status)
   * @param op the SQL operator to use (e.g., SqlOperator.IN for "column IN (:param)")
   * @param values the collection of values to compare to (e.g., List.of("PENDING", "SHIPPED"))
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column OP (:param)", where the column is
   *     compared to the provided collection of values using the specified operator. The parameter
   *     value is bound to the collection, allowing for filtering rows where a column's value
   *     matches any of the specified values (for IN) or does not match any of the specified values
   *     (for NOT IN).
   */
  private static <T, R> Condition columnCollectionCondition(
      ColumnRef<T, R> columnRef, SqlOperator op, Collection<?> values) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          String paramName = gen.next();
          String sql =
              qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName())
                  + " "
                  + op.getSql()
                  + " (:"
                  + paramName
                  + ")";
          return new Resolved(sql, Map.of(paramName, values));
        });
  }

  /**
   * Builds a {@code column OP (:param)} condition for collection values.
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param op the SQL operator to use (e.g., SqlOperator.IN for "expression IN (:param)")
   * @param values the collection of values to compare to (e.g., List.of(100, 200, 300))
   * @return a Condition representing the SQL condition "expression OP (:param)", where the
   *     expression is compared to the provided collection of values using the specified operator.
   *     The parameter value is bound to the collection, allowing for filtering rows where a
   *     computed expression's value matches any of the specified values (for IN) or does not match
   *     any of the specified values (for NOT IN).
   */
  private static Condition columnCollectionCondition(
      Expression expression, SqlOperator op, Collection<?> values) {
    return new Condition(
        (qs, gen) -> {
          String paramName = gen.next();
          Map<String, Object> params = new HashMap<>(expression.getParams());
          params.put(paramName, values);
          return new Resolved(
              expression.getSql() + " " + op.getSql() + " (:" + paramName + ")", params);
        });
  }

  /**
   * Builds a {@code column OP (subquery)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param op the SQL operator to use (e.g., SqlOperator.GT for "expression > (subquery)")
   * @param subquery the subquery to compare to (e.g., a Select representing "SELECT AVG(amount)
   *     FROM orders")
   * @return a Condition representing the SQL condition "expression OP (subquery)", where the
   *     expression is compared to the result of the subquery using the specified operator. This is
   *     used for conditions where a computed expression's value is compared to the result of a
   *     subquery using various operators, such as "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN",
   *     etc.
   */
  private static Condition columnSubqueryCondition(
      Expression expression, SqlOperator op, Select subquery) {
    return new Condition(
        (qs, gen) -> {
          Select.SqlAndParams sp = subquery.toSqlAndParams(gen);
          Map<String, Object> params = new HashMap<>(expression.getParams());
          params.putAll(sp.params());
          return new Resolved(
              expression.getSql() + " " + op.getSql() + " (" + sp.sql() + ")", params);
        });
  }

  /**
   * Builds a {@code column OP (:param)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param expression the raw SQL expression to compare (e.g., "orders.amount - orders.discount")
   * @param op the SQL operator to use (e.g., SqlOperator.GT for "expression > :param")
   * @param value the value to compare to (e.g., 100)
   * @return a Condition representing the SQL condition "expression OP :param", where the expression
   *     is compared to the provided value using the specified operator. The parameter value is
   *     bound to the provided value, allowing for filtering rows based on a computed expression's
   *     value compared to a specific value using various operators, such as "=", "!=", ">", "<",
   *     ">=", "<=", "LIKE", etc.
   */
  private static Condition columnValueCondition(
      Expression expression, SqlOperator op, Object value) {
    return new Condition(
        (qs, gen) -> {
          String paramName = gen.next();
          Map<String, Object> params = new HashMap<>(expression.getParams());
          params.put(paramName, value);
          return new Resolved(expression.getSql() + " " + op.getSql() + " :" + paramName, params);
        });
  }

  /**
   * Builds a {@code column OP (:param)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param aggregate the aggregate function to compare (e.g., new Aggregate("SUM", orders.amount))
   * @param op the SQL operator to use (e.g., SqlOperator.GT for "aggregate > :param")
   * @param value the value to compare to (e.g., 100)
   * @return a Condition representing the SQL condition "aggregate OP :param", where the aggregate
   *     function's result is compared to the provided value using the specified operator. The
   *     parameter value is bound to the provided value, allowing for filtering rows based on the
   *     result of an aggregate function compared to a specific value using various operators, such
   *     as "=", "!=", ">", "<", ">=", "<=", etc.
   */
  private static Condition columnValueCondition(Aggregate aggregate, SqlOperator op, Object value) {
    return new Condition(
        (qs, gen) -> {
          String paramName = gen.next();
          Aggregate.Resolved r = aggregate.resolve(qs, gen);
          String sql = r.sql() + " " + op.getSql() + " :" + paramName;
          Map<String, Object> params = new HashMap<>();
          params.put(paramName, value);
          params.putAll(r.params());
          return new Resolved(sql, params);
        });
  }

  /**
   * Builds a {@code OP (column)} condition, sharing the parent {@link ParamNameGenerator}.
   *
   * @param columnRef the column reference (e.g., customers.name)
   * @param op the SQL operator to use (e.g., SqlOperator.IS_NULL for "column IS NULL")
   * @param <T> The entity type
   * @param <R> The return type
   * @return a Condition representing the SQL condition "column OP", where the column is compared
   *     using the specified operator. This is used for conditions that involve a column and an
   *     operator without a value, such as "IS NULL" or "IS NOT NULL", where the condition checks
   *     for the presence or absence of a value in the column rather than comparing it to a specific
   *     value.
   */
  private static <T, R> Condition columnSubqueryCondition(
      ColumnRef<T, R> columnRef, SqlOperator op) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Condition(
        (qs, gen) -> {
          String sql =
              qs.quote(tableAndColumnName.tableName())
                  + "."
                  + qs.quote(tableAndColumnName.columnName())
                  + " "
                  + op.getSql();
          return new Resolved(sql, new HashMap<>());
        });
  }
}
