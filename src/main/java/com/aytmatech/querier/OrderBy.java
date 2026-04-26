package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Represents an ORDER BY clause item. */
public class OrderBy {
  private static final Pattern TABLE_COLUMN_PATTERN =
      Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b");

  private final String columnExpression;
  private final SortDirection direction;
  private final Map<String, Object> params;

  /** CaseWhen reference for deferred resolution (null for simple column/expression order bys). */
  private final CaseWhen caseWhen;

  private OrderBy(String columnExpression, SortDirection direction) {
    this(columnExpression, direction, new HashMap<>(), null);
  }

  private OrderBy(
      String columnExpression,
      SortDirection direction,
      Map<String, Object> params,
      CaseWhen caseWhen) {
    this.columnExpression = columnExpression;
    this.direction = direction;
    this.params = params;
    this.caseWhen = caseWhen;
  }

  /**
   * Holds a fully-resolved SQL fragment and its bound parameters.
   *
   * @param sql The fully-resolved SQL fragment for this ORDER BY item, with parameter placeholders
   *     (e.g., ":paramName") if parameters are present
   * @param params The map of parameter names to values that should be bound when executing the
   *     query, with parameter names matching the placeholders in the SQL fragment
   */
  record Resolved(String sql, Map<String, Object> params) {}

  /**
   * Creates an ascending ORDER BY clause.
   *
   * @param columnRef A method reference to a getter method for the column to order by (e.g.,
   *     MyEntity::getMyColumn). The column name will be resolved from the method reference, and the
   *     table name will be resolved from the declaring class of the method reference.
   * @param <T> the entity type
   * @param <R> the return type
   * @return An OrderBy instance representing an ascending ORDER BY clause for the specified column
   *     reference
   */
  public static <T, R> OrderBy asc(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new OrderBy(
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName(), SortDirection.ASC);
  }

  /**
   * Creates a descending ORDER BY clause.
   *
   * @param columnRef A method reference to a getter method for the column to order by (e.g.,
   *     MyEntity::getMyColumn). The column name will be resolved from the method reference, and the
   *     table name will be resolved from the declaring class of the method reference.
   * @param <T> the entity type
   * @param <R> the return type
   * @return An OrderBy instance representing a descending ORDER BY clause for the specified column
   *     reference
   */
  public static <T, R> OrderBy desc(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new OrderBy(
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName(), SortDirection.DESC);
  }

  /**
   * Creates an ascending ORDER BY clause from a raw expression.
   *
   * @param expression The raw SQL expression to order by, which may contain parameter placeholders
   *     (e.g., ":paramName") if parameters are bound to it. This expression will be used directly
   *     in the ORDER BY clause, with the specified sort direction.
   * @return An OrderBy instance representing an ascending ORDER BY clause for the specified raw
   *     expression
   */
  public static OrderBy asc(String expression) {
    return new OrderBy(expression, SortDirection.ASC);
  }

  /**
   * Creates a descending ORDER BY clause from a raw expression.
   *
   * @param expression The raw SQL expression to order by, which may contain parameter placeholders
   *     (e.g., ":paramName") if parameters are bound to it. This expression will be used directly
   *     in the ORDER BY clause, with the specified sort direction.
   * @return An OrderBy instance representing a descending ORDER BY clause for the specified raw
   *     expression
   */
  public static OrderBy desc(String expression) {
    return new OrderBy(expression, SortDirection.DESC);
  }

  /**
   * Creates an ascending ORDER BY clause from a CaseWhen expression.
   *
   * @param caseWhen The CaseWhen expression to order by. This allows for complex conditional
   *     ordering logic to be encapsulated in a CaseWhen expression and used directly in the ORDER
   *     BY clause.
   * @return An OrderBy instance representing an ascending ORDER BY clause for the specified
   *     CaseWhen
   */
  public static OrderBy asc(CaseWhen caseWhen) {
    return new OrderBy(null, SortDirection.ASC, new HashMap<>(), caseWhen);
  }

  /**
   * Creates a descending ORDER BY clause from a CaseWhen expression.
   *
   * @param caseWhen The CaseWhen expression to order by. This allows for complex conditional
   *     ordering logic to be encapsulated in a CaseWhen expression and used directly in the ORDER
   *     BY clause.
   * @return An OrderBy instance representing a descending ORDER BY clause for the specified
   *     CaseWhen
   */
  public static OrderBy desc(CaseWhen caseWhen) {
    return new OrderBy(null, SortDirection.DESC, new HashMap<>(), caseWhen);
  }

  /**
   * Resolves the ORDER BY SQL + params using the supplied generator so that parameter names are
   * unique within the enclosing query.
   *
   * @param quoteStrategy the quote strategy to apply when generating the SQL fragment for this
   *     ORDER BY item
   * @param gen the parameter name generator to use for generating unique parameter names for any
   *     parameters bound to this ORDER BY item
   * @return a Resolved object containing the fully-resolved SQL fragment for this ORDER BY item,
   *     with parameter placeholders, and the map of parameter names to values that should be bound
   *     when executing the query
   */
  Resolved resolve(QuoteStrategy quoteStrategy, ParamNameGenerator gen) {
    if (caseWhen != null) {
      CaseWhen.Resolved cwResolved = caseWhen.resolveWithoutAlias(quoteStrategy, gen);
      return new Resolved(cwResolved.sql() + " " + direction.getSql(), cwResolved.params());
    }
    return new Resolved(toSql(quoteStrategy), params);
  }

  /**
   * Gets the parameters bound to this ORDER BY item. If this is a simple column/expression order
   * by, this will return the params map directly on this OrderBy instance. If this is a
   * CaseWhen-based order by, this will return the params from the associated CaseWhen instance
   * instead, since any parameters for a CaseWhen-based order by would be defined on the CaseWhen
   * expression rather than directly on the OrderBy.
   *
   * @return a map of parameter names to their bound values for this ORDER BY item. For simple
   *     column/expression order bys, this will be the params map on this OrderBy instance. For
   *     CaseWhen-based order bys, this will be the params from the associated CaseWhen instance.
   */
  public Map<String, Object> getParams() {
    return caseWhen != null ? caseWhen.getParams() : params;
  }

  /**
   * Generates the SQL for this ORDER BY item without applying any quoting to identifiers. This is
   * provided as a convenience method for cases where the caller knows that quoting is not necessary
   * (e.g., when using simple column names that do not conflict with reserved words). For more
   * control over quoting, use the toSql(QuoteStrategy) method instead.
   *
   * @return the SQL for this ORDER BY item without any quoting applied to identifiers
   */
  public String toSql() {
    return toSql(QuoteStrategy.NONE);
  }

  /**
   * Generates SQL with the specified quote strategy applied.
   *
   * @param quoteStrategy the quote strategy to apply
   * @return the SQL with quoted identifiers
   */
  public String toSql(QuoteStrategy quoteStrategy) {
    if (caseWhen != null) {
      return caseWhen.toSqlWithoutAlias(quoteStrategy) + " " + direction.getSql();
    }

    String quotedExpression = columnExpression;

    if (quoteStrategy != QuoteStrategy.NONE) {
      Matcher matcher = TABLE_COLUMN_PATTERN.matcher(columnExpression);
      StringBuilder result = new StringBuilder();
      int lastEnd = 0;

      while (matcher.find()) {
        result.append(columnExpression, lastEnd, matcher.start());
        String tableName = matcher.group(1);
        String columnName = matcher.group(2);
        result
            .append(quoteStrategy.quote(tableName))
            .append(".")
            .append(quoteStrategy.quote(columnName));
        lastEnd = matcher.end();
      }
      result.append(columnExpression.substring(lastEnd));
      quotedExpression = result.toString();
    }

    return quotedExpression + " " + direction.getSql();
  }
}
