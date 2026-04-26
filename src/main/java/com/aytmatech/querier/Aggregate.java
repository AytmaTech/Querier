package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.HashMap;
import java.util.Map;

/** Represents SQL aggregate functions. */
public class Aggregate {
  private final AggregateFunction function;

  /** Column expression SQL (null for CaseWhen-based aggregates). */
  private final String columnExpression;

  private String alias;
  private final Map<String, Object> params;

  /** CaseWhen reference for deferred resolution (null for simple column aggregates). */
  private final CaseWhen caseWhen;

  private Aggregate(AggregateFunction function, String columnExpression) {
    this(function, columnExpression, new HashMap<>(), null);
  }

  private Aggregate(
      AggregateFunction function,
      String columnExpression,
      Map<String, Object> params,
      CaseWhen caseWhen) {
    this.function = function;
    this.columnExpression = columnExpression;
    this.params = params;
    this.caseWhen = caseWhen;
  }

  /** Holds a fully-resolved SQL fragment and its bound parameters. */
  record Resolved(String sql, Map<String, Object> params) {}

  /**
   * COUNT(column)
   *
   * @param columnRef a method reference to the column to count
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing COUNT(column)
   */
  public static <T, R> Aggregate count(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.COUNT,
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
  }

  /**
   * COUNT(*)
   *
   * @return An Aggregate representing COUNT(*)
   */
  public static Aggregate countAll() {
    return new Aggregate(AggregateFunction.COUNT, SqlValue.ALL.getSql());
  }

  /**
   * COUNT(DISTINCT column)
   *
   * @param columnRef a method reference to the column to count distinct values of
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing COUNT(DISTINCT column)
   */
  public static <T, R> Aggregate countDistinct(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.COUNT,
        SqlClause.DISTINCT.getSql()
            + " "
            + tableAndColumnName.tableName()
            + "."
            + tableAndColumnName.columnName());
  }

  /**
   * SUM(column)
   *
   * @param columnRef a method reference to the column to sum
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing SUM(column)
   */
  public static <T, R> Aggregate sum(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.SUM,
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
  }

  /**
   * AVG(column)
   *
   * @param columnRef a method reference to the column to average
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing AVG(column)
   */
  public static <T, R> Aggregate avg(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.AVG,
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
  }

  /**
   * MIN(column)
   *
   * @param columnRef a method reference to the column to find the minimum of
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing MIN(column)
   */
  public static <T, R> Aggregate min(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.MIN,
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
  }

  /**
   * MAX(column)
   *
   * @param columnRef a method reference to the column to find the maximum of
   * @param <T> The entity type
   * @param <R> The return type
   * @return An Aggregate representing MAX(column)
   */
  public static <T, R> Aggregate max(ColumnRef<T, R> columnRef) {
    NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
    return new Aggregate(
        AggregateFunction.MAX,
        tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
  }

  /**
   * SUM(CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate sum(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.SUM, null, new HashMap<>(), caseWhen);
  }

  /**
   * COUNT(CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate count(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.COUNT, null, new HashMap<>(), caseWhen);
  }

  /**
   * COUNT(DISTINCT CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate countDistinct(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.COUNT_DISTINCT, null, new HashMap<>(), caseWhen);
  }

  /**
   * AVG(CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate avg(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.AVG, null, new HashMap<>(), caseWhen);
  }

  /**
   * MIN(CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate min(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.MIN, null, new HashMap<>(), caseWhen);
  }

  /**
   * MAX(CASE WHEN ...)
   *
   * @param caseWhen a CaseWhen expression to use as the argument to the aggregate function
   * @return An Aggregate representing the aggregate function applied to the CaseWhen expression
   */
  public static Aggregate max(CaseWhen caseWhen) {
    return new Aggregate(AggregateFunction.MAX, null, new HashMap<>(), caseWhen);
  }

  /**
   * Sets an alias for this aggregate.
   *
   * @param alias the alias to set for this aggregate
   * @return this Aggregate instance for chaining
   */
  public Aggregate as(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Returns the SQL string for the aggregate function (e.g., "COUNT", "SUM", etc.) without any
   * arguments or parentheses. This method returns only the function name itself, as defined in the
   * {@link AggregateFunction} enum, and does not include any column expressions, DISTINCT keywords,
   * or aliases.
   *
   * @return the SQL string for the aggregate function (e.g., "COUNT", "SUM", etc.) without any
   *     arguments or parentheses.
   */
  public String getFunction() {
    return function.getSql();
  }

  /**
   * Returns the column expression for this aggregate. For simple column-based aggregates, this will
   * be the original column expression (e.g., "orders.total"). For CaseWhen-based aggregates, this
   * will return the SQL representation of the underlying CaseWhen expression without any alias,
   * since the CaseWhen expression itself determines the column expression in that context.
   *
   * @return the column expression for this aggregate, or the SQL of the underlying CaseWhen if this
   *     is a CaseWhen-based aggregate
   */
  public String getColumnExpression() {
    return caseWhen != null ? caseWhen.toSqlWithoutAlias() : columnExpression;
  }

  /**
   * Returns the alias for this aggregate, or null if no alias has been set. Note that for
   * CaseWhen-based aggregates, the alias is determined by the CaseWhen expression rather than the
   * Aggregate itself. In such cases, this method will return the alias of the underlying CaseWhen
   * expression if it exists, or null otherwise.
   *
   * @return the alias for this aggregate.
   */
  public String getAlias() {
    return alias;
  }

  /**
   * Returns the parameters for this aggregate. For simple column-based aggregates, this will return
   * the parameters directly associated with the Aggregate instance (which may be empty). For
   * CaseWhen-based aggregates, this will return the parameters from the underlying CaseWhen
   * expression, since any parameters needed for the aggregate would come from the CaseWhen
   * conditions and values.
   *
   * @return the parameters for this aggregate, either from the CaseWhen if this is a CaseWhen-based
   *     aggregate, or directly from this Aggregate instance otherwise.
   */
  public Map<String, Object> getParams() {
    return caseWhen != null ? caseWhen.getParams() : params;
  }

  /**
   * Resolves the aggregate SQL + params using the supplied generator so that parameter names are
   * unique within the enclosing query.
   */
  Resolved resolve(QuoteStrategy quoteStrategy, ParamNameGenerator gen) {
    if (caseWhen != null) {
      CaseWhen.Resolved cwResolved = caseWhen.resolveWithoutAlias(quoteStrategy, gen);
      String innerSql =
          function == AggregateFunction.COUNT_DISTINCT
              ? SqlClause.DISTINCT.getSql() + " " + cwResolved.sql()
              : cwResolved.sql();
      String sql = function.getSql() + "(" + innerSql + ")";
      if (alias != null) sql += " " + SqlClause.AS.getSql() + " " + alias;
      return new Resolved(sql, cwResolved.params());
    }
    return new Resolved(toSql(), params);
  }

  /**
   * Returns the SQL string for this aggregate, including the function name, column expression,
   * DISTINCT keyword if applicable, and alias if set. For simple column-based aggregates, this will
   * return a string like "COUNT(orders.total) AS total_count". For CaseWhen-based aggregates, this
   * will return a string like "SUM(CASE WHEN orders.status = 'shipped' THEN 1 ELSE 0 END) AS
   * shipped_count", where the inner CASE WHEN expression is included as the column expression. The
   * DISTINCT keyword will be included in the column expression for COUNT_DISTINCT aggregates.
   *
   * @return the SQL representation of this aggregate, including function name, column expression,
   *     DISTINCT keyword if applicable, and alias if set.
   */
  public String toSql() {
    String inner = buildColumnExpression();
    String sql = function.getSql() + "(" + inner + ")";
    if (alias != null) {
      sql += " " + SqlClause.AS.getSql() + " " + alias;
    }
    return sql;
  }

  /**
   * Returns SQL without alias for use in HAVING and ORDER BY clauses.
   *
   * @return the SQL representation of this aggregate without any alias
   */
  public String toSqlWithoutAlias() {
    String inner = buildColumnExpression();
    return function.getSql() + "(" + inner + ")";
  }

  private String buildColumnExpression() {
    if (caseWhen != null) {
      String cwSql = caseWhen.toSqlWithoutAlias();
      return function == AggregateFunction.COUNT_DISTINCT
          ? SqlClause.DISTINCT.getSql() + " " + cwSql
          : cwSql;
    }
    return columnExpression;
  }
}
