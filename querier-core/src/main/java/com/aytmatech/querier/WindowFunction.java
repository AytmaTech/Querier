package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.*;

/** Represents a window function for analytics queries. */
public class WindowFunction {
  private final Aggregate aggregate;
  private final List<String> partitionByColumns;
  private final List<OrderBy> orderByList;
  private String alias;

  private WindowFunction(Aggregate aggregate) {
    this.aggregate = aggregate;
    this.partitionByColumns = new ArrayList<>();
    this.orderByList = new ArrayList<>();
  }

  /**
   * Creates a window function from an aggregate.
   *
   * @param aggregate The aggregate to use as the basis for this window function
   * @return A new WindowFunction instance based on the provided aggregate
   */
  public static WindowFunction of(Aggregate aggregate) {
    return new WindowFunction(aggregate);
  }

  /**
   * Adds PARTITION BY columns.
   *
   * @param columnRefs One or more ColumnRef instances representing the columns to partition by
   * @param <T> The entity type
   * @param <R> The return type
   * @return This WindowFunction instance with the specified PARTITION BY columns added
   */
  @SafeVarargs
  public final <T, R> WindowFunction partitionBy(ColumnRef<T, R>... columnRefs) {
    for (ColumnRef<T, R> columnRef : columnRefs) {
      NameResolver.TableAndColumnName tableAndColumnName = NameResolver.resolve(columnRef);
      partitionByColumns.add(
          tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
    }
    return this;
  }

  /**
   * Adds ORDER BY clauses.
   *
   * @param orderBys One or more OrderBy clauses to add to this window function
   * @return This WindowFunction instance with the specified ORDER BY clauses added
   */
  public WindowFunction orderBy(OrderBy... orderBys) {
    orderByList.addAll(Arrays.asList(orderBys));
    return this;
  }

  /**
   * Sets an alias for this window function.
   *
   * @param alias The alias to set for this window function
   * @return This WindowFunction instance with the specified alias set
   */
  public WindowFunction as(String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Gets the alias for this window function, or null if no alias is set.
   *
   * @return The alias for this window function, or null if no alias is set
   */
  public String getAlias() {
    return alias;
  }

  /** Package-private: resolves this window function with the shared param generator. */
  record Resolved(String sql, Map<String, Object> params) {}

  Resolved resolve(QuoteStrategy strategy, ParamNameGenerator gen) {
    Map<String, Object> allParams = new LinkedHashMap<>();

    String funcSql = "";
    if (aggregate != null) {
      Aggregate.Resolved aggResolved = aggregate.resolveWithoutAlias(strategy, gen);
      funcSql = aggResolved.sql();
      allParams.putAll(aggResolved.params());
    }

    StringBuilder sql = new StringBuilder(funcSql);
    sql.append(" ").append(SqlClause.OVER.getSql()).append(" (");

    if (!partitionByColumns.isEmpty()) {
      sql.append(SqlClause.PARTITION_BY.getSql()).append(" ");
      List<String> quotedCols =
          partitionByColumns.stream().map(c -> quoteColumn(c, strategy)).toList();
      sql.append(String.join(", ", quotedCols));
    }

    if (!orderByList.isEmpty()) {
      if (!partitionByColumns.isEmpty()) sql.append(" ");
      sql.append(SqlClause.ORDER_BY.getSql()).append(" ");
      List<String> orderSqls = new ArrayList<>();
      for (OrderBy ob : orderByList) {
        OrderBy.Resolved obr = ob.resolve(strategy, gen);
        orderSqls.add(obr.sql());
        allParams.putAll(obr.params());
      }
      sql.append(String.join(", ", orderSqls));
    }

    sql.append(")");
    if (alias != null) {
      sql.append(" ").append(SqlClause.AS.getSql()).append(" ").append(alias);
    }

    return new Resolved(sql.toString(), Collections.unmodifiableMap(allParams));
  }

  private static String quoteColumn(String col, QuoteStrategy strategy) {
    return strategy.quoteQualified(col);
  }

  /**
   * Renders this window function as a SQL string (e.g., "SUM(sales) OVER (PARTITION BY region ORDER
   * BY date DESC) AS total_sales"). The alias is included if it is set.
   *
   * @return The SQL string representation of this window function
   */
  public String toSql() {
    StringBuilder sql = new StringBuilder();
    if (aggregate != null) {
      sql.append(aggregate.toSqlWithoutAlias());
    }
    sql.append(" ").append(SqlClause.OVER.getSql()).append(" (");

    if (!partitionByColumns.isEmpty()) {
      sql.append(SqlClause.PARTITION_BY.getSql()).append(" ");
      sql.append(String.join(", ", partitionByColumns));
    }

    if (!orderByList.isEmpty()) {
      if (!partitionByColumns.isEmpty()) {
        sql.append(" ");
      }
      sql.append(SqlClause.ORDER_BY.getSql()).append(" ");
      sql.append(String.join(", ", orderByList.stream().map(OrderBy::toSql).toList()));
    }

    sql.append(")");

    if (alias != null) {
      sql.append(" ").append(SqlClause.AS.getSql()).append(" ").append(alias);
    }

    return sql.toString();
  }
}
