package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import java.util.*;
import java.util.regex.Pattern;

/** Main query builder for SELECT statements. */
public class Select {
  private static final Pattern PARAM_PATTERN = Pattern.compile(":(param[a-zA-Z0-9]+)");

  private final List<Object> selectItems;
  private final Class<?> fromTable;
  private final Expression fromExpression;
  private final List<JoinClause> joins;
  private final Condition whereCondition;
  private final List<ColumnRef<?, ?>> groupByColumnRefs;
  private final Condition havingCondition;
  private final List<OrderBy> orderByList;
  private final Integer limit;
  private final Integer offset;
  private final boolean distinct;
  private final List<CTE> ctes;
  private final SetOperation setOperation;
  private final QuoteStrategy quoteStrategy;

  /**
   * When non-null, {@link #toSqlAndParams()} short-circuits all builder state and returns this SQL
   * verbatim with an empty parameter map. Set via {@link #rawQuery(String)} or {@link
   * Builder#rawSql(String)}.
   *
   * <p>This is the bridge used by the AI SQL-first path: the AI produces standard SQL, we validate
   * it against the schema, then wrap it in a {@code Select} so the rest of the application can
   * treat it uniformly — call {@code toSqlAndParams()}, {@code toPositionalSql()}, {@code
   * toIndexedSql()}, or {@code toPlainSql()} exactly as with any other {@code Select}.
   */
  private final String rawSql;

  private Select(Builder builder) {
    this.selectItems = builder.selectItems;
    this.fromTable = builder.fromTable;
    this.fromExpression = builder.fromExpression;
    this.joins = builder.joins;
    this.whereCondition = builder.whereCondition;
    this.groupByColumnRefs = builder.groupByColumnRefs;
    this.havingCondition = builder.havingCondition;
    this.orderByList = builder.orderByList;
    this.limit = builder.limit;
    this.offset = builder.offset;
    this.distinct = builder.distinct;
    this.ctes = builder.ctes;
    this.setOperation = builder.setOperation;
    this.quoteStrategy = builder.quoteStrategy;
    this.rawSql = builder.rawSql;
  }

  /**
   * Creates a new builder.
   *
   * @return a new Builder instance for constructing a Select query
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a {@code Select} that wraps a raw SQL string verbatim.
   *
   * <p>Use this when you have a complete, validated SQL string (e.g., produced by an AI or an
   * external tool) and want to pass it through the standard {@code Select} execution path without
   * any builder-level transformation.
   *
   * <p>All output methods ({@link #toSqlAndParams()}, {@link #toPositionalSql()}, {@link
   * #toIndexedSql()}, {@link #toPlainSql()}) return the SQL as-is with an empty parameter map.
   * Named parameter placeholders in the form {@code :paramXXX} are still supported — they will be
   * recognised by {@link #toPositionalSql()} and {@link #toIndexedSql()}.
   *
   * <p><strong>Security note:</strong> the caller is responsible for ensuring the SQL has been
   * validated against the allowed schema before calling this method. Never pass untrusted user
   * input directly.
   *
   * <pre>{@code
   * // AI SQL-first path — SQL has already been validated against AiSchemaContext
   * Select select = Select.rawQuery(validatedSql);
   * List<Map<String, Object>> rows = jdbcRunner.run(select);
   * }</pre>
   *
   * @param sql a complete, validated SQL SELECT statement
   * @return a {@code Select} instance whose output methods return {@code sql} verbatim
   * @throws IllegalArgumentException if {@code sql} is null or blank
   */
  public static Select rawQuery(String sql) {
    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("rawQuery sql must not be null or blank");
    }
    return builder().rawSql(sql).build();
  }

  /**
   * Returns the raw SQL string if this {@code Select} was created via {@link #rawQuery(String)} or
   * {@link Builder#rawSql(String)}, or {@link Optional#empty()} otherwise.
   *
   * @return an {@code Optional} containing the raw SQL string if present, or empty if not
   */
  public Optional<String> getRawSql() {
    return Optional.ofNullable(rawSql);
  }

  /**
   * Converts this query to plain SQL with parameter values inlined.
   *
   * <p><strong>WARNING:</strong> This method is intended for <strong>logging, debugging, and
   * display purposes only</strong>. The generated SQL is <strong>NOT safe for direct
   * execution</strong> as it does not properly escape values and is vulnerable to SQL injection.
   * Always use {@link #toSqlAndParams()} for actual query execution.
   *
   * <p>This method replaces named parameter placeholders ({@code :paramXXX}) with their actual
   * literal values:
   *
   * <ul>
   *   <li>Strings/enums: wrapped in single quotes with proper escaping (e.g., {@code 'O''Brien'})
   *   <li>Numbers: inserted as-is without quotes
   *   <li>Booleans: inserted as {@code TRUE} or {@code FALSE}
   *   <li>null: inserted as {@code NULL}
   *   <li>Collections: expanded into comma-separated values (e.g., {@code 'A', 'B', 'C'} or {@code
   *       1, 2, 3})
   *   <li>Dates/temporal types: wrapped in single quotes, formatted as strings
   *   <li>Other objects: {@code toString()} called and wrapped in single quotes
   * </ul>
   *
   * @return a plain SQL string with all parameters inlined
   */
  public String toPlainSql() {
    SqlAndParams sqlAndParams = toSqlAndParams();
    String sql = sqlAndParams.sql();
    Map<String, Object> params = sqlAndParams.params();

    List<String> paramNames = new ArrayList<>(params.keySet());
    paramNames.sort((a, b) -> Integer.compare(b.length(), a.length()));

    for (String paramName : paramNames) {
      Object value = params.get(paramName);
      String replacement = formatValue(value);
      sql = sql.replace(":" + paramName, replacement);
    }

    return sql;
  }

  /**
   * Formats a parameter value for inlining into SQL. This method is NOT safe for production use -
   * only for debugging/logging.
   *
   * @param value the parameter value to format
   * @return the formatted value as a string suitable for inlining into SQL
   */
  private String formatValue(Object value) {
    if (value == null) {
      return SqlValue.NULL.getSql();
    }

    if (value instanceof Collection<?> collection) {
      List<String> formatted = new ArrayList<>();
      for (Object item : collection) {
        formatted.add(formatValue(item));
      }
      return String.join(", ", formatted);
    }

    if (value instanceof Number) {
      return value.toString();
    }

    if (value instanceof Boolean booleanValue) {
      return booleanValue ? SqlValue.TRUE.getSql() : SqlValue.FALSE.getSql();
    }

    if (value instanceof String stringValue) {
      return "'" + stringValue.replace("'", "''") + "'";
    }

    if (value instanceof Enum<?>) {
      return "'" + ((Enum<?>) value).name() + "'";
    }

    if (value instanceof java.time.temporal.Temporal
        || value instanceof java.util.Date
        || value instanceof java.sql.Date
        || value instanceof java.sql.Time
        || value instanceof java.sql.Timestamp) {
      return "'" + value.toString() + "'";
    }

    return "'" + value.toString().replace("'", "''") + "'";
  }

  /**
   * Converts this query to SQL and parameter map.
   *
   * <p>If this {@code Select} was created via {@link #rawQuery(String)} or {@link
   * Builder#rawSql(String)}, the raw SQL string is returned verbatim with an empty parameter map,
   * bypassing all builder state.
   *
   * @return SqlAndParams containing SQL with named (:param) placeholders and parameter map
   */
  public SqlAndParams toSqlAndParams() {
    if (rawSql != null) {
      return new SqlAndParams(rawSql, Map.of());
    }
    return toSqlAndParams(new ParamNameGenerator());
  }

  /**
   * Converts this query to SQL and parameter map using the supplied generator. Sharing the
   * generator across nested queries (CTEs, set operations, EXISTS subqueries) ensures all parameter
   * names are unique within the combined SQL.
   *
   * <p>If this {@code Select} was created via {@link #rawQuery(String)} or {@link
   * Builder#rawSql(String)}, the raw SQL string is returned verbatim with an empty parameter map
   * regardless of the supplied generator.
   *
   * @param paramGen the parameter name generator to use for generating unique parameter names
   *     across this query and any nested queries
   * @return SqlAndParams containing SQL with named (:param) placeholders and parameter map
   */
  SqlAndParams toSqlAndParams(ParamNameGenerator paramGen) {
    if (rawSql != null) {
      return new SqlAndParams(rawSql, Map.of());
    }

    StringBuilder sql = new StringBuilder();
    Map<String, Object> allParams = new HashMap<>();

    if (!ctes.isEmpty()) {
      sql.append(SqlClause.WITH.getSql()).append(" ");
      for (int i = 0; i < ctes.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        CTE cte = ctes.get(i);
        SqlAndParams cteQuerySqlAndParams = cte.query.toSqlAndParams(paramGen);
        sql.append(cte.name)
            .append(" ")
            .append(SqlClause.AS.getSql())
            .append(" (")
            .append(cteQuerySqlAndParams.sql())
            .append(")");
        allParams.putAll(cteQuerySqlAndParams.params());
      }
      sql.append(" ");
    }

    sql.append(SqlClause.SELECT.getSql()).append(" ");
    if (distinct) {
      sql.append(SqlClause.DISTINCT.getSql()).append(" ");
    }

    if (selectItems.isEmpty()) {
      sql.append(SqlValue.ALL.getSql());
    } else {
      List<String> selectSqls = new ArrayList<>();
      for (Object item : selectItems) {
        if (item instanceof ColumnRef<?, ?> columnRef) {
          NameResolver.TableAndColumnName tableAndColumnName =
              NameResolver.resolve(columnRef, quoteStrategy);
          selectSqls.add(tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
        } else if (item instanceof Aggregate agg) {
          Aggregate.Resolved r = agg.resolve(quoteStrategy, paramGen);
          selectSqls.add(r.sql());
          allParams.putAll(r.params());
        } else if (item instanceof Expression expr) {
          selectSqls.add(expr.toSql());
          allParams.putAll(expr.getParams());
        } else if (item instanceof WindowFunction wf) {
          selectSqls.add(wf.toSql());
        } else if (item instanceof CaseWhen caseWhen) {
          CaseWhen.Resolved r = caseWhen.resolve(quoteStrategy, paramGen);
          selectSqls.add(r.sql());
          allParams.putAll(r.params());
        }
      }
      sql.append(String.join(", ", selectSqls));
    }

    sql.append(" ").append(SqlClause.FROM.getSql()).append(" ");
    if (fromTable != null) {
      sql.append(NameResolver.getTableName(fromTable, quoteStrategy));
    } else if (fromExpression != null) {
      sql.append(fromExpression.getSql());
      allParams.putAll(fromExpression.getParams());
    }

    for (JoinClause join : joins) {
      sql.append(" ").append(join.joinType.getSql()).append(" ");
      if (join.joinTable != null) {
        sql.append(NameResolver.getTableName(join.joinTable, quoteStrategy));
      } else if (join.joinExpression != null) {
        sql.append(join.joinExpression.getSql());
        allParams.putAll(join.joinExpression.getParams());
      }
      if (join.condition != null) {
        Condition.Resolved cr = join.condition.resolve(quoteStrategy, paramGen);
        sql.append(" ").append(SqlClause.ON.getSql()).append(" ").append(cr.sql());
        allParams.putAll(cr.params());
      }
    }

    if (whereCondition != null) {
      Condition.Resolved cr = whereCondition.resolve(quoteStrategy, paramGen);
      sql.append(" ").append(SqlClause.WHERE.getSql()).append(" ").append(cr.sql());
      allParams.putAll(cr.params());
    }

    if (!groupByColumnRefs.isEmpty()) {
      List<String> groupByColumns = new ArrayList<>();
      for (ColumnRef<?, ?> columnRef : groupByColumnRefs) {
        NameResolver.TableAndColumnName tableAndColumnName =
            NameResolver.resolve(columnRef, quoteStrategy);
        groupByColumns.add(tableAndColumnName.tableName() + "." + tableAndColumnName.columnName());
      }
      sql.append(" ")
          .append(SqlClause.GROUP_BY.getSql())
          .append(" ")
          .append(String.join(", ", groupByColumns));
    }

    if (havingCondition != null) {
      Condition.Resolved cr = havingCondition.resolve(quoteStrategy, paramGen);
      sql.append(" ").append(SqlClause.HAVING.getSql()).append(" ").append(cr.sql());
      allParams.putAll(cr.params());
    }

    if (!orderByList.isEmpty()) {
      List<String> orderBySqls = new ArrayList<>();
      for (OrderBy ob : orderByList) {
        OrderBy.Resolved r = ob.resolve(quoteStrategy, paramGen);
        orderBySqls.add(r.sql());
        allParams.putAll(r.params());
      }
      sql.append(" ")
          .append(SqlClause.ORDER_BY.getSql())
          .append(" ")
          .append(String.join(", ", orderBySqls));
    }

    if (limit != null) {
      sql.append(" ").append(SqlClause.LIMIT.getSql()).append(" ").append(limit);
    }

    if (offset != null) {
      sql.append(" ").append(SqlClause.OFFSET.getSql()).append(" ").append(offset);
    }

    if (setOperation != null) {
      sql.append(" ").append(setOperation.operation.getSql()).append(" ");
      SqlAndParams otherSql = setOperation.other.toSqlAndParams(paramGen);
      sql.append(otherSql.sql());
      allParams.putAll(otherSql.params());
    }

    return new SqlAndParams(sql.toString(), allParams);
  }

  /**
   * Converts this query to SQL with positional (?) placeholders and an ordered list of parameter
   * values. Compatible with plain JDBC PreparedStatement, Spring JdbcTemplate, and jOOQ.
   *
   * @return PositionalSqlAndParams containing SQL with ? placeholders and ordered parameter list
   */
  public PositionalSqlAndParams toPositionalSql() {
    SqlAndParams sqlAndParams = toSqlAndParams();
    String sql = sqlAndParams.sql();
    Map<String, Object> params = sqlAndParams.params();

    List<String> paramOrder = new ArrayList<>();
    java.util.regex.Matcher matcher = PARAM_PATTERN.matcher(sql);
    while (matcher.find()) {
      paramOrder.add(matcher.group(1));
    }

    String positionalSql = PARAM_PATTERN.matcher(sql).replaceAll("?");

    List<Object> orderedParams = new ArrayList<>();
    for (String paramName : paramOrder) {
      orderedParams.add(params.get(paramName));
    }

    return new PositionalSqlAndParams(positionalSql, orderedParams);
  }

  /**
   * Converts this query to SQL with indexed ($1, $2, ...) placeholders and an ordered list of
   * parameter values. Compatible with Vert.x SQL Client and PostgreSQL native drivers.
   *
   * @return IndexedSqlAndParams containing SQL with $N placeholders and ordered parameter list
   */
  public IndexedSqlAndParams toIndexedSql() {
    SqlAndParams sqlAndParams = toSqlAndParams();
    String sql = sqlAndParams.sql();
    Map<String, Object> params = sqlAndParams.params();

    List<String> paramOrder = new ArrayList<>();
    java.util.regex.Matcher matcher = PARAM_PATTERN.matcher(sql);
    while (matcher.find()) {
      paramOrder.add(matcher.group(1));
    }

    StringBuilder indexedSql = new StringBuilder();
    matcher.reset();
    int index = 1;
    int lastEnd = 0;
    while (matcher.find()) {
      indexedSql.append(sql, lastEnd, matcher.start());
      indexedSql.append("$").append(index++);
      lastEnd = matcher.end();
    }
    indexedSql.append(sql.substring(lastEnd));

    List<Object> orderedParams = new ArrayList<>();
    for (String paramName : paramOrder) {
      orderedParams.add(params.get(paramName));
    }

    return new IndexedSqlAndParams(indexedSql.toString(), orderedParams);
  }

  /**
   * Record representing SQL query and parameters.
   *
   * @param sql the SQL with named (:param) placeholders
   * @param params parameter map
   */
  public record SqlAndParams(String sql, Map<String, Object> params) {
    /**
     * Creates a SqlAndParams record, ensuring the parameter map is immutable. The SQL string should
     * contain named parameter placeholders (e.g., :paramName) that correspond to the keys in the
     * parameter map.
     *
     * @param sql the SQL with named (:param) placeholders
     * @param params the map of parameter names to their bound values, where keys correspond to the
     *     named placeholders in the SQL string
     */
    public SqlAndParams {
      params = Map.copyOf(params);
    }

    /**
     * Gets the parameter map for this query. The returned map is unmodifiable and contains the
     * parameter names and their corresponding values that should be bound to the named placeholders
     * in the SQL string.
     *
     * @return an unmodifiable map of parameter names to their bound values for this query, where
     *     keys correspond to the named placeholders in the SQL string
     */
    @Override
    public Map<String, Object> params() {
      return Map.copyOf(params);
    }
  }

  /**
   * Record containing SQL with positional (?) placeholders and ordered parameter list. Compatible
   * with plain JDBC PreparedStatement, Spring JdbcTemplate, and jOOQ.
   *
   * @param sql the SQL with positional (?) placeholders
   * @param params ordered parameter list
   */
  public record PositionalSqlAndParams(String sql, List<Object> params) {
    /**
     * Creates a PositionalSqlAndParams record, ensuring the parameter list is immutable. The SQL
     * string should contain positional placeholders (?) corresponding to the order of parameters in
     * the list.
     *
     * @param sql the SQL with positional (?) placeholders
     * @param params the ordered list of parameter values corresponding to the positional
     *     placeholders in the SQL
     */
    public PositionalSqlAndParams {
      params = List.copyOf(params);
    }

    /**
     * Gets the ordered list of parameter values for this query. The returned list is unmodifiable
     * and corresponds to the positional placeholders (?) in the SQL string.
     *
     * @return an unmodifiable list of parameter values in the order they appear in the SQL string
     *     for positional placeholders
     */
    @Override
    public List<Object> params() {
      return List.copyOf(params);
    }
  }

  /**
   * Record containing SQL with indexed ($1, $2, ...) placeholders and ordered parameter list.
   * Compatible with Vert.x SQL Client and PostgreSQL native drivers.
   *
   * @param sql the SQL with indexed ($1, $2, ...) placeholders
   * @param params ordered parameter list
   */
  public record IndexedSqlAndParams(String sql, List<Object> params) {
    /**
     * Creates an IndexedSqlAndParams record, ensuring the parameter list is immutable. The SQL
     * string should contain indexed placeholders ($1, $2, ...) corresponding to the order of
     * parameters in the list.
     *
     * @param sql the SQL with indexed ($1, $2, ...) placeholders
     * @param params the ordered list of parameter values corresponding to the indexed placeholders
     *     in the SQL
     */
    public IndexedSqlAndParams {
      params = List.copyOf(params);
    }

    /**
     * Gets the ordered list of parameter values for this query. The returned list is unmodifiable
     * and corresponds to the indexed placeholders ($1, $2, ...) in the SQL string.
     *
     * @return an unmodifiable list of parameter values in the order they appear in the SQL string
     *     for indexed placeholders
     */
    @Override
    public List<Object> params() {
      return List.copyOf(params);
    }
  }

  /** Builder for Select queries. */
  public static class Builder {
    private final List<Object> selectItems = new ArrayList<>();
    private Class<?> fromTable;
    private Expression fromExpression;
    private final List<JoinClause> joins = new ArrayList<>();
    private Condition whereCondition;
    private final List<ColumnRef<?, ?>> groupByColumnRefs = new ArrayList<>();
    private Condition havingCondition;
    private final List<OrderBy> orderByList = new ArrayList<>();
    private Integer limit;
    private Integer offset;
    private boolean distinct = false;
    private final List<CTE> ctes = new ArrayList<>();
    private SetOperation setOperation;
    private QuoteStrategy quoteStrategy = QuoteStrategy.NONE;
    private String rawSql;

    /**
     * Adds a SELECT column reference.
     *
     * @param columnRef the column reference to select
     * @param <T> The entity type
     * @param <R> The return type
     * @return this builder for chaining
     */
    public <T, R> Builder select(ColumnRef<T, R> columnRef) {
      selectItems.add(columnRef);
      return this;
    }

    /**
     * Adds a SELECT aggregate.
     *
     * @param aggregate the aggregate to select
     * @return this builder for chaining
     */
    public Builder select(Aggregate aggregate) {
      selectItems.add(aggregate);
      return this;
    }

    /**
     * Adds a SELECT expression.
     *
     * @param expression the expression to select, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder select(Expression expression) {
      selectItems.add(expression);
      return this;
    }

    /**
     * Adds a SELECT window function.
     *
     * @param windowFunction the window function to select
     * @return this builder for chaining
     */
    public Builder select(WindowFunction windowFunction) {
      selectItems.add(windowFunction);
      return this;
    }

    /**
     * Adds a SELECT CASE WHEN expression.
     *
     * @param caseWhen the CASE WHEN expression to select, which may contain parameter placeholders
     *     (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder select(CaseWhen caseWhen) {
      selectItems.add(caseWhen);
      return this;
    }

    /**
     * Sets the FROM table.
     *
     * @param table the table class to select from
     * @return this builder for chaining
     */
    public Builder from(Class<?> table) {
      this.fromTable = table;
      return this;
    }

    /**
     * Sets the FROM expression (e.g., for CTEs or subqueries).
     *
     * @param expression the expression to select from, which may contain parameter placeholders
     *     (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder from(Expression expression) {
      this.fromExpression = expression;
      return this;
    }

    /**
     * Sets a raw SQL string that completely overrides all builder state when {@link
     * Select#toSqlAndParams()} is called.
     *
     * <p>Use this when you have a complete, validated SQL string and still want to record a primary
     * entity class via {@link #from(Class)} for result-mapping purposes (e.g., the ORM knows which
     * type to map rows to), while returning the raw SQL for actual execution.
     *
     * <p>When {@code rawSql} is set, {@link Select#toSqlAndParams()} returns the raw SQL verbatim
     * with an empty parameter map, ignoring all other builder state (select items, joins, where,
     * etc.). All other output methods ({@link Select#toPositionalSql()}, {@link
     * Select#toIndexedSql()}, {@link Select#toPlainSql()}) behave accordingly.
     *
     * <p>Prefer {@link Select#rawQuery(String)} when you do not need to record an entity class.
     *
     * <p><strong>Security note:</strong> the caller is responsible for ensuring the SQL has been
     * validated against the allowed schema. Never pass untrusted user input directly.
     *
     * <pre>{@code
     * // AI SQL-first path: entity class recorded for result mapping, raw SQL used for execution
     * Select select = Select.builder()
     *     .from(Customer.class)   // tells the ORM which type to map rows to
     *     .rawSql(validatedSql)   // actual SQL sent to the database
     *     .build();
     * }</pre>
     *
     * @param sql a complete, validated SQL SELECT statement; must not be null or blank
     * @return this builder for chaining
     * @throws IllegalArgumentException if {@code sql} is null or blank
     */
    public Builder rawSql(String sql) {
      if (sql == null || sql.isBlank()) {
        throw new IllegalArgumentException("rawSql must not be null or blank");
      }
      this.rawSql = sql;
      return this;
    }

    /**
     * Adds an INNER JOIN.
     *
     * @param table the table class to join
     * @param condition the join condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder join(Class<?> table, Condition condition) {
      joins.add(new JoinClause(JoinType.INNER, table, null, condition));
      return this;
    }

    /**
     * Adds a LEFT JOIN.
     *
     * @param table the table class to join
     * @param condition the join condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder leftJoin(Class<?> table, Condition condition) {
      joins.add(new JoinClause(JoinType.LEFT, table, null, condition));
      return this;
    }

    /**
     * Adds a RIGHT JOIN.
     *
     * @param table the table class to join
     * @param condition the join condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder rightJoin(Class<?> table, Condition condition) {
      joins.add(new JoinClause(JoinType.RIGHT, table, null, condition));
      return this;
    }

    /**
     * Adds a FULL OUTER JOIN.
     *
     * @param table the table class to join
     * @param condition the join condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder fullJoin(Class<?> table, Condition condition) {
      joins.add(new JoinClause(JoinType.FULL, table, null, condition));
      return this;
    }

    /**
     * Adds a CROSS JOIN.
     *
     * @param table the table class to join
     * @return this builder for chaining
     */
    public Builder crossJoin(Class<?> table) {
      joins.add(new JoinClause(JoinType.CROSS, table, null, null));
      return this;
    }

    /**
     * Sets the WHERE condition.
     *
     * @param condition the WHERE condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder where(Condition condition) {
      this.whereCondition = condition;
      return this;
    }

    /**
     * Adds GROUP BY columns.
     *
     * @param columnRefs the column references to group by
     * @param <T> The entity type
     * @param <R> The return type
     * @return this builder for chaining
     */
    @SafeVarargs
    public final <T, R> Builder groupBy(ColumnRef<T, R>... columnRefs) {
      groupByColumnRefs.addAll(Arrays.asList(columnRefs));
      return this;
    }

    /**
     * Sets the HAVING condition.
     *
     * @param condition the HAVING condition, which may contain parameter placeholders (e.g.,
     *     ":paramName")
     * @return this builder for chaining
     */
    public Builder having(Condition condition) {
      this.havingCondition = condition;
      return this;
    }

    /**
     * Adds ORDER BY clauses.
     *
     * @param orderBys the ORDER BY clauses to add
     * @return this builder for chaining
     */
    public Builder orderBy(OrderBy... orderBys) {
      orderByList.addAll(Arrays.asList(orderBys));
      return this;
    }

    /**
     * Sets the LIMIT.
     *
     * @param limit the maximum number of rows to return
     * @return this builder for chaining
     */
    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets the OFFSET.
     *
     * @param offset the number of rows to skip before starting to return rows
     * @return this builder for chaining
     */
    public Builder offset(int offset) {
      this.offset = offset;
      return this;
    }

    /**
     * Enables DISTINCT.
     *
     * @return this builder for chaining
     */
    public Builder distinct() {
      this.distinct = true;
      return this;
    }

    /**
     * Adds a CTE (WITH clause).
     *
     * @param name the name of the CTE to reference in the main query
     * @param query the Select query that defines the CTE, which may contain parameter placeholders
     *     (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder with(String name, Select query) {
      ctes.add(new CTE(name, query));
      return this;
    }

    /**
     * Adds a UNION.
     *
     * @param other the Select query to combine with this query using UNION, which may contain
     *     parameter placeholders (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder union(Select other) {
      this.setOperation = new SetOperation(SetOperationType.UNION, other);
      return this;
    }

    /**
     * Adds a UNION ALL.
     *
     * @param other the Select query to combine with this query using UNION ALL, which may contain
     *     parameter placeholders (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder unionAll(Select other) {
      this.setOperation = new SetOperation(SetOperationType.UNION_ALL, other);
      return this;
    }

    /**
     * Adds an INTERSECT.
     *
     * @param other the Select query to combine with this query using INTERSECT, which may contain
     *     parameter placeholders (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder intersect(Select other) {
      this.setOperation = new SetOperation(SetOperationType.INTERSECT, other);
      return this;
    }

    /**
     * Adds an EXCEPT.
     *
     * @param other the Select query to combine with this query using EXCEPT, which may contain
     *     parameter placeholders (e.g., ":paramName")
     * @return this builder for chaining
     */
    public Builder except(Select other) {
      this.setOperation = new SetOperation(SetOperationType.EXCEPT, other);
      return this;
    }

    /**
     * Sets the quote strategy for table and column names. Default is NONE for backward
     * compatibility.
     *
     * @param quoteStrategy the quote strategy to use
     * @return this builder
     */
    public Builder quoteStrategy(QuoteStrategy quoteStrategy) {
      this.quoteStrategy = quoteStrategy;
      return this;
    }

    /**
     * Builds the Select query.
     *
     * @return a Select instance representing the constructed query
     */
    public Select build() {
      return new Select(this);
    }
  }

  /**
   * Record representing a JOIN clause in the FROM part of the query. It can represent different
   * types of joins (INNER, LEFT, RIGHT, FULL, CROSS) and can join either a table (specified by a
   * Class) or an arbitrary expression (e.g., for CTEs or subqueries). The join condition is
   * optional for CROSS JOINs.
   *
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, CROSS)
   * @param joinTable the table class to join, or null if joining an expression
   * @param joinExpression the expression to join, or null if joining a table class
   * @param condition the join condition, which may contain parameter placeholders (e.g.,
   *     ":paramName"), or null if no condition (e.g., for CROSS JOIN)
   */
  private record JoinClause(
      JoinType joinType, Class<?> joinTable, Expression joinExpression, Condition condition) {}

  /**
   * Record representing a Common Table Expression (CTE) defined in a WITH clause. Each CTE has a
   * name that can be referenced in the main query's FROM clause or elsewhere, and a Select query
   * that defines the CTE's result set. The CTE query may contain parameter placeholders (e.g.,
   * ":paramName") which will be included in the overall parameter map when generating SQL.
   *
   * @param name the name of the CTE to reference in the main query
   * @param query the Select query that defines the CTE, which may contain parameter placeholders
   *     (e.g., ":paramName")
   */
  private record CTE(String name, Select query) {}

  /**
   * Record representing a set operation (UNION, UNION ALL, INTERSECT, EXCEPT) that combines this
   * Select query with another Select query. The operation type specifies which set operation to
   * use, and the other query is the Select query to combine with this one. Both queries may contain
   * parameter placeholders (e.g., ":paramName") which will be included in the overall parameter map
   * when generating SQL.
   *
   * @param operation the type of set operation (UNION, UNION ALL, INTERSECT, EXCEPT)
   * @param other the Select query to combine with this query using the specified set operation,
   *     which may contain parameter placeholders (e.g., ":paramName")
   */
  private record SetOperation(SetOperationType operation, Select other) {}
}
