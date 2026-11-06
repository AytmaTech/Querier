package com.aytmatech.querier;

import com.aytmatech.querier.util.NameResolver;
import com.aytmatech.querier.util.ParamNameGenerator;
import com.aytmatech.querier.vendor.MsSqlDialectFunctions;
import com.aytmatech.querier.vendor.MySqlDialectFunctions;
import com.aytmatech.querier.vendor.OracleDialectFunctions;
import com.aytmatech.querier.vendor.PgDialectFunctions;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a vendor-specific (or standard) SQL function call with type-safe arguments.
 *
 * <p>Create instances via the vendor factory classes: {@link PgDialectFunctions}, {@link
 * MySqlDialectFunctions}, {@link MsSqlDialectFunctions}, {@link OracleDialectFunctions}.
 *
 * <p>Or build a custom function directly:
 *
 * <pre>{@code
 * DbFunction.of("MY_FUNC", DbFunction.col(Order::getTotal), DbFunction.val(100))
 * }</pre>
 *
 * <p>A {@code DbFunction} can appear in:
 *
 * <ul>
 *   <li>SELECT lists via {@code Select.Builder.select(DbFunction)}
 *   <li>WHERE conditions via {@code Condition.eq(DbFunction, value)}, etc.
 *   <li>ORDER BY clauses via {@code OrderBy.asc(DbFunction)} / {@code OrderBy.desc(DbFunction)}
 *   <li>Window function frames via {@code WindowFunction.of(DbFunction)}
 * </ul>
 *
 * <p>All {@code ColumnRef} arguments are resolved eagerly (table and column names extracted at
 * construction time); quoting via {@link QuoteStrategy} is applied lazily at SQL-generation time,
 * exactly as in {@link Condition} and {@link Aggregate}.
 */
public final class DbFunction {

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

  /**
   * Functional interface for fully-custom SQL resolution. Use this for non-standard function syntax
   * such as {@code CAST(col AS type)} or {@code EXTRACT(EPOCH FROM col)}.
   *
   * <pre>{@code
   * DbFunction.custom((strategy, gen) -> {
   *     NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
   *     String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
   *     return new DbFunction.Resolved("CAST(" + colSql + " AS TEXT)", Map.of());
   * })
   * }</pre>
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

  /**
   * A typed argument for a {@link DbFunction}.
   *
   * <ul>
   *   <li>{@link Col} — type-safe column reference, resolved eagerly, quoted lazily
   *   <li>{@link Val} — dynamic value → named parameter {@code :paramN}
   *   <li>{@link Lit} — fixed SQL string literal rendered as {@code 'value'}
   *   <li>{@link Nested} — composed inner {@code DbFunction}
   *   <li>{@link RawSql} — verbatim SQL fragment: keywords, type names, integer constants
   * </ul>
   */
  public sealed interface Arg
      permits DbFunction.Col,
          DbFunction.Val,
          DbFunction.Lit,
          DbFunction.Nested,
          DbFunction.RawSql {}

  /**
   * A type-safe column reference. Names are resolved eagerly; quoting is applied lazily.
   *
   * @param name the column name
   */
  public record Col(NameResolver.TableAndColumnName name) implements Arg {}

  /**
   * A dynamic value rendered as a named parameter ({@code :paramN}).
   *
   * @param value the value to bind
   */
  public record Val(Object value) implements Arg {}

  /**
   * A fixed SQL string literal, rendered as {@code 'value'} with internal single-quote escaping.
   * Use for constant function arguments such as date-part units ({@code 'month'}), format strings
   * ({@code '%Y-%m'}), or JSON path keys.
   *
   * @param literal the literal value
   */
  public record Lit(String literal) implements Arg {}

  /**
   * A nested {@code DbFunction}, resolved recursively without its alias.
   *
   * @param fn the nested function
   */
  public record Nested(DbFunction fn) implements Arg {}

  /**
   * A raw SQL fragment injected verbatim — no escaping. Use for SQL keywords, type names, or
   * integer/numeric constants that are part of the function syntax (e.g., {@code "1"} as an offset
   * in {@code LEAD(col, 1)}, {@code "ASC"} in suffixes).
   *
   * @param sql the raw SQL fragment
   */
  public record RawSql(String sql) implements Arg {}

  /**
   * Creates a type-safe column argument. The table and column names are resolved immediately via
   * {@link NameResolver}; quoting is applied lazily at SQL-generation time.
   *
   * @param ref a method reference to a getter of a column in an entity class
   * @param <T> the entity type
   * @param <R> the column type
   * @return a {@link Col} argument for a {@link DbFunction}
   */
  public static <T, R> Arg col(ColumnRef<T, R> ref) {
    return new Col(NameResolver.resolve(ref));
  }

  /**
   * Creates a parameterized value argument. The value becomes {@code :paramN} in the SQL.
   *
   * @param value the dynamic value to bind
   * @return a {@link Val} argument for a {@link DbFunction}
   */
  public static Arg val(Object value) {
    return new Val(value);
  }

  /**
   * Creates a SQL string literal argument, rendered as {@code 'value'}. Use for constant function
   * parameters that are part of the SQL syntax: date units, format strings, JSON keys, separator
   * characters, etc.
   *
   * @param literal the literal string to render
   * @return a {@link Lit} argument for a {@link DbFunction}
   */
  public static Arg lit(String literal) {
    return new Lit(literal);
  }

  /**
   * Creates a nested-function argument (composed function calls).
   *
   * @param fn the inner {@link DbFunction} to nest
   * @return a {@link Nested} argument for a {@link DbFunction}
   */
  public static Arg nested(DbFunction fn) {
    return new Nested(fn);
  }

  /**
   * Creates a raw SQL fragment argument injected verbatim without escaping. Use for SQL keywords,
   * type names, and integer/boolean constants that are structurally part of the function call
   * syntax.
   *
   * @param sql the raw SQL fragment to inject
   * @return a {@link RawSql} argument for a {@link DbFunction}
   */
  public static Arg raw(String sql) {
    return new RawSql(sql);
  }

  /**
   * Creates a standard {@code name(arg1, arg2, ...)} function.
   *
   * <pre>{@code
   * DbFunction.of("COALESCE", DbFunction.col(Order::getNotes), DbFunction.val("N/A"))
   * // → COALESCE(orders.notes, :param0)
   * }</pre>
   *
   * @param name the function name
   * @param args the function arguments
   * @return a {@link DbFunction} instance
   */
  public static DbFunction of(String name, Arg... args) {
    return of(name, Arrays.asList(args), null);
  }

  /**
   * Creates a function with an optional suffix appended after the closing parenthesis. Useful for
   * aggregate-style functions with internal ORDER BY: {@code STRING_AGG(col, ', ' ORDER BY col
   * ASC)}.
   *
   * @param name the function name
   * @param args the function arguments
   * @param suffix an optional SQL fragment to append after the closing parenthesis
   * @return a {@link DbFunction} instance
   */
  public static DbFunction of(String name, List<Arg> args, String suffix) {
    List<Arg> immutableArgs = List.copyOf(args);
    return new DbFunction(
        (strategy, gen) -> {
          StringBuilder sb = new StringBuilder(name).append("(");
          Map<String, Object> params = new LinkedHashMap<>();
          for (int i = 0; i < immutableArgs.size(); i++) {
            if (i > 0) sb.append(", ");
            appendArg(immutableArgs.get(i), sb, params, strategy, gen);
          }
          sb.append(")");
          if (suffix != null) sb.append(suffix);
          return new Resolved(sb.toString(), Collections.unmodifiableMap(params));
        });
  }

  /**
   * Creates a {@code DbFunction} with a fully-custom SQL resolver. Use this for non-standard SQL
   * function syntax where the generic {@code func(arg, arg, ...)} form does not apply (e.g., {@code
   * CAST(x AS type)}, {@code EXTRACT(field FROM col)}, {@code TRIM(LEADING ' ' FROM col)}).
   *
   * <pre>{@code
   * public static DbFunction extract(String field, ColumnRef<?, ?> col) {
   *     NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
   *     return DbFunction.custom((strategy, gen) -> {
   *         String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
   *         return new DbFunction.Resolved("EXTRACT(" + field.toUpperCase() + " FROM " + colSql + ")", Map.of());
   *     });
   * }
   * }</pre>
   *
   * @param resolver a custom SQL resolver for the function
   * @return a {@link DbFunction} instance
   */
  public static DbFunction custom(Resolver resolver) {
    return new DbFunction(resolver);
  }

  private final Resolver resolver;
  private volatile String alias;
  private final AtomicReference<Resolved> standaloneCache = new AtomicReference<>();

  private DbFunction(Resolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Sets a SELECT alias ({@code AS alias}). Returns {@code this} for fluent chaining.
   *
   * @param alias the alias to use in SELECT lists
   * @return this instance
   */
  public DbFunction as(String alias) {
    this.alias = alias;
    return this;
  }

  Resolved resolve(QuoteStrategy strategy, ParamNameGenerator gen) {
    Resolved core = resolver.resolve(strategy, gen);
    if (alias != null && !alias.isBlank()) {
      return new Resolved(core.sql() + " AS " + alias, core.params());
    }
    return core;
  }

  Resolved resolveWithoutAlias(QuoteStrategy strategy, ParamNameGenerator gen) {
    return resolver.resolve(strategy, gen);
  }

  /**
   * Returns the SQL for this function (with alias if set) using a fresh parameter generator. Param
   * names start from {@code param0}. For embedded use inside a {@code Select} query, param names
   * are assigned by the query's shared generator automatically.
   *
   * @return the SQL string with parameter placeholders
   */
  public String toSql() {
    ensureStandaloneCache();
    return standaloneCache.get().sql();
  }

  /**
   * Returns the SQL without alias — for use in HAVING references, ORDER BY, and WHERE conditions.
   *
   * @return the SQL string without alias
   */
  public String toSqlWithoutAlias() {
    return resolver.resolve(QuoteStrategy.NONE, new ParamNameGenerator()).sql();
  }

  /**
   * Returns the parameter map for standalone use. For embedded use, parameters are merged into the
   * query's global map by {@code Select.toSqlAndParams()}.
   *
   * @return the parameter map
   */
  public Map<String, Object> getParams() {
    ensureStandaloneCache();
    return standaloneCache.get().params();
  }

  private void ensureStandaloneCache() {
    if (standaloneCache.get() == null) {
      synchronized (this) {
        if (standaloneCache.get() == null) {
          Resolved r = resolve(QuoteStrategy.NONE, new ParamNameGenerator());
          standaloneCache.set(new Resolved(r.sql(), Map.copyOf(r.params())));
        }
      }
    }
  }

  private static void appendArg(
      Arg arg,
      StringBuilder sql,
      Map<String, Object> params,
      QuoteStrategy strategy,
      ParamNameGenerator gen) {
    if (arg instanceof Col c) {
      NameResolver.TableAndColumnName tcn = c.name();
      sql.append(strategy.quoteQualified(tcn.tableName()))
          .append(".")
          .append(strategy.quote(tcn.columnName()));
    } else if (arg instanceof Val v) {
      String p = gen.next();
      sql.append(":").append(p);
      params.put(p, v.value());
    } else if (arg instanceof Lit l)
      sql.append("'").append(l.literal().replace("'", "''")).append("'");
    else if (arg instanceof Nested n) {
      Resolved nested = n.fn().resolveWithoutAlias(strategy, gen);
      sql.append(nested.sql());
      params.putAll(nested.params());
    } else if (arg instanceof RawSql r) sql.append(r.sql());
  }
}
