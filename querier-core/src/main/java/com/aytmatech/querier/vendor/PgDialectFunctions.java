package com.aytmatech.querier.vendor;

import com.aytmatech.querier.ColumnRef;
import com.aytmatech.querier.DbFunction;
import com.aytmatech.querier.util.NameResolver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Type-safe factory for PostgreSQL-specific SQL functions.
 *
 * <p>These functions produce {@link DbFunction} instances that can be used anywhere a standard
 * Querier expression is accepted: SELECT lists, WHERE conditions, ORDER BY clauses, and analytic
 * window frames.
 *
 * <pre>{@code
 * // Monthly revenue grouped by customer
 * Select.builder()
 *     .select(Order::getCustomerId)
 *     .select(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt).as("month"))
 *     .select(Aggregate.sum(Order::getTotal).as("monthly_total"))
 *     .from(Order.class)
 *     .where(Condition.gte(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt), startDate))
 *     .groupBy(Order::getCustomerId)
 *     .orderBy(OrderBy.asc(PgDialectFunctions.dateTrunc("month", Order::getCreatedAt)))
 *     .build();
 *
 * // Customers whose notes are null → fallback
 * .select(PgDialectFunctions.coalesce(Order::getNotes, "No notes").as("notes"))
 *
 * // JSONB extraction
 * .where(Condition.eq(PgDialectFunctions.jsonbExtractPathText(Order::getMeta, "status"), "active"))
 * }</pre>
 */
public final class PgDialectFunctions {

  private PgDialectFunctions() {}

  /**
   * {@code DATE_TRUNC('unit', col)} — Truncates a timestamp to the specified precision. Common
   * units: {@code 'microseconds'}, {@code 'milliseconds'}, {@code 'second'}, {@code 'minute'},
   * {@code 'hour'}, {@code 'day'}, {@code 'week'}, {@code 'month'}, {@code 'quarter'}, {@code
   * 'year'}.
   *
   * <pre>{@code DATE_TRUNC('month', orders.created_at)}</pre>
   *
   * @param unit The unit of time to truncate to.
   * @param col The column reference to truncate.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction dateTrunc(String unit, ColumnRef<T, R> col) {
    return DbFunction.of("DATE_TRUNC", DbFunction.lit(unit), DbFunction.col(col));
  }

  /**
   * {@code DATE_PART('field', col)} — Extracts a sub-field as a double-precision number. Common
   * fields: {@code 'epoch'}, {@code 'year'}, {@code 'month'}, {@code 'day'}, {@code 'hour'}, {@code
   * 'minute'}, {@code 'second'}, {@code 'dow'}, {@code 'week'}.
   *
   * <pre>{@code DATE_PART('year', orders.created_at)}</pre>
   *
   * @param field The field to extract.
   * @param col The column reference to extract from.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction datePart(String field, ColumnRef<T, R> col) {
    return DbFunction.of("DATE_PART", DbFunction.lit(field), DbFunction.col(col));
  }

  /**
   * {@code EXTRACT(field FROM col)} — Extracts a sub-field (non-standard syntax). Prefer {@link
   * #datePart} for simplicity; {@code EXTRACT} may return {@code numeric} rather than {@code double
   * precision} depending on the PostgreSQL version.
   *
   * <pre>{@code EXTRACT(EPOCH FROM orders.created_at)}</pre>
   *
   * @param field The field to extract.
   * @param col The column reference to extract from.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction extract(String field, ColumnRef<T, R> col) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved(
              "EXTRACT(" + field.toUpperCase() + " FROM " + colSql + ")", Map.of());
        });
  }

  /**
   * {@code TO_CHAR(col, 'format')} — Converts a timestamp or number to a formatted string.
   *
   * <pre>{@code TO_CHAR(orders.created_at, 'YYYY-MM')}</pre>
   *
   * @param col The column reference to format.
   * @param format The format string.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction toChar(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TO_CHAR", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code AGE(col)} — Computes the interval between the given timestamp and the current time.
   *
   * <pre>{@code AGE(orders.created_at)}</pre>
   *
   * @param col The column reference to compute the age of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction age(ColumnRef<T, R> col) {
    return DbFunction.of("AGE", DbFunction.col(col));
  }

  /**
   * {@code AGE(end, start)} — Computes the interval between two timestamps.
   *
   * <pre>{@code AGE(orders.delivered_at, orders.created_at)}</pre>
   *
   * @param start The starting timestamp column reference.
   * @param end The ending timestamp column reference.
   * @param <T1> The entity type of the end column
   * @param <R1> The column type of the end column
   * @param <T2> The entity type of the start column
   * @param <R2> The column type of the start column
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T1, R1, T2, R2> DbFunction age(ColumnRef<T1, R1> end, ColumnRef<T2, R2> start) {
    return DbFunction.of("AGE", DbFunction.col(end), DbFunction.col(start));
  }

  /**
   * {@code NOW()} — Returns the current transaction timestamp.
   *
   * <pre>{@code NOW()}</pre>
   *
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static DbFunction now() {
    return DbFunction.of("NOW");
  }

  /**
   * {@code UPPER(col)}
   *
   * @param col The column reference to convert to uppercase.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction upper(ColumnRef<T, R> col) {
    return DbFunction.of("UPPER", DbFunction.col(col));
  }

  /**
   * {@code LOWER(col)}
   *
   * @param col The column reference to convert to lowercase.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction lower(ColumnRef<T, R> col) {
    return DbFunction.of("LOWER", DbFunction.col(col));
  }

  /**
   * {@code INITCAP(col)} — Converts the first letter of each word to upper case.
   *
   * @param col The column reference to convert to title case.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction initcap(ColumnRef<T, R> col) {
    return DbFunction.of("INITCAP", DbFunction.col(col));
  }

  /**
   * {@code TRIM(col)}
   *
   * @param col The column reference to trim whitespace from.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction trim(ColumnRef<T, R> col) {
    return DbFunction.of("TRIM", DbFunction.col(col));
  }

  /**
   * {@code LENGTH(col)} — Number of characters in the string.
   *
   * @param col The column reference to measure the length of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction length(ColumnRef<T, R> col) {
    return DbFunction.of("LENGTH", DbFunction.col(col));
  }

  /**
   * {@code LEFT(col, n)} — Returns the first {@code n} characters.
   *
   * @param col The column reference to extract from.
   * @param n The number of characters to extract.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction left(ColumnRef<T, R> col, int n) {
    return DbFunction.of("LEFT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code RIGHT(col, n)} — Returns the last {@code n} characters.
   *
   * @param col The column reference to extract from.
   * @param n The number of characters to extract.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction right(ColumnRef<T, R> col, int n) {
    return DbFunction.of("RIGHT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code LPAD(col, length, fill)} — Left-pads the string to {@code length} with {@code fill}.
   *
   * <pre>{@code LPAD(orders.code, 10, '0')}</pre>
   *
   * @param col The column reference to pad.
   * @param length The target length of the string.
   * @param fill The string to use for padding.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction lpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "LPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code RPAD(col, length, fill)} — Right-pads the string to {@code length} with {@code fill}.
   *
   * @param col The column reference to pad.
   * @param length The target length of the string.
   * @param fill The string to use for padding.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction rpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "RPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code CONCAT(col1, col2, ...)} — Concatenates columns (NULL-safe: ignores NULLs).
   *
   * @param cols The column references to concatenate.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  @SafeVarargs
  public static <T, R> DbFunction concat(ColumnRef<T, R>... cols) {
    DbFunction.Arg[] args = Arrays.stream(cols).map(DbFunction::col).toArray(DbFunction.Arg[]::new);
    return DbFunction.of("CONCAT", args);
  }

  /**
   * {@code CONCAT_WS('sep', col1, col2, ...)} — Concatenates with a separator (skips NULLs).
   *
   * <pre>{@code CONCAT_WS(' ', customers.first_name, customers.last_name)}</pre>
   *
   * @param separator The separator string.
   * @param cols The column references to concatenate.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  @SafeVarargs
  public static <T, R> DbFunction concatWs(String separator, ColumnRef<T, R>... cols) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.lit(separator));
    for (ColumnRef<T, R> col : cols) args.add(DbFunction.col(col));
    return DbFunction.of("CONCAT_WS", args, null);
  }

  /**
   * {@code REGEXP_REPLACE(col, pattern, replacement)} — Replaces substrings matching a regex. The
   * {@code pattern} and {@code replacement} become named parameters (safe for user input).
   *
   * <pre>{@code REGEXP_REPLACE(customers.phone, '\D', '', 'g')}</pre>
   *
   * @param col The column reference to perform the replacement on.
   * @param pattern The regex pattern to match.
   * @param replacement The replacement string.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction regexpReplace(
      ColumnRef<T, R> col, String pattern, String replacement) {
    return DbFunction.of(
        "REGEXP_REPLACE",
        DbFunction.col(col),
        DbFunction.val(pattern),
        DbFunction.val(replacement));
  }

  /**
   * {@code REGEXP_REPLACE(col, pattern, replacement, flags)} — Variant with flags (e.g., {@code
   * 'g'}).
   *
   * @param col The column reference to perform the replacement on.
   * @param pattern The regex pattern to match.
   * @param replacement The replacement string.
   * @param flags The regex flags (e.g., {@code 'g'} for global).
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction regexpReplace(
      ColumnRef<T, R> col, String pattern, String replacement, String flags) {
    return DbFunction.of(
        "REGEXP_REPLACE",
        DbFunction.col(col),
        DbFunction.val(pattern),
        DbFunction.val(replacement),
        DbFunction.lit(flags));
  }

  /**
   * {@code STRING_AGG(col, 'sep')} — Concatenates non-null values with a separator. Use in SELECT
   * with a GROUP BY clause.
   *
   * <pre>{@code STRING_AGG(orders.status, ', ')}</pre>
   *
   * @param col The column reference to aggregate.
   * @param separator The separator string.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction stringAgg(ColumnRef<T, R> col, String separator) {
    return DbFunction.of("STRING_AGG", DbFunction.col(col), DbFunction.lit(separator));
  }

  /**
   * {@code STRING_AGG(col, 'sep' ORDER BY orderCol ASC/DESC)} — Ordered string aggregation.
   *
   * <pre>{@code STRING_AGG(orders.status, ', ' ORDER BY orders.created_at ASC)}</pre>
   *
   * @param col The column reference to aggregate.
   * @param separator The separator string.
   * @param orderCol The column reference to order by.
   * @param ascending Whether to order ascending (true) or descending (false).
   * @param <T1> The entity type of the column to aggregate
   * @param <R1> The column type of the column to aggregate
   * @param <T2> The entity type of the column to order by
   * @param <R2> The column type of the column to order by
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T1, R1, T2, R2> DbFunction stringAggOrdered(
      ColumnRef<T1, R1> col, String separator, ColumnRef<T2, R2> orderCol, boolean ascending) {
    NameResolver.TableAndColumnName orderTcn = NameResolver.resolve(orderCol);
    String direction = ascending ? "ASC" : "DESC";
    return DbFunction.of(
        "STRING_AGG",
        List.of(DbFunction.col(col), DbFunction.lit(separator)),
        " ORDER BY " + orderTcn.tableName() + "." + orderTcn.columnName() + " " + direction);
  }

  /**
   * {@code ARRAY_AGG(col)} — Aggregates values into a PostgreSQL array. Use in SELECT with a GROUP
   * BY clause.
   *
   * @param col The column reference to aggregate.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction arrayAgg(ColumnRef<T, R> col) {
    return DbFunction.of("ARRAY_AGG", DbFunction.col(col));
  }

  /**
   * {@code ARRAY_LENGTH(col, dimension)} — Returns the length of the array in the given dimension.
   * The first dimension is {@code 1}.
   *
   * @param col The column reference representing the array.
   * @param dimension The dimension to measure (1-based).
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction arrayLength(ColumnRef<T, R> col, int dimension) {
    return DbFunction.of(
        "ARRAY_LENGTH", DbFunction.col(col), DbFunction.raw(String.valueOf(dimension)));
  }

  /**
   * {@code UNNEST(col)} — Expands an array column to a set of rows. Typically used in the FROM
   * clause via {@code Expression.raw("UNNEST(...)")}, but also valid in SELECT for lateral
   * unnesting in some contexts.
   *
   * @param col The column reference representing the array.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction unnest(ColumnRef<T, R> col) {
    return DbFunction.of("UNNEST", DbFunction.col(col));
  }

  /**
   * {@code jsonb_extract_path_text(col, 'key1', 'key2', ...)} — Extracts a JSONB value as text.
   *
   * <pre>{@code
   * Condition.eq(PgDialectFunctions.jsonbExtractPathText(Order::getMeta, "status"), "active")
   * // → jsonb_extract_path_text(orders.meta, 'status') = :param0
   * }</pre>
   *
   * @param col The column reference representing the JSONB data.
   * @param keys The path of keys to extract.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction jsonbExtractPathText(ColumnRef<T, R> col, String... keys) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.col(col));
    for (String key : keys) args.add(DbFunction.lit(key));
    return DbFunction.of("jsonb_extract_path_text", args, null);
  }

  /**
   * {@code jsonb_extract_path(col, 'key1', 'key2', ...)} — Extracts a JSONB sub-object.
   *
   * @param col The column reference representing the JSONB data.
   * @param keys The path of keys to extract.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction jsonbExtractPath(ColumnRef<T, R> col, String... keys) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.col(col));
    for (String key : keys) args.add(DbFunction.lit(key));
    return DbFunction.of("jsonb_extract_path", args, null);
  }

  /**
   * {@code to_jsonb(col)} — Converts any SQL value to a JSONB representation.
   *
   * @param col The column reference to convert.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction toJsonb(ColumnRef<T, R> col) {
    return DbFunction.of("to_jsonb", DbFunction.col(col));
  }

  /**
   * {@code COALESCE(col, defaultValue)} — Returns the first non-NULL value.
   *
   * <pre>{@code COALESCE(orders.notes, :param0)}</pre>
   *
   * @param col The column reference to check for NULL.
   * @param defaultValue The default value to return if the column is NULL.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction coalesce(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("COALESCE", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code COALESCE(col1, col2, ...)} — Returns the first non-NULL column value.
   *
   * @param cols The column references to check for NULL.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  @SafeVarargs
  public static <T, R> DbFunction coalesce(ColumnRef<T, R>... cols) {
    DbFunction.Arg[] args = Arrays.stream(cols).map(DbFunction::col).toArray(DbFunction.Arg[]::new);
    return DbFunction.of("COALESCE", args);
  }

  /**
   * {@code NULLIF(col, value)} — Returns NULL if {@code col = value}, otherwise returns {@code
   * col}.
   *
   * @param col The column reference to compare.
   * @param value The value to compare against.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction nullif(ColumnRef<T, R> col, Object value) {
    return DbFunction.of("NULLIF", DbFunction.col(col), DbFunction.val(value));
  }

  /**
   * {@code GREATEST(col1, col2, ...)} — Returns the largest non-NULL value.
   *
   * @param cols The column references to compare.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  @SafeVarargs
  public static <T, R> DbFunction greatest(ColumnRef<T, R>... cols) {
    DbFunction.Arg[] args = Arrays.stream(cols).map(DbFunction::col).toArray(DbFunction.Arg[]::new);
    return DbFunction.of("GREATEST", args);
  }

  /**
   * {@code LEAST(col1, col2, ...)} — Returns the smallest non-NULL value.
   *
   * @param cols The column references to compare.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  @SafeVarargs
  public static <T, R> DbFunction least(ColumnRef<T, R>... cols) {
    DbFunction.Arg[] args = Arrays.stream(cols).map(DbFunction::col).toArray(DbFunction.Arg[]::new);
    return DbFunction.of("LEAST", args);
  }

  /**
   * {@code ROUND(col, places)} — Rounds to the given number of decimal places.
   *
   * @param col The column reference to round.
   * @param places The number of decimal places to round to.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction round(ColumnRef<T, R> col, int places) {
    return DbFunction.of("ROUND", DbFunction.col(col), DbFunction.raw(String.valueOf(places)));
  }

  /**
   * {@code FLOOR(col)}
   *
   * @param col The column reference to floor.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction floor(ColumnRef<T, R> col) {
    return DbFunction.of("FLOOR", DbFunction.col(col));
  }

  /**
   * {@code CEIL(col)}
   *
   * @param col The column reference to ceil.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction ceil(ColumnRef<T, R> col) {
    return DbFunction.of("CEIL", DbFunction.col(col));
  }

  /**
   * {@code ABS(col)}
   *
   * @param col The column reference to get the absolute value of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction abs(ColumnRef<T, R> col) {
    return DbFunction.of("ABS", DbFunction.col(col));
  }

  /**
   * {@code SQRT(col)}
   *
   * @param col The column reference to get the square root of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction sqrt(ColumnRef<T, R> col) {
    return DbFunction.of("SQRT", DbFunction.col(col));
  }

  /**
   * {@code POWER(col, exponent)} — Raises {@code col} to the given power. The {@code exponent}
   * becomes a named parameter.
   *
   * @param col The column reference to raise to a power.
   * @param exponent The exponent to raise the column to.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction power(ColumnRef<T, R> col, Number exponent) {
    return DbFunction.of("POWER", DbFunction.col(col), DbFunction.val(exponent));
  }

  /**
   * {@code CAST(col AS pgType)} — Type-casts a column to the specified PostgreSQL type. The {@code
   * pgType} is injected verbatim (e.g., {@code "TEXT"}, {@code "BIGINT"}, {@code "NUMERIC(10,2)"},
   * {@code "TIMESTAMPTZ"}).
   *
   * <pre>{@code PgDialectFunctions.cast(Order::getId, "TEXT")  →  CAST(orders.id AS TEXT)}</pre>
   *
   * @param col The column reference to cast.
   * @param pgType The PostgreSQL type to cast to.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction cast(ColumnRef<T, R> col, String pgType) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved("CAST(" + colSql + " AS " + pgType + ")", Map.of());
        });
  }

  /**
   * {@code LAG(col, offset, default)} — Returns the value of {@code col} from a previous row.
   * Designed for use with {@code
   * WindowFunction.of(PgDialectFunctions.lag(...)).partitionBy(...).orderBy(...)}.
   *
   * <p>Use integer literals for {@code offset} (it become SQL raw fragments, not parameters,
   * because analytic offsets are structural SQL constants). While {@code defaultVal} will be a
   * parameter, so you can use any type for it.
   *
   * <pre>{@code
   * WindowFunction.of(PgDialectFunctions.lag(Order::getTotal, 1, 0))
   *     .partitionBy(Order::getCustomerId)
   *     .orderBy(OrderBy.asc(Order::getCreatedAt))
   *     .as("prev_total")
   * // → LAG(orders.total, 1, 0) OVER (PARTITION BY orders.customer_id ORDER BY orders.created_at ASC) AS prev_total
   * }</pre>
   *
   * @param col The column reference to get the previous value of.
   * @param offset The number of rows back to look (must be a non-negative integer literal).
   * @param defaultVal The default value to return if there is no previous row.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction lag(ColumnRef<T, R> col, int offset, Object defaultVal) {
    return DbFunction.of(
        "LAG",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(offset)),
        DbFunction.val(defaultVal));
  }

  /**
   * {@code LEAD(col, offset, default)} — Returns the value of {@code col} from a subsequent row.
   *
   * @param col The column reference to get the next value of.
   * @param offset The number of rows forward to look (must be a non-negative integer literal).
   * @param defaultVal The default value to return if there is no subsequent row.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction lead(ColumnRef<T, R> col, int offset, Object defaultVal) {
    return DbFunction.of(
        "LEAD",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(offset)),
        DbFunction.val(defaultVal));
  }

  /**
   * {@code FIRST_VALUE(col)} — Returns the first value in the window frame.
   *
   * @param col The column reference to get the first value of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction firstValue(ColumnRef<T, R> col) {
    return DbFunction.of("FIRST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code LAST_VALUE(col)} — Returns the last value in the window frame.
   *
   * @param col The column reference to get the last value of.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction lastValue(ColumnRef<T, R> col) {
    return DbFunction.of("LAST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code NTH_VALUE(col, n)} — Returns the n-th value in the window frame.
   *
   * @param col The column reference to get the n-th value of.
   * @param n The 1-based index of the value to return.
   * @param <T> The entity type
   * @param <R> The column type
   * @return A {@link DbFunction} representing the SQL expression.
   */
  public static <T, R> DbFunction nthValue(ColumnRef<T, R> col, int n) {
    return DbFunction.of("NTH_VALUE", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }
}
