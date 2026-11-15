package com.aytmatech.querier.vendor;

import com.aytmatech.querier.ColumnRef;
import com.aytmatech.querier.DbFunction;
import com.aytmatech.querier.util.NameResolver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Type-safe factory for MySQL / MariaDB-specific SQL functions.
 *
 * <pre>{@code
 * // Monthly revenue
 * Select.builder()
 *     .select(MySqlDialectFunctions.dateFormat(Order::getCreatedAt, "%Y-%m").as("month"))
 *     .select(Aggregate.sum(Order::getTotal).as("monthly_total"))
 *     .from(Order.class)
 *     .groupBy(Order::getCustomerId)
 *     .build();
 *
 * // Null-safe fallback
 * .select(MySqlDialectFunctions.ifNull(Order::getNotes, "No notes").as("notes"))
 * }</pre>
 */
public final class MySqlDialectFunctions {

  private MySqlDialectFunctions() {}

  /**
   * {@code DATE_FORMAT(col, 'format')} — Formats a date/datetime column as a string. Common format
   * specifiers: {@code '%Y'} year, {@code '%m'} month, {@code '%d'} day, {@code '%H'} hour, {@code
   * '%i'} minute, {@code '%s'} second.
   *
   * <pre>{@code DATE_FORMAT(orders.created_at, '%Y-%m')}</pre>
   *
   * @param col the column reference
   * @param format the MySQL date format string
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction dateFormat(ColumnRef<T, R> col, String format) {
    return DbFunction.of("DATE_FORMAT", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code STR_TO_DATE(col, 'format')} — Parses a string column into a date using a format.
   *
   * @param col the column reference
   * @param format the MySQL date format string
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction strToDate(ColumnRef<T, R> col, String format) {
    return DbFunction.of("STR_TO_DATE", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code DATEDIFF(end, start)} — Returns the number of days between two date columns.
   *
   * <pre>{@code DATEDIFF(orders.delivered_at, orders.created_at)}</pre>
   *
   * @param start the start date column reference
   * @param end the end date column reference
   * @param <T1> The entity type of the end column
   * @param <R1> The column type of the end column
   * @param <T2> The entity type of the start column
   * @param <R2> The column type of the start column
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T1, R1, T2, R2> DbFunction dateDiff(
      ColumnRef<T1, R1> end, ColumnRef<T2, R2> start) {
    return DbFunction.of("DATEDIFF", DbFunction.col(end), DbFunction.col(start));
  }

  /**
   * {@code DATE_ADD(col, INTERVAL :amount unit)} — Adds an interval to a date. Common units: {@code
   * "DAY"}, {@code "MONTH"}, {@code "YEAR"}, {@code "HOUR"}. The {@code amount} becomes a named
   * parameter.
   *
   * <pre>{@code DATE_ADD(orders.created_at, INTERVAL 30 DAY)}</pre>
   *
   * @param col the date column reference
   * @param amount the amount to add (as a named parameter)
   * @param unit the interval unit (e.g., "DAY", "MONTH", "YEAR")
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction dateAdd(ColumnRef<T, R> col, Number amount, String unit) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          String p = gen.next();
          return new DbFunction.Resolved(
              "DATE_ADD(" + colSql + ", INTERVAL :" + p + " " + unit.toUpperCase() + ")",
              Map.of(p, amount));
        });
  }

  /**
   * {@code DATE_SUB(col, INTERVAL :amount unit)} — Subtracts an interval from a date.
   *
   * @param col the date column reference
   * @param amount the amount to subtract (as a named parameter)
   * @param unit the interval unit (e.g., "DAY", "MONTH", "YEAR")
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction dateSub(ColumnRef<T, R> col, Number amount, String unit) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          String p = gen.next();
          return new DbFunction.Resolved(
              "DATE_SUB(" + colSql + ", INTERVAL :" + p + " " + unit.toUpperCase() + ")",
              Map.of(p, amount));
        });
  }

  /**
   * {@code YEAR(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction year(ColumnRef<T, R> col) {
    return DbFunction.of("YEAR", DbFunction.col(col));
  }

  /**
   * {@code MONTH(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction month(ColumnRef<T, R> col) {
    return DbFunction.of("MONTH", DbFunction.col(col));
  }

  /**
   * {@code DAY(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction day(ColumnRef<T, R> col) {
    return DbFunction.of("DAY", DbFunction.col(col));
  }

  /**
   * {@code HOUR(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction hour(ColumnRef<T, R> col) {
    return DbFunction.of("HOUR", DbFunction.col(col));
  }

  /**
   * {@code MINUTE(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction minute(ColumnRef<T, R> col) {
    return DbFunction.of("MINUTE", DbFunction.col(col));
  }

  /**
   * {@code SECOND(col)}
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction second(ColumnRef<T, R> col) {
    return DbFunction.of("SECOND", DbFunction.col(col));
  }

  /**
   * {@code WEEK(col)} — Returns the week number (0–53).
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction week(ColumnRef<T, R> col) {
    return DbFunction.of("WEEK", DbFunction.col(col));
  }

  /**
   * {@code QUARTER(col)} — Returns the quarter (1–4).
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction quarter(ColumnRef<T, R> col) {
    return DbFunction.of("QUARTER", DbFunction.col(col));
  }

  /**
   * {@code LAST_DAY(col)} — Returns the last day of the month for the given date.
   *
   * @param col the date column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction lastDay(ColumnRef<T, R> col) {
    return DbFunction.of("LAST_DAY", DbFunction.col(col));
  }

  /**
   * {@code UPPER(col)}
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction upper(ColumnRef<T, R> col) {
    return DbFunction.of("UPPER", DbFunction.col(col));
  }

  /**
   * {@code LOWER(col)}
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction lower(ColumnRef<T, R> col) {
    return DbFunction.of("LOWER", DbFunction.col(col));
  }

  /**
   * {@code TRIM(col)}
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction trim(ColumnRef<T, R> col) {
    return DbFunction.of("TRIM", DbFunction.col(col));
  }

  /**
   * {@code LENGTH(col)} — Number of bytes; use {@code CHAR_LENGTH} for multi-byte strings.
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction length(ColumnRef<T, R> col) {
    return DbFunction.of("LENGTH", DbFunction.col(col));
  }

  /**
   * {@code CHAR_LENGTH(col)} — Number of characters (multi-byte safe).
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction charLength(ColumnRef<T, R> col) {
    return DbFunction.of("CHAR_LENGTH", DbFunction.col(col));
  }

  /**
   * {@code LEFT(col, n)}
   *
   * @param col the string column reference
   * @param n the number of characters to return from the left
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction left(ColumnRef<T, R> col, int n) {
    return DbFunction.of("LEFT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code RIGHT(col, n)}
   *
   * @param col the string column reference
   * @param n the number of characters to return from the right
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction right(ColumnRef<T, R> col, int n) {
    return DbFunction.of("RIGHT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code SUBSTRING(col, start, length)} — 1-based start position.
   *
   * @param col the string column reference
   * @param start the 1-based start position
   * @param length the number of characters to return
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction substring(ColumnRef<T, R> col, int start, int length) {
    return DbFunction.of(
        "SUBSTRING",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(start)),
        DbFunction.raw(String.valueOf(length)));
  }

  /**
   * {@code LPAD(col, length, fill)} — Left-pads to {@code length} with {@code fill}.
   *
   * @param col the string column reference
   * @param length the total length of the resulting string
   * @param fill the string to pad with
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction lpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "LPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code RPAD(col, length, fill)} — Right-pads to {@code length} with {@code fill}.
   *
   * @param col the string column reference
   * @param length the total length of the resulting string
   * @param fill the string to pad with
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction rpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "RPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code CONCAT(col1, col2, ...)} — Concatenates values (returns NULL if any arg is NULL). For
   * NULL-safe concatenation, use {@link #concatWs}.
   *
   * @param cols the string column references
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  @SafeVarargs
  public static <T, R> DbFunction concat(ColumnRef<T, R>... cols) {
    DbFunction.Arg[] args = Arrays.stream(cols).map(DbFunction::col).toArray(DbFunction.Arg[]::new);
    return DbFunction.of("CONCAT", args);
  }

  /**
   * {@code CONCAT_WS('sep', col1, col2, ...)} — Concatenates with separator, skipping NULLs.
   *
   * @param separator the separator string
   * @param cols the string column references
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  @SafeVarargs
  public static <T, R> DbFunction concatWs(String separator, ColumnRef<T, R>... cols) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.lit(separator));
    for (ColumnRef<T, R> col : cols) args.add(DbFunction.col(col));
    return DbFunction.of("CONCAT_WS", args, null);
  }

  /**
   * {@code GROUP_CONCAT(col)} — Concatenates group values into a comma-separated string. Use in
   * SELECT with a GROUP BY clause.
   *
   * <pre>{@code GROUP_CONCAT(orders.status)}</pre>
   *
   * @param col the string column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction groupConcat(ColumnRef<T, R> col) {
    return DbFunction.of("GROUP_CONCAT", DbFunction.col(col));
  }

  /**
   * {@code GROUP_CONCAT(col SEPARATOR 'sep')} — Group concatenation with a custom separator.
   * Non-standard syntax handled via custom resolver.
   *
   * @param col the string column reference
   * @param separator the separator string
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction groupConcat(ColumnRef<T, R> col, String separator) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved(
              "GROUP_CONCAT(" + colSql + " SEPARATOR '" + separator.replace("'", "''") + "')",
              Map.of());
        });
  }

  /**
   * {@code INSTR(col, :searchStr)} — Returns the position of the first occurrence of {@code
   * searchStr} in the column (1-based, 0 if not found). The search string is parameterized.
   *
   * @param col the string column reference
   * @param searchStr the string to search for
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction instr(ColumnRef<T, R> col, String searchStr) {
    return DbFunction.of("INSTR", DbFunction.col(col), DbFunction.val(searchStr));
  }

  /**
   * {@code FIND_IN_SET(:needle, col)} — Returns the position of {@code needle} in a comma-separated
   * list stored in the column (1-based, 0 if not found).
   *
   * @param col the string column reference
   * @param needle the string to search for
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction findInSet(String needle, ColumnRef<T, R> col) {
    return DbFunction.of("FIND_IN_SET", DbFunction.val(needle), DbFunction.col(col));
  }

  /**
   * {@code JSON_EXTRACT(col, '$.path')} — Extracts a value from a JSON column by path. Multiple
   * paths may be supplied and are converted to SQL string literals.
   *
   * <pre>{@code JSON_EXTRACT(orders.meta, '$.status')}</pre>
   *
   * @param col the JSON column reference
   * @param paths the JSON paths to extract
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction jsonExtract(ColumnRef<T, R> col, String... paths) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.col(col));
    for (String path : paths) args.add(DbFunction.lit(path));
    return DbFunction.of("JSON_EXTRACT", args, null);
  }

  /**
   * {@code JSON_VALUE(col, '$.path')} — Extracts a scalar value from a JSON column (MySQL 8.0+).
   *
   * @param col the JSON column reference
   * @param path the JSON path to extract
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction jsonValue(ColumnRef<T, R> col, String path) {
    return DbFunction.of("JSON_VALUE", DbFunction.col(col), DbFunction.lit(path));
  }

  /**
   * {@code JSON_ARRAYAGG(col)} — Aggregates column values into a JSON array (MySQL 5.7.22+).
   *
   * @param col the column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction jsonArrayAgg(ColumnRef<T, R> col) {
    return DbFunction.of("JSON_ARRAYAGG", DbFunction.col(col));
  }

  /**
   * {@code JSON_OBJECTAGG(keyCol, valCol)} — Aggregates key-value pairs into a JSON object.
   *
   * @param keyCol the column reference for keys
   * @param valCol the column reference for values
   * @param <T1> The entity type for keys
   * @param <R1> The column type for keys
   * @param <T2> The entity type for values
   * @param <R2> The column type for values
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T1, R1, T2, R2> DbFunction jsonObjectAgg(
      ColumnRef<T1, R1> keyCol, ColumnRef<T2, R2> valCol) {
    return DbFunction.of("JSON_OBJECTAGG", DbFunction.col(keyCol), DbFunction.col(valCol));
  }

  /**
   * {@code ROUND(col, dp)} — Rounds to {@code dp} decimal places.
   *
   * @param col the numeric column reference
   * @param dp the number of decimal places
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction round(ColumnRef<T, R> col, int dp) {
    return DbFunction.of("ROUND", DbFunction.col(col), DbFunction.raw(String.valueOf(dp)));
  }

  /**
   * {@code TRUNCATE(col, dp)} — Truncates to {@code dp} decimal places (not standard SQL).
   *
   * @param col the numeric column reference
   * @param dp the number of decimal places
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction truncate(ColumnRef<T, R> col, int dp) {
    return DbFunction.of("TRUNCATE", DbFunction.col(col), DbFunction.raw(String.valueOf(dp)));
  }

  /**
   * {@code FLOOR(col)}
   *
   * @param col the numeric column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction floor(ColumnRef<T, R> col) {
    return DbFunction.of("FLOOR", DbFunction.col(col));
  }

  /**
   * {@code CEILING(col)}
   *
   * @param col the numeric column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction ceiling(ColumnRef<T, R> col) {
    return DbFunction.of("CEILING", DbFunction.col(col));
  }

  /**
   * {@code ABS(col)}
   *
   * @param col the numeric column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction abs(ColumnRef<T, R> col) {
    return DbFunction.of("ABS", DbFunction.col(col));
  }

  /**
   * {@code SQRT(col)}
   *
   * @param col the numeric column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction sqrt(ColumnRef<T, R> col) {
    return DbFunction.of("SQRT", DbFunction.col(col));
  }

  /**
   * {@code POW(col, exponent)}
   *
   * @param col the numeric column reference
   * @param exponent the exponent value
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction pow(ColumnRef<T, R> col, Number exponent) {
    return DbFunction.of("POW", DbFunction.col(col), DbFunction.val(exponent));
  }

  /**
   * {@code SIGN(col)} — Returns -1, 0, or 1.
   *
   * @param col the numeric column reference
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction sign(ColumnRef<T, R> col) {
    return DbFunction.of("SIGN", DbFunction.col(col));
  }

  /**
   * {@code IFNULL(col, defaultValue)} — Returns {@code defaultValue} if {@code col} is NULL.
   * MySQL's NULL-safe two-argument version; prefer {@link #coalesce} for portability.
   *
   * @param col the column reference
   * @param defaultValue the default value to return if {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction ifNull(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("IFNULL", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code NULLIF(col, value)} — Returns NULL if {@code col = value}, otherwise returns {@code
   * col}.
   *
   * @param col the column reference
   * @param value the value to compare against
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction nullif(ColumnRef<T, R> col, Object value) {
    return DbFunction.of("NULLIF", DbFunction.col(col), DbFunction.val(value));
  }

  /**
   * {@code COALESCE(col, defaultValue)} — Returns the first non-NULL value.
   *
   * @param col the column reference
   * @param defaultValue the default value to return if {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction coalesce(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("COALESCE", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code CAST(col AS type)} — Type conversion. MySQL types: {@code "CHAR"}, {@code "SIGNED"},
   * {@code "UNSIGNED"}, {@code "DECIMAL(p,s)"}, {@code "DATE"}, {@code "DATETIME"}, {@code "TIME"},
   * {@code "BINARY"}, {@code "JSON"}.
   *
   * @param col the column reference
   * @param mysqlType the target type as a string
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the SQL function
   */
  public static <T, R> DbFunction cast(ColumnRef<T, R> col, String mysqlType) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved("CAST(" + colSql + " AS " + mysqlType + ")", Map.of());
        });
  }
}
