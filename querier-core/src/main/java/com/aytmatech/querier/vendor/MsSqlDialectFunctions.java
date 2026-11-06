package com.aytmatech.querier.vendor;

import com.aytmatech.querier.ColumnRef;
import com.aytmatech.querier.DbFunction;
import com.aytmatech.querier.util.NameResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Type-safe factory for Microsoft SQL Server / Azure SQL-specific SQL functions.
 *
 * <pre>{@code
 * // SQL Server null fallback
 * .select(MsSqlDialectFunctions.isNull(Order::getNotes, "No notes").as("notes"))
 *
 * // JSON value extraction (SQL Server 2016+)
 * .where(Condition.eq(MsSqlDialectFunctions.jsonValue(Order::getMeta, "$.status"), "active"))
 *
 * // Running total with LAG
 * WindowFunction.of(MsSqlDialectFunctions.lag(Order::getTotal, 1, 0))
 *     .partitionBy(Order::getCustomerId)
 *     .orderBy(OrderBy.asc(Order::getCreatedAt))
 *     .as("prev_total")
 * }</pre>
 */
public final class MsSqlDialectFunctions {

  private MsSqlDialectFunctions() {}

  /**
   * {@code DATEPART(unit, col)} — Extracts an integer date part. Common units: {@code "year"},
   * {@code "month"}, {@code "day"}, {@code "hour"}, {@code "minute"}, {@code "second"}, {@code
   * "week"}, {@code "weekday"}, {@code "quarter"}.
   *
   * <pre>{@code DATEPART(month, orders.created_at)}</pre>
   *
   * @param unit The date part to extract (e.g., "year", "month", "day", etc.)
   * @param col The column reference from which to extract the date part
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the DATEPART SQL function
   */
  public static <T, R> DbFunction datePart(String unit, ColumnRef<T, R> col) {
    return DbFunction.of("DATEPART", DbFunction.raw(unit.toLowerCase()), DbFunction.col(col));
  }

  /**
   * {@code DATEDIFF(unit, start, end)} — Returns the difference between two dates in the given
   * unit.
   *
   * <pre>{@code DATEDIFF(day, orders.created_at, orders.delivered_at)}</pre>
   *
   * @param unit The unit of time to measure the difference (e.g., "day", "month", "year", etc.)
   * @param start The starting date column reference
   * @param end The ending date column reference
   * @param <T1> The entity type of the start column
   * @param <R1> The type of the start column
   * @param <T2> The entity type of the end column
   * @param <R2> The type of the end column
   * @return A DbFunction representing the DATEDIFF SQL function
   */
  public static <T1, R1, T2, R2> DbFunction dateDiff(
      String unit, ColumnRef<T1, R1> start, ColumnRef<T2, R2> end) {
    return DbFunction.of(
        "DATEDIFF", DbFunction.raw(unit.toLowerCase()), DbFunction.col(start), DbFunction.col(end));
  }

  /**
   * {@code DATEADD(unit, :amount, col)} — Adds an interval to a date. The {@code amount} becomes a
   * named parameter.
   *
   * <pre>{@code DATEADD(day, 30, orders.created_at)}</pre>
   *
   * @param unit The unit of time to add (e.g., "day", "month", "year", etc.)
   * @param amount The amount of the unit to add (can be positive or negative)
   * @param col The column reference to which the interval will be added
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the DATEADD SQL function
   */
  public static <T, R> DbFunction dateAdd(String unit, Number amount, ColumnRef<T, R> col) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          String p = gen.next();
          return new DbFunction.Resolved(
              "DATEADD(" + unit.toLowerCase() + ", :" + p + ", " + colSql + ")", Map.of(p, amount));
        });
  }

  /**
   * {@code FORMAT(col, 'format')} — Formats a date/number as a string using .NET format strings.
   *
   * <pre>{@code FORMAT(orders.created_at, 'yyyy-MM')}</pre>
   *
   * @param col The column reference to format
   * @param format The .NET format string to use for formatting
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the FORMAT SQL function
   */
  public static <T, R> DbFunction format(ColumnRef<T, R> col, String format) {
    return DbFunction.of("FORMAT", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code EOMONTH(col)} — Returns the last day of the month containing the given date.
   *
   * @param col The column reference representing the date
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the EOMONTH SQL function
   */
  public static <T, R> DbFunction eoMonth(ColumnRef<T, R> col) {
    return DbFunction.of("EOMONTH", DbFunction.col(col));
  }

  /**
   * {@code UPPER(col)}
   *
   * @param col The column reference to convert to uppercase
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the UPPER SQL function
   */
  public static <T, R> DbFunction upper(ColumnRef<T, R> col) {
    return DbFunction.of("UPPER", DbFunction.col(col));
  }

  /**
   * {@code LOWER(col)}
   *
   * @param col The column reference to convert to lowercase
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LOWER SQL function
   */
  public static <T, R> DbFunction lower(ColumnRef<T, R> col) {
    return DbFunction.of("LOWER", DbFunction.col(col));
  }

  /**
   * {@code TRIM(col)} — SQL Server 2017+
   *
   * @param col The column reference to trim whitespace from
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the TRIM SQL function
   */
  public static <T, R> DbFunction trim(ColumnRef<T, R> col) {
    return DbFunction.of("TRIM", DbFunction.col(col));
  }

  /**
   * {@code LEN(col)} — Number of characters (SQL Server uses {@code LEN}, not {@code LENGTH}).
   *
   * @param col The column reference to measure the length of
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LEN SQL function
   */
  public static <T, R> DbFunction len(ColumnRef<T, R> col) {
    return DbFunction.of("LEN", DbFunction.col(col));
  }

  /**
   * {@code LEFT(col, n)}
   *
   * @param col The column reference from which to take the leftmost characters
   * @param n The number of characters to take from the left
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LEFT SQL function
   */
  public static <T, R> DbFunction left(ColumnRef<T, R> col, int n) {
    return DbFunction.of("LEFT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code RIGHT(col, n)}
   *
   * @param col The column reference from which to take the rightmost characters
   * @param n The number of characters to take from the right
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the RIGHT SQL function
   */
  public static <T, R> DbFunction right(ColumnRef<T, R> col, int n) {
    return DbFunction.of("RIGHT", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code SUBSTRING(col, start, length)} — 1-based.
   *
   * @param col The column reference from which to extract the substring
   * @param start The starting position (1-based index)
   * @param length The length of the substring to extract
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the SUBSTRING SQL function
   */
  public static <T, R> DbFunction substring(ColumnRef<T, R> col, int start, int length) {
    return DbFunction.of(
        "SUBSTRING",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(start)),
        DbFunction.raw(String.valueOf(length)));
  }

  /**
   * {@code CHARINDEX(:searchStr, col)} — Returns the 1-based position of the first occurrence of
   * {@code searchStr} in the column (0 if not found). The search string is parameterized.
   *
   * @param col The column reference in which to search
   * @param searchStr The string to search for
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CHARINDEX SQL function
   */
  public static <T, R> DbFunction charIndex(String searchStr, ColumnRef<T, R> col) {
    return DbFunction.of("CHARINDEX", DbFunction.val(searchStr), DbFunction.col(col));
  }

  /**
   * {@code PATINDEX('%pattern%', col)} — Returns the 1-based position of a pattern in the column.
   * The pattern is parameterized.
   *
   * @param col The column reference in which to search
   * @param pattern The pattern to search for (should include wildcards, e.g., '%abc%')
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the PATINDEX SQL function
   */
  public static <T, R> DbFunction patIndex(String pattern, ColumnRef<T, R> col) {
    return DbFunction.of("PATINDEX", DbFunction.val(pattern), DbFunction.col(col));
  }

  /**
   * {@code STUFF(col, start, length, :replacement)} — Deletes {@code length} characters from {@code
   * start} and inserts {@code replacement}. The replacement string is parameterized.
   *
   * @param col The column reference from which to delete and insert
   * @param start The starting position (1-based index) for deletion
   * @param length The number of characters to delete
   * @param replacement The string to insert at the starting position
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the STUFF SQL function
   */
  public static <T, R> DbFunction stuff(
      ColumnRef<T, R> col, int start, int length, String replacement) {
    return DbFunction.of(
        "STUFF",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(start)),
        DbFunction.raw(String.valueOf(length)),
        DbFunction.val(replacement));
  }

  /**
   * {@code CONCAT_WS('sep', col1, col2, ...)} — Concatenates with separator (SQL Server 2017+).
   *
   * @param separator The separator string to use between concatenated values
   * @param cols The column references to concatenate
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CONCAT_WS SQL function
   */
  @SafeVarargs
  public static <T, R> DbFunction concatWs(String separator, ColumnRef<T, R>... cols) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.lit(separator));
    for (ColumnRef<T, R> col : cols) args.add(DbFunction.col(col));
    return DbFunction.of("CONCAT_WS", args, null);
  }

  /**
   * {@code STRING_AGG(col, 'sep')} — Aggregates values into a delimited string (SQL Server 2017+).
   * Use in SELECT with a GROUP BY clause.
   *
   * @param col The column reference to aggregate
   * @param separator The separator string to use between aggregated values
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the STRING_AGG SQL function
   */
  public static <T, R> DbFunction stringAgg(ColumnRef<T, R> col, String separator) {
    return DbFunction.of("STRING_AGG", DbFunction.col(col), DbFunction.lit(separator));
  }

  /**
   * {@code JSON_VALUE(col, '$.path')} — Extracts a scalar value from a JSON column.
   *
   * <pre>{@code JSON_VALUE(orders.meta, '$.status')}</pre>
   *
   * @param col The column reference containing JSON data
   * @param path The JSON path expression to extract the value
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the JSON_VALUE SQL function
   */
  public static <T, R> DbFunction jsonValue(ColumnRef<T, R> col, String path) {
    return DbFunction.of("JSON_VALUE", DbFunction.col(col), DbFunction.lit(path));
  }

  /**
   * {@code JSON_QUERY(col, '$.path')} — Extracts an object or array from a JSON column.
   *
   * @param col The column reference containing JSON data
   * @param path The JSON path expression to extract the object or array
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the JSON_QUERY SQL function
   */
  public static <T, R> DbFunction jsonQuery(ColumnRef<T, R> col, String path) {
    return DbFunction.of("JSON_QUERY", DbFunction.col(col), DbFunction.lit(path));
  }

  /**
   * {@code ROUND(col, length)} — Rounds to {@code length} decimal places.
   *
   * @param col The column reference to round
   * @param length The number of decimal places to round to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the ROUND SQL function
   */
  public static <T, R> DbFunction round(ColumnRef<T, R> col, int length) {
    return DbFunction.of("ROUND", DbFunction.col(col), DbFunction.raw(String.valueOf(length)));
  }

  /**
   * {@code CEILING(col)}
   *
   * @param col The column reference to apply the CEILING function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CEILING SQL function
   */
  public static <T, R> DbFunction ceiling(ColumnRef<T, R> col) {
    return DbFunction.of("CEILING", DbFunction.col(col));
  }

  /**
   * {@code FLOOR(col)}
   *
   * @param col The column reference to apply the FLOOR function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the FLOOR SQL function
   */
  public static <T, R> DbFunction floor(ColumnRef<T, R> col) {
    return DbFunction.of("FLOOR", DbFunction.col(col));
  }

  /**
   * {@code ABS(col)}
   *
   * @param col The column reference to apply the ABS function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the ABS SQL function
   */
  public static <T, R> DbFunction abs(ColumnRef<T, R> col) {
    return DbFunction.of("ABS", DbFunction.col(col));
  }

  /**
   * {@code SQRT(col)}
   *
   * @param col The column reference to apply the SQRT function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the SQRT SQL function
   */
  public static <T, R> DbFunction sqrt(ColumnRef<T, R> col) {
    return DbFunction.of("SQRT", DbFunction.col(col));
  }

  /**
   * {@code POWER(col, exponent)}
   *
   * @param col The column reference to apply the POWER function to
   * @param exponent The exponent to raise the column value to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the POWER SQL function
   */
  public static <T, R> DbFunction power(ColumnRef<T, R> col, Number exponent) {
    return DbFunction.of("POWER", DbFunction.col(col), DbFunction.val(exponent));
  }

  /**
   * {@code SIGN(col)} — Returns -1, 0, or 1.
   *
   * @param col The column reference to apply the SIGN function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the SIGN SQL function
   */
  public static <T, R> DbFunction sign(ColumnRef<T, R> col) {
    return DbFunction.of("SIGN", DbFunction.col(col));
  }

  /**
   * {@code ISNULL(col, replacement)} — SQL Server's two-argument null-substitution function.
   * Returns {@code replacement} if {@code col} is NULL. The replacement is parameterized.
   *
   * <pre>{@code ISNULL(orders.notes, :param0)  →  orders.notes or 'No notes'}</pre>
   *
   * @param col The column reference to check for NULL
   * @param replacement The value to return if {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the ISNULL SQL function
   */
  public static <T, R> DbFunction isNull(ColumnRef<T, R> col, Object replacement) {
    return DbFunction.of("ISNULL", DbFunction.col(col), DbFunction.val(replacement));
  }

  /**
   * {@code NULLIF(col, value)} — Returns NULL if {@code col = value}, otherwise returns {@code
   * col}.
   *
   * @param col The column reference to compare
   * @param value The value to compare against
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the NULLIF SQL function
   */
  public static <T, R> DbFunction nullif(ColumnRef<T, R> col, Object value) {
    return DbFunction.of("NULLIF", DbFunction.col(col), DbFunction.val(value));
  }

  /**
   * {@code COALESCE(col, defaultValue)} — Returns the first non-NULL value (standard SQL, also
   * available in SQL Server).
   *
   * @param col The column reference to check for NULL
   * @param defaultValue The value to return if {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the COALESCE SQL function
   */
  public static <T, R> DbFunction coalesce(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("COALESCE", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code IIF(col, trueResult, falseResult)} — Inline IF (SQL Server 2012+). Both {@code
   * trueResult} and {@code falseResult} become named parameters. For condition-based IIF, use
   * {@code CaseWhen} instead.
   *
   * <pre>{@code IIF(orders.is_active, 'Yes', 'No')}</pre>
   *
   * @param col The column reference to evaluate as a boolean
   * @param trueResult The value to return if {@code col} is true
   * @param falseResult The value to return if {@code col} is false
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the IIF SQL function
   */
  public static <T, R> DbFunction iif(ColumnRef<T, R> col, Object trueResult, Object falseResult) {
    return DbFunction.of(
        "IIF", DbFunction.col(col), DbFunction.val(trueResult), DbFunction.val(falseResult));
  }

  /**
   * {@code CHOOSE(col, val1, val2, ...)} — Returns the item at the 1-based index given by {@code
   * col}. The option values become named parameters.
   *
   * @param col The column reference to use as a 1-based index
   * @param options The values to choose from
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CHOOSE SQL function
   */
  public static <T, R> DbFunction choose(ColumnRef<T, R> col, Object... options) {
    List<DbFunction.Arg> args = new ArrayList<>();
    args.add(DbFunction.col(col));
    for (Object opt : options) args.add(DbFunction.val(opt));
    return DbFunction.of("CHOOSE", args, null);
  }

  /**
   * {@code CAST(col AS type)} — Type conversion. SQL Server types: {@code "NVARCHAR(MAX)"}, {@code
   * "INT"}, {@code "BIGINT"}, {@code "DECIMAL(p,s)"}, {@code "DATETIME2"}, {@code "DATE"}, {@code
   * "BIT"}.
   *
   * @param col The column reference to cast
   * @param sqlServerType The SQL Server type to cast to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CAST SQL function
   */
  public static <T, R> DbFunction cast(ColumnRef<T, R> col, String sqlServerType) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved("CAST(" + colSql + " AS " + sqlServerType + ")", Map.of());
        });
  }

  /**
   * {@code TRY_CAST(col AS type)} — Like {@code CAST} but returns NULL on conversion failure
   * instead of raising an error (SQL Server 2012+).
   *
   * @param col The column reference to try to cast
   * @param sqlServerType The SQL Server type to cast to
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the TRY_CAST SQL function
   */
  public static <T, R> DbFunction tryCast(ColumnRef<T, R> col, String sqlServerType) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved(
              "TRY_CAST(" + colSql + " AS " + sqlServerType + ")", Map.of());
        });
  }

  /**
   * {@code CONVERT(type, col, style)} — SQL Server-specific type conversion with optional style.
   * The {@code style} becomes a named parameter.
   *
   * <pre>{@code CONVERT(VARCHAR(10), orders.created_at, 120)  →  'yyyy-mm-dd'}</pre>
   *
   * @param col The column reference to convert
   * @param sqlServerType The SQL Server type to convert to
   * @param style The style code for formatting (optional)
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the CONVERT SQL function
   */
  public static <T, R> DbFunction convert(String sqlServerType, ColumnRef<T, R> col, int style) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved(
              "CONVERT(" + sqlServerType + ", " + colSql + ", " + style + ")", Map.of());
        });
  }

  /**
   * {@code LAG(col, offset, default)} — Returns a value from a prior row in the window.
   *
   * <pre>{@code
   * WindowFunction.of(MsSqlDialectFunctions.lag(Order::getTotal, 1, 0))
   *     .partitionBy(Order::getCustomerId)
   *     .orderBy(OrderBy.asc(Order::getCreatedAt))
   *     .as("prev_total")
   * }</pre>
   *
   * @param col The column reference to retrieve the lagged value from
   * @param offset The number of rows back to look (default is 1)
   * @param defaultVal The value to return if there is no prior row (optional)
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LAG SQL function
   */
  public static <T, R> DbFunction lag(ColumnRef<T, R> col, int offset, Object defaultVal) {
    return DbFunction.of(
        "LAG",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(offset)),
        DbFunction.val(defaultVal));
  }

  /**
   * {@code LEAD(col, offset, default)} — Returns a value from a subsequent row in the window.
   *
   * @param col The column reference to retrieve the lead value from
   * @param offset The number of rows forward to look (default is 1)
   * @param defaultVal The value to return if there is no subsequent row (optional)
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LEAD SQL function
   */
  public static <T, R> DbFunction lead(ColumnRef<T, R> col, int offset, Object defaultVal) {
    return DbFunction.of(
        "LEAD",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(offset)),
        DbFunction.val(defaultVal));
  }

  /**
   * {@code FIRST_VALUE(col)} — Returns the first value in the ordered window frame.
   *
   * @param col The column reference to retrieve the first value from
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the FIRST_VALUE SQL function
   */
  public static <T, R> DbFunction firstValue(ColumnRef<T, R> col) {
    return DbFunction.of("FIRST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code LAST_VALUE(col)} — Returns the last value in the ordered window frame.
   *
   * @param col The column reference to retrieve the last value from
   * @param <T> The entity type
   * @param <R> The column type
   * @return A DbFunction representing the LAST_VALUE SQL function
   */
  public static <T, R> DbFunction lastValue(ColumnRef<T, R> col) {
    return DbFunction.of("LAST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code NTILE(n)} — Distributes rows into {@code n} roughly equal groups. Takes no column
   * argument; use with {@code WindowFunction.of(MsSqlDialectFunctions.ntile(4)).orderBy(...)}.
   *
   * @param n The number of groups to distribute rows into
   * @return A DbFunction representing the NTILE SQL function
   */
  public static DbFunction ntile(int n) {
    return DbFunction.of("NTILE", DbFunction.raw(String.valueOf(n)));
  }
}
