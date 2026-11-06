package com.aytmatech.querier.vendor;

import com.aytmatech.querier.ColumnRef;
import com.aytmatech.querier.DbFunction;
import com.aytmatech.querier.util.NameResolver;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Type-safe factory for Oracle Database-specific SQL functions.
 *
 * <p>These functions produce {@link DbFunction} instances that can be used anywhere a standard
 * Querier expression is accepted: SELECT lists, WHERE conditions, ORDER BY clauses, and analytic
 * window frames.
 *
 * <pre>{@code
 * // Monthly revenue grouped by customer (Oracle style)
 * Select.builder()
 *     .select(Order::getCustomerId)
 *     .select(OracleDialectFunctions.toChar(Order::getCreatedAt, "YYYY-MM").as("month"))
 *     .select(Aggregate.sum(Order::getTotal).as("monthly_total"))
 *     .from(Order.class)
 *     .where(Condition.gte(OracleDialectFunctions.trunc(Order::getCreatedAt, "MM"), startDate))
 *     .groupBy(Order::getCustomerId)
 *     .orderBy(OrderBy.asc(OracleDialectFunctions.trunc(Order::getCreatedAt, "MM")))
 *     .quoteStrategy(QuoteStrategy.ANSI)
 *     .build();
 *
 * // Null-safe fallback (Oracle NVL)
 * .select(OracleDialectFunctions.nvl(Order::getNotes, "No notes").as("notes"))
 *
 * // DECODE lookup
 * .select(OracleDialectFunctions.decode(Order::getStatus,
 *     "PAID",     "Completed",
 *     "SHIPPED",  "In Transit",
 *     "Pending").as("label"))
 * }</pre>
 */
public final class OracleDialectFunctions {

  private OracleDialectFunctions() {}

  /**
   * {@code TRUNC(col, 'format')} — Truncates a date/timestamp to the specified unit.
   *
   * <p>Common format masks: {@code 'YYYY'} year, {@code 'MM'} month, {@code 'DD'} day (default),
   * {@code 'HH'} hour, {@code 'MI'} minute, {@code 'D'} start of week, {@code 'Q'} quarter.
   *
   * <pre>{@code TRUNC(orders.created_at, 'MM')}</pre>
   *
   * @param col the column reference to truncate
   * @param format the truncation format mask
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the truncation operation
   */
  public static <T, R> DbFunction trunc(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TRUNC", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code TRUNC(col, places)} — Truncates a number to {@code places} decimal places.
   *
   * <p>Note: Oracle uses {@code TRUNC} for both date and numeric truncation; the method is
   * overloaded to distinguish the two via the argument type.
   *
   * <pre>{@code TRUNC(orders.total, 2)}</pre>
   *
   * @param col the column reference to truncate
   * @param places the number of decimal places to truncate to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the truncation operation
   */
  public static <T, R> DbFunction trunc(ColumnRef<T, R> col, int places) {
    return DbFunction.of("TRUNC", DbFunction.col(col), DbFunction.raw(String.valueOf(places)));
  }

  /**
   * {@code ADD_MONTHS(col, :n)} — Adds {@code n} months to a date. {@code n} becomes a named
   * parameter. Use a negative value to subtract months.
   *
   * <pre>{@code ADD_MONTHS(orders.created_at, :param0)}</pre>
   *
   * @param col the column reference to add months to
   * @param months the number of months to add (negative to subtract)
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the month addition operation
   */
  public static <T, R> DbFunction addMonths(ColumnRef<T, R> col, int months) {
    return DbFunction.of("ADD_MONTHS", DbFunction.col(col), DbFunction.val(months));
  }

  /**
   * {@code MONTHS_BETWEEN(end, start)} — Returns the number of months between two date columns (may
   * return a fractional number if the dates are not on the same day).
   *
   * <pre>{@code MONTHS_BETWEEN(orders.delivered_at, orders.created_at)}</pre>
   *
   * @param start the starting date column reference
   * @param end the ending date column reference
   * @param <T1> The entity type of the end column
   * @param <R1> The column type of the end column
   * @param <T2> The entity type of the start column
   * @param <R2> The column type of the start column
   * @return a {@link DbFunction} representing the month difference operation
   */
  public static <T1, R1, T2, R2> DbFunction monthsBetween(
      ColumnRef<T1, R1> end, ColumnRef<T2, R2> start) {
    return DbFunction.of("MONTHS_BETWEEN", DbFunction.col(end), DbFunction.col(start));
  }

  /**
   * {@code LAST_DAY(col)} — Returns the last calendar day of the month containing the given date.
   *
   * <pre>{@code LAST_DAY(orders.created_at)}</pre>
   *
   * @param col the column reference to find the last day of the month for
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the last day of month operation
   */
  public static <T, R> DbFunction lastDay(ColumnRef<T, R> col) {
    return DbFunction.of("LAST_DAY", DbFunction.col(col));
  }

  /**
   * {@code NEXT_DAY(col, :dayName)} — Returns the date of the next occurrence of the named weekday
   * after {@code col}. The day name is parameterized (use the session language, e.g., {@code
   * 'MONDAY'}).
   *
   * <pre>{@code NEXT_DAY(orders.created_at, :param0)}</pre>
   *
   * @param col the column reference to find the next weekday for
   * @param dayName the name of the weekday (e.g., "MONDAY", "TUESDAY", etc.)
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the next weekday operation
   */
  public static <T, R> DbFunction nextDay(ColumnRef<T, R> col, String dayName) {
    return DbFunction.of("NEXT_DAY", DbFunction.col(col), DbFunction.val(dayName));
  }

  /**
   * {@code EXTRACT(field FROM col)} — Extracts a date part as a number. Supported fields: {@code
   * YEAR}, {@code MONTH}, {@code DAY}, {@code HOUR}, {@code MINUTE}, {@code SECOND}.
   *
   * <pre>{@code EXTRACT(YEAR FROM orders.created_at)}</pre>
   *
   * @param col the column reference to extract from
   * @param field the date part to extract (e.g., "YEAR", "MONTH", "DAY", etc.)
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the extraction operation
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
   * {@code TO_DATE(col, 'format')} — Converts a string column to an Oracle DATE using the given
   * format mask.
   *
   * <pre>{@code TO_DATE(orders.created_at, 'YYYY-MM-DD')}</pre>
   *
   * @param col the column reference to convert
   * @param format the date format mask (e.g., "YYYY-MM-DD")
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the date conversion operation
   */
  public static <T, R> DbFunction toDate(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TO_DATE", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code TO_TIMESTAMP(col, 'format')} — Converts a string column to an Oracle TIMESTAMP.
   *
   * <pre>{@code TO_TIMESTAMP(orders.created_at, 'YYYY-MM-DD HH24:MI:SS')}</pre>
   *
   * @param col the column reference to convert
   * @param format the timestamp format mask (e.g., "YYYY-MM-DD HH24:MI:SS")
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the timestamp conversion operation
   */
  public static <T, R> DbFunction toTimestamp(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TO_TIMESTAMP", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code TO_CHAR(col, 'format')} — Converts a date or number column to a formatted string.
   *
   * <p>Common date masks: {@code 'YYYY-MM'}, {@code 'YYYY-MM-DD'}, {@code 'YYYY-Q'}. Number masks:
   * {@code '999G999D99'}.
   *
   * <pre>{@code TO_CHAR(orders.created_at, 'YYYY-MM')}</pre>
   *
   * @param col the column reference to convert
   * @param format the format mask for the conversion
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the string conversion operation
   */
  public static <T, R> DbFunction toChar(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TO_CHAR", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code SYSDATE} — Returns the current date and time in the database server time zone.
   *
   * <p>Note: Oracle's {@code SYSDATE} has no parentheses; this is handled via a custom resolver.
   *
   * @return a {@link DbFunction} representing the current date and time
   */
  public static DbFunction sysdate() {
    return DbFunction.custom((strategy, gen) -> new DbFunction.Resolved("SYSDATE", Map.of()));
  }

  /**
   * {@code SYSTIMESTAMP} — Returns the current timestamp with fractional seconds and time zone.
   *
   * <p>Note: Oracle's {@code SYSTIMESTAMP} has no parentheses; handled via a custom resolver.
   *
   * @return a {@link DbFunction} representing the current timestamp
   */
  public static DbFunction systimestamp() {
    return DbFunction.custom((strategy, gen) -> new DbFunction.Resolved("SYSTIMESTAMP", Map.of()));
  }

  /**
   * {@code UPPER(col)}
   *
   * @param col the column reference to convert to upper case
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the upper case conversion
   */
  public static <T, R> DbFunction upper(ColumnRef<T, R> col) {
    return DbFunction.of("UPPER", DbFunction.col(col));
  }

  /**
   * {@code LOWER(col)}
   *
   * @param col the column reference to convert to lower case
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the lower case conversion
   */
  public static <T, R> DbFunction lower(ColumnRef<T, R> col) {
    return DbFunction.of("LOWER", DbFunction.col(col));
  }

  /**
   * {@code INITCAP(col)} — Converts the first letter of each word to upper case.
   *
   * @param col the column reference to convert to initcap
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the initcap conversion
   */
  public static <T, R> DbFunction initcap(ColumnRef<T, R> col) {
    return DbFunction.of("INITCAP", DbFunction.col(col));
  }

  /**
   * {@code TRIM(col)} — Removes leading and trailing spaces.
   *
   * @param col the column reference to trim
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the trim operation
   */
  public static <T, R> DbFunction trim(ColumnRef<T, R> col) {
    return DbFunction.of("TRIM", DbFunction.col(col));
  }

  /**
   * {@code LTRIM(col)} — Removes leading spaces.
   *
   * @param col the column reference to left trim
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the left trim operation
   */
  public static <T, R> DbFunction ltrim(ColumnRef<T, R> col) {
    return DbFunction.of("LTRIM", DbFunction.col(col));
  }

  /**
   * {@code RTRIM(col)} — Removes trailing spaces.
   *
   * @param col the column reference to right trim
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the right trim operation
   */
  public static <T, R> DbFunction rtrim(ColumnRef<T, R> col) {
    return DbFunction.of("RTRIM", DbFunction.col(col));
  }

  /**
   * {@code LENGTH(col)} — Returns the number of characters in the string.
   *
   * @param col the column reference to get the length of
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the length operation
   */
  public static <T, R> DbFunction length(ColumnRef<T, R> col) {
    return DbFunction.of("LENGTH", DbFunction.col(col));
  }

  /**
   * {@code SUBSTR(col, start, length)} — Oracle's substring function (1-based start position).
   *
   * <p>Note: Oracle uses {@code SUBSTR}, not {@code SUBSTRING}.
   *
   * <pre>{@code SUBSTR(orders.status, 1, 3)}</pre>
   *
   * @param col the column reference to extract the substring from
   * @param start the starting position (1-based index)
   * @param length the length of the substring to extract
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the substring operation
   */
  public static <T, R> DbFunction substr(ColumnRef<T, R> col, int start, int length) {
    return DbFunction.of(
        "SUBSTR",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(start)),
        DbFunction.raw(String.valueOf(length)));
  }

  /**
   * {@code INSTR(col, :searchStr)} — Returns the 1-based position of the first occurrence of {@code
   * searchStr} in the column (0 if not found). The search string is parameterized.
   *
   * <pre>{@code INSTR(orders.status, :param0)}</pre>
   *
   * @param col the column reference to search
   * @param searchStr the string to search for
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the INSTR operation
   */
  public static <T, R> DbFunction instr(ColumnRef<T, R> col, String searchStr) {
    return DbFunction.of("INSTR", DbFunction.col(col), DbFunction.val(searchStr));
  }

  /**
   * {@code LPAD(col, length, fill)} — Left-pads to {@code length} characters with {@code fill}.
   *
   * @param col the column reference to left pad
   * @param length the total length of the resulting string
   * @param fill the string to use for padding
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the left pad operation
   */
  public static <T, R> DbFunction lpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "LPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code RPAD(col, length, fill)} — Right-pads to {@code length} characters with {@code fill}.
   *
   * @param col the column reference to right pad
   * @param length the total length of the resulting string
   * @param fill the string to use for padding
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the right pad operation
   */
  public static <T, R> DbFunction rpad(ColumnRef<T, R> col, int length, String fill) {
    return DbFunction.of(
        "RPAD", DbFunction.col(col), DbFunction.raw(String.valueOf(length)), DbFunction.lit(fill));
  }

  /**
   * {@code REPLACE(col, :search, :replacement)} — Replaces every occurrence of {@code search} with
   * {@code replacement}. Both strings become named parameters.
   *
   * <pre>{@code REPLACE(orders.status, :param0, :param1)}</pre>
   *
   * @param col the column reference to perform the replacement on
   * @param search the string to search for
   * @param replacement the string to replace with
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the replace operation
   */
  public static <T, R> DbFunction replace(ColumnRef<T, R> col, String search, String replacement) {
    return DbFunction.of(
        "REPLACE", DbFunction.col(col), DbFunction.val(search), DbFunction.val(replacement));
  }

  /**
   * {@code REGEXP_REPLACE(col, :pattern, :replacement)} — Replaces substrings matching a POSIX
   * regular expression. Both pattern and replacement are parameterized.
   *
   * <pre>{@code REGEXP_REPLACE(customers.phone, :param0, :param1)}</pre>
   *
   * @param col the column reference to perform the regex replacement on
   * @param pattern the POSIX regular expression pattern to match
   * @param replacement the string to replace matches with
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the regex replace operation
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
   * {@code REGEXP_SUBSTR(col, :pattern)} — Extracts the first substring matching the regular
   * expression. The pattern is parameterized.
   *
   * <pre>{@code REGEXP_SUBSTR(orders.notes, :param0)}</pre>
   *
   * @param col the column reference to extract the substring from
   * @param pattern the POSIX regular expression pattern to match
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the regex substring operation
   */
  public static <T, R> DbFunction regexpSubstr(ColumnRef<T, R> col, String pattern) {
    return DbFunction.of("REGEXP_SUBSTR", DbFunction.col(col), DbFunction.val(pattern));
  }

  /**
   * {@code CONCAT(col1, col2)} — Concatenates exactly two columns. Oracle's built-in {@code CONCAT}
   * accepts exactly two arguments; nest calls for more: {@code DbFunction.nested(concat(col1,
   * col2))} as the first argument of another concat.
   *
   * <pre>{@code CONCAT(customers.first_name, customers.last_name)}</pre>
   *
   * @param col1 the first column reference to concatenate
   * @param col2 the second column reference to concatenate
   * @param <T1> The entity type of the first column
   * @param <R1> The column type of the first column
   * @param <T2> The entity type of the second column
   * @param <R2> The column type of the second column
   * @return a {@link DbFunction} representing the concatenation operation
   */
  public static <T1, R1, T2, R2> DbFunction concat(ColumnRef<T1, R1> col1, ColumnRef<T2, R2> col2) {
    return DbFunction.of("CONCAT", DbFunction.col(col1), DbFunction.col(col2));
  }

  /**
   * {@code LISTAGG(col, 'sep') WITHIN GROUP (ORDER BY orderCol direction)} — Aggregates non-null
   * column values into a single delimited string within each group. Requires the {@code WITHIN
   * GROUP} clause with an {@code ORDER BY} (non-standard syntax handled via custom resolver).
   *
   * <pre>{@code
   * OracleDialectFunctions.listagg(Order::getStatus, ", ", Order::getCreatedAt, true)
   * // → LISTAGG(orders.status, ', ') WITHIN GROUP (ORDER BY orders.created_at ASC)
   * }</pre>
   *
   * @param col the column reference to aggregate
   * @param separator the string to use as a separator between values
   * @param orderCol the column reference to order by within the group
   * @param ascending true for ascending order, false for descending
   * @param <T1> The entity type of the column to aggregate
   * @param <R1> The column type of the column to aggregate
   * @param <T2> The entity type of the column to order by
   * @param <R2> The column type of the column to order by
   * @return a {@link DbFunction} representing the LISTAGG operation
   */
  public static <T1, R1, T2, R2> DbFunction listagg(
      ColumnRef<T1, R1> col, String separator, ColumnRef<T2, R2> orderCol, boolean ascending) {
    NameResolver.TableAndColumnName colTcn = NameResolver.resolve(col);
    NameResolver.TableAndColumnName orderTcn = NameResolver.resolve(orderCol);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql =
              strategy.quote(colTcn.tableName()) + "." + strategy.quote(colTcn.columnName());
          String orderSql =
              strategy.quote(orderTcn.tableName()) + "." + strategy.quote(orderTcn.columnName());
          String direction = ascending ? "ASC" : "DESC";
          return new DbFunction.Resolved(
              "LISTAGG("
                  + colSql
                  + ", '"
                  + separator.replace("'", "''")
                  + "') WITHIN GROUP (ORDER BY "
                  + orderSql
                  + " "
                  + direction
                  + ")",
              Map.of());
        });
  }

  /**
   * {@code ROUND(col, places)} — Rounds to the given number of decimal places.
   *
   * @param col the column reference to round
   * @param places the number of decimal places to round to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the round operation
   */
  public static <T, R> DbFunction round(ColumnRef<T, R> col, int places) {
    return DbFunction.of("ROUND", DbFunction.col(col), DbFunction.raw(String.valueOf(places)));
  }

  /**
   * {@code CEIL(col)}
   *
   * @param col the column reference to apply the ceiling function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the ceiling operation
   */
  public static <T, R> DbFunction ceil(ColumnRef<T, R> col) {
    return DbFunction.of("CEIL", DbFunction.col(col));
  }

  /**
   * {@code FLOOR(col)}
   *
   * @param col the column reference to apply the floor function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the floor operation
   */
  public static <T, R> DbFunction floor(ColumnRef<T, R> col) {
    return DbFunction.of("FLOOR", DbFunction.col(col));
  }

  /**
   * {@code ABS(col)}
   *
   * @param col the column reference to apply the absolute value function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the absolute value operation
   */
  public static <T, R> DbFunction abs(ColumnRef<T, R> col) {
    return DbFunction.of("ABS", DbFunction.col(col));
  }

  /**
   * {@code SQRT(col)}
   *
   * @param col the column reference to apply the square root function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the square root operation
   */
  public static <T, R> DbFunction sqrt(ColumnRef<T, R> col) {
    return DbFunction.of("SQRT", DbFunction.col(col));
  }

  /**
   * {@code POWER(col, exponent)} — The exponent becomes a named parameter.
   *
   * @param col the column reference to raise to a power
   * @param exponent the exponent to raise the column value to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the power operation
   */
  public static <T, R> DbFunction power(ColumnRef<T, R> col, Number exponent) {
    return DbFunction.of("POWER", DbFunction.col(col), DbFunction.val(exponent));
  }

  /**
   * {@code SIGN(col)} — Returns -1, 0, or 1.
   *
   * @param col the column reference to apply the sign function to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the sign operation
   */
  public static <T, R> DbFunction sign(ColumnRef<T, R> col) {
    return DbFunction.of("SIGN", DbFunction.col(col));
  }

  /**
   * {@code MOD(col, :divisor)} — Returns the remainder of {@code col} divided by {@code divisor}.
   * The divisor becomes a named parameter.
   *
   * <pre>{@code MOD(orders.id, :param0)}</pre>
   *
   * @param col the column reference to apply the modulus operation to
   * @param divisor the divisor to use for the modulus operation
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the modulus operation
   */
  public static <T, R> DbFunction mod(ColumnRef<T, R> col, Number divisor) {
    return DbFunction.of("MOD", DbFunction.col(col), DbFunction.val(divisor));
  }

  /**
   * {@code NVL(col, :defaultValue)} — Oracle's two-argument null-substitution function. Returns
   * {@code defaultValue} when {@code col} is NULL. The default value is parameterized.
   *
   * <pre>{@code NVL(orders.notes, :param0)}</pre>
   *
   * @param col the column reference to check for NULL
   * @param defaultValue the value to return when {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the NVL operation
   */
  public static <T, R> DbFunction nvl(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("NVL", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code NVL2(col, :notNullVal, :nullVal)} — Returns {@code notNullVal} when {@code col} is not
   * NULL, or {@code nullVal} when it is NULL. Both results are parameterized.
   *
   * <pre>{@code NVL2(orders.notes, 'Has notes', 'No notes')}</pre>
   *
   * @param col the column reference to check for NULL
   * @param notNullVal the value to return when {@code col} is not NULL
   * @param nullVal the value to return when {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the NVL2 operation
   */
  public static <T, R> DbFunction nvl2(ColumnRef<T, R> col, Object notNullVal, Object nullVal) {
    return DbFunction.of(
        "NVL2", DbFunction.col(col), DbFunction.val(notNullVal), DbFunction.val(nullVal));
  }

  /**
   * {@code COALESCE(col, :defaultValue)} — Returns the first non-NULL value (standard SQL,
   * supported by Oracle).
   *
   * @param col the column reference to check for NULL
   * @param defaultValue the value to return when {@code col} is NULL
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the COALESCE operation
   */
  public static <T, R> DbFunction coalesce(ColumnRef<T, R> col, Object defaultValue) {
    return DbFunction.of("COALESCE", DbFunction.col(col), DbFunction.val(defaultValue));
  }

  /**
   * {@code NULLIF(col, :value)} — Returns NULL if {@code col = value}, otherwise returns {@code
   * col}.
   *
   * @param col the column reference to check for equality
   * @param value the value to compare against
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the NULLIF operation
   */
  public static <T, R> DbFunction nullif(ColumnRef<T, R> col, Object value) {
    return DbFunction.of("NULLIF", DbFunction.col(col), DbFunction.val(value));
  }

  /**
   * {@code DECODE(col, search1, result1, search2, result2, ..., default)} — Oracle's procedural
   * equality search. Equivalent to {@code CASE WHEN col = search1 THEN result1 ... ELSE default
   * END}.
   *
   * <p>Arguments must be supplied in pairs of {@code (searchN, resultN)}, with an optional trailing
   * default value. All arguments become sequential named parameters.
   *
   * <pre>{@code
   * OracleDialectFunctions.decode(Order::getStatus,
   *     "PAID",    "Completed",
   *     "SHIPPED", "In Transit",
   *     "Pending")
   * // → DECODE(orders.status, :param0, :param1, :param2, :param3, :param4)
   * }</pre>
   *
   * @param col the column reference to evaluate
   * @param searchResultPairsAndDefault the search-result pairs and optional default value
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the DECODE operation
   */
  public static <T, R> DbFunction decode(
      ColumnRef<T, R> col, Object... searchResultPairsAndDefault) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          Map<String, Object> params = new LinkedHashMap<>();
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          StringBuilder sb = new StringBuilder("DECODE(").append(colSql);
          for (Object arg : searchResultPairsAndDefault) {
            String p = gen.next();
            sb.append(", :").append(p);
            params.put(p, arg);
          }
          sb.append(")");
          return new DbFunction.Resolved(sb.toString(), Map.copyOf(params));
        });
  }

  /**
   * {@code JSON_VALUE(col, '$.path')} — Extracts a scalar JSON value as a SQL scalar (Oracle 12c+).
   *
   * <pre>{@code JSON_VALUE(orders.meta, '$.status')}</pre>
   *
   * @param col the column reference containing JSON data
   * @param path the JSON path expression to extract the value
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the JSON_VALUE operation
   */
  public static <T, R> DbFunction jsonValue(ColumnRef<T, R> col, String path) {
    return DbFunction.of("JSON_VALUE", DbFunction.col(col), DbFunction.lit(path));
  }

  /**
   * {@code JSON_QUERY(col, '$.path')} — Extracts a JSON object or array from a JSON column (Oracle
   * 12c+).
   *
   * <pre>{@code JSON_QUERY(orders.meta, '$.address')}</pre>
   *
   * @param col the column reference containing JSON data
   * @param path the JSON path expression to extract the object or array
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the JSON_QUERY operation
   */
  public static <T, R> DbFunction jsonQuery(ColumnRef<T, R> col, String path) {
    return DbFunction.of("JSON_QUERY", DbFunction.col(col), DbFunction.lit(path));
  }

  /**
   * {@code CAST(col AS oracleType)} — Type conversion. Common Oracle types: {@code "NUMBER"},
   * {@code "VARCHAR2(n)"}, {@code "DATE"}, {@code "TIMESTAMP"}, {@code "CLOB"}.
   *
   * <pre>{@code CAST(orders.id AS VARCHAR2(50))}</pre>
   *
   * @param col the column reference to cast
   * @param oracleType the Oracle type to cast to
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the CAST operation
   */
  public static <T, R> DbFunction cast(ColumnRef<T, R> col, String oracleType) {
    NameResolver.TableAndColumnName tcn = NameResolver.resolve(col);
    return DbFunction.custom(
        (strategy, gen) -> {
          String colSql = strategy.quote(tcn.tableName()) + "." + strategy.quote(tcn.columnName());
          return new DbFunction.Resolved("CAST(" + colSql + " AS " + oracleType + ")", Map.of());
        });
  }

  /**
   * {@code TO_NUMBER(col)} — Converts a string or expression to an Oracle NUMBER.
   *
   * @param col the column reference to convert
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the TO_NUMBER operation
   */
  public static <T, R> DbFunction toNumber(ColumnRef<T, R> col) {
    return DbFunction.of("TO_NUMBER", DbFunction.col(col));
  }

  /**
   * {@code TO_NUMBER(col, 'format')} — Converts with an Oracle format mask (e.g., {@code
   * '999G999D99'} for European decimal notation).
   *
   * @param col the column reference to convert
   * @param format the Oracle format mask
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the TO_NUMBER operation with format
   */
  public static <T, R> DbFunction toNumber(ColumnRef<T, R> col, String format) {
    return DbFunction.of("TO_NUMBER", DbFunction.col(col), DbFunction.lit(format));
  }

  /**
   * {@code LAG(col, offset, default)} — Returns a value from a prior row within the window
   * partition. The offset is a raw SQL integer constant; the default value is parameterized.
   *
   * <pre>{@code
   * WindowFunction.of(OracleDialectFunctions.lag(Order::getTotal, 1, BigDecimal.ZERO))
   *     .partitionBy(Order::getCustomerId)
   *     .orderBy(OrderBy.asc(Order::getCreatedAt))
   *     .as("prev_total")
   * // → LAG(orders.total, 1, :param0) OVER (PARTITION BY orders.customer_id ORDER BY orders.created_at ASC) AS prev_total
   * }</pre>
   *
   * @param col the column reference to retrieve from a prior row
   * @param offset the number of rows back to retrieve (must be a constant)
   * @param defaultVal the value to return when there is no prior row
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the LAG operation
   */
  public static <T, R> DbFunction lag(ColumnRef<T, R> col, int offset, Object defaultVal) {
    return DbFunction.of(
        "LAG",
        DbFunction.col(col),
        DbFunction.raw(String.valueOf(offset)),
        DbFunction.val(defaultVal));
  }

  /**
   * {@code LEAD(col, offset, default)} — Returns a value from a subsequent row within the window
   * partition. The offset is a raw SQL integer constant; the default value is parameterized.
   *
   * @param col the column reference to retrieve from a subsequent row
   * @param offset the number of rows forward to retrieve (must be a constant)
   * @param defaultVal the value to return when there is no subsequent row
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the LEAD operation
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
   * @param col the column reference to retrieve the first value from
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the FIRST_VALUE operation
   */
  public static <T, R> DbFunction firstValue(ColumnRef<T, R> col) {
    return DbFunction.of("FIRST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code LAST_VALUE(col)} — Returns the last value in the ordered window frame.
   *
   * @param col the column reference to retrieve the last value from
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the LAST_VALUE operation
   */
  public static <T, R> DbFunction lastValue(ColumnRef<T, R> col) {
    return DbFunction.of("LAST_VALUE", DbFunction.col(col));
  }

  /**
   * {@code NTH_VALUE(col, n)} — Returns the n-th row value in the ordered window frame (Oracle
   * 11g+). {@code n} is a raw SQL integer constant.
   *
   * @param col the column reference to retrieve the n-th value from
   * @param n the 1-based index of the value to retrieve (must be a constant)
   * @param <T> The entity type
   * @param <R> The column type
   * @return a {@link DbFunction} representing the NTH_VALUE operation
   */
  public static <T, R> DbFunction nthValue(ColumnRef<T, R> col, int n) {
    return DbFunction.of("NTH_VALUE", DbFunction.col(col), DbFunction.raw(String.valueOf(n)));
  }

  /**
   * {@code NTILE(n)} — Divides rows into {@code n} roughly equal buckets and returns the bucket
   * number for each row. Use with {@code WindowFunction.of(OracleDialectFunctions.ntile(4)).orderBy(...)}.
   *
   * @param n the number of buckets to divide the rows into (must be a constant)
   * @return a {@link DbFunction} representing the NTILE operation
   */
  public static DbFunction ntile(int n) {
    return DbFunction.of("NTILE", DbFunction.raw(String.valueOf(n)));
  }
}
