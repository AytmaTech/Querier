package com.aytmatech.querier.ai.sql;

import com.aytmatech.querier.ai.model.AiEntityField;
import com.aytmatech.querier.ai.model.AiEntitySchema;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validates that AI-produced SQL:
 *
 * <ol>
 *   <li>Is a SELECT or WITH…SELECT — never DML or DDL.
 *   <li>Only references tables declared in the {@link AiSchemaContext}.
 *   <li>Only references known columns for dot-notation references ({@code alias.column} / {@code
 *       table.column}).
 * </ol>
 *
 * <p>Alias resolution is delegated entirely to {@link SqlAliasResolver} so that the mapping logic
 * is shared with {@link SqlToSelectParser} and tested once.
 */
public final class SqlSchemaValidator {

  /** Matches dot-notation column references: alias.column or table.column */
  private static final Pattern DOT_NOTATION =
      Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\.\\s*([a-zA-Z_*][a-zA-Z0-9_]*)\\b");

  /**
   * SQL keywords and built-in function/type names that are never column or table identifiers. This
   * list covers anything that could appear on the left side of a dot in valid SQL (e.g., {@code
   * INTERVAL '1 day'} is not a column ref).
   */
  private static final Set<String> SQL_KEYWORDS =
      Set.of(
          "SELECT",
          "FROM",
          "WHERE",
          "JOIN",
          "INNER",
          "LEFT",
          "RIGHT",
          "FULL",
          "OUTER",
          "CROSS",
          "ON",
          "AND",
          "OR",
          "NOT",
          "IN",
          "IS",
          "NULL",
          "TRUE",
          "FALSE",
          "AS",
          "BY",
          "GROUP",
          "ORDER",
          "HAVING",
          "LIMIT",
          "OFFSET",
          "DISTINCT",
          "WITH",
          "UNION",
          "ALL",
          "EXCEPT",
          "INTERSECT",
          "CASE",
          "WHEN",
          "THEN",
          "ELSE",
          "END",
          "EXISTS",
          "BETWEEN",
          "LIKE",
          "ILIKE",
          "ASC",
          "DESC",
          "INTERVAL",
          "OVER",
          "PARTITION",
          "ROWS",
          "RANGE",
          "CURRENT",
          "PRECEDING",
          "FOLLOWING",
          "UNBOUNDED",
          "RECURSIVE",
          "LATERAL",
          "COALESCE",
          "NULLIF",
          "CAST",
          "COUNT",
          "SUM",
          "AVG",
          "MIN",
          "MAX",
          "ROW_NUMBER",
          "RANK",
          "DENSE_RANK",
          "PERCENT_RANK",
          "CUME_DIST",
          "LAG",
          "LEAD",
          "FIRST_VALUE",
          "LAST_VALUE",
          "NTH_VALUE",
          "NTILE",
          "EXTRACT",
          "DATE_TRUNC",
          "DATE_PART",
          "ROUND",
          "CEIL",
          "FLOOR",
          "ABS",
          "MOD",
          "UPPER",
          "LOWER",
          "TRIM",
          "LENGTH",
          "SUBSTRING",
          "CONCAT",
          "NOW",
          "CURRENT_DATE",
          "CURRENT_TIMESTAMP",
          "INT",
          "INTEGER",
          "BIGINT",
          "DECIMAL",
          "NUMERIC",
          "FLOAT",
          "BOOLEAN",
          "TEXT",
          "VARCHAR",
          "CHAR",
          "DATE",
          "TIME",
          "TIMESTAMP",
          "UUID");

  private SqlSchemaValidator() {}

  /**
   * Validates the SQL string against the schema context.
   *
   * @param sql the AI-produced SQL to validate
   * @param context the allowed schema scope
   * @return list of human-readable issues; empty means the SQL passed all checks
   */
  public static List<String> validate(String sql, AiSchemaContext context) {
    List<String> issues = new ArrayList<>();

    if (sql == null || sql.isBlank()) {
      issues.add("SQL must not be blank");
      return issues;
    }

    String upperTrimmed = sql.strip().toUpperCase(Locale.ROOT);
    if (!upperTrimmed.startsWith("SELECT") && !upperTrimmed.startsWith("WITH")) {
      issues.add(
          "AI-generated SQL must be a SELECT or WITH statement. Got: '"
              + sql.strip().substring(0, Math.min(30, sql.strip().length()))
              + "'");
      return issues;
    }

    List<String> unknownTables = new ArrayList<>();
    Map<String, AiEntitySchema> aliasMap = SqlAliasResolver.resolve(sql, context, unknownTables);

    for (String unknown : unknownTables) {
      issues.add(
          "SQL references unknown table '"
              + unknown
              + "'. Allowed tables: "
              + knownTableNames(context));
    }

    Set<String> cteNames = SqlAliasResolver.extractCteNames(sql);

    Matcher dotMatcher = DOT_NOTATION.matcher(sql);
    while (dotMatcher.find()) {
      String prefix = dotMatcher.group(1);
      String column = dotMatcher.group(2);

      if (SQL_KEYWORDS.contains(prefix.toUpperCase(Locale.ROOT))) continue;
      if ("*".equals(column)) continue;
      if (column.matches("\\d+")) continue;
      if (cteNames.contains(prefix.toLowerCase(Locale.ROOT))) continue;

      AiEntitySchema schema = aliasMap.get(prefix.toLowerCase(Locale.ROOT));
      if (schema == null) {
        if (prefix.length() > 2) {
          issues.add(
              "Reference '"
                  + prefix
                  + "."
                  + column
                  + "' uses unknown table or alias '"
                  + prefix
                  + "'. Known tables/aliases: "
                  + aliasMap.keySet());
        }
        continue;
      }

      if (!columnKnown(column, schema)) {
        issues.add(
            "Column '"
                + column
                + "' does not exist on table '"
                + schema.tableName()
                + "'. Known columns: "
                + knownColumns(schema));
      }
    }

    if (aliasMap.isEmpty() && unknownTables.isEmpty()) {
      issues.add(
          "SQL does not reference any known table. Known tables: " + knownTableNames(context));
    }

    return issues;
  }

  /**
   * Returns weather the column name is known in the schema. This includes checking against SQL
   * keywords and the schema's fields.
   *
   * @param columnName the column name to check
   * @param schema the schema to check against
   * @return true if the column name is known, false otherwise
   */
  private static boolean columnKnown(String columnName, AiEntitySchema schema) {
    if (SQL_KEYWORDS.contains(columnName.toUpperCase(Locale.ROOT))) return true;
    return schema.fieldByColumn(columnName).isPresent();
  }

  /**
   * Returns a string representation of the list of known table names in the schema context.
   *
   * @param context the schema context to extract table names from
   * @return a string representation of the list of known table names in the schema context
   */
  private static String knownTableNames(AiSchemaContext context) {
    return context.schemas().stream().map(AiEntitySchema::tableName).toList().toString();
  }

  /**
   * Returns a string representation of the list of known columns in the schema.
   *
   * @param schema the schema to extract known columns from
   * @return a string representation of the list of known columns in the schema
   */
  private static String knownColumns(AiEntitySchema schema) {
    return schema.fields().stream().map(AiEntityField::columnName).toList().toString();
  }
}
