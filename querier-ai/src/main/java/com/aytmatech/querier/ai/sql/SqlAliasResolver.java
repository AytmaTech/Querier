package com.aytmatech.querier.ai.sql;

import com.aytmatech.querier.ai.model.AiEntitySchema;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses FROM/JOIN clauses from an AI-produced SQL string and builds a case-insensitive alias →
 * {@link AiEntitySchema} map.
 *
 * <p>Shared by {@link SqlSchemaValidator} (which needs the map to validate column references) and
 * {@link SqlToSelectParser} (which needs it to pick the primary entity class). Centralising this
 * logic means alias resolution is consistent, tested in one place, and never duplicated.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>Keys in the returned map are <em>lowercase</em> — callers must lower-case any lookup key
 *       before querying the map.
 *   <li>CTE names are excluded from the map; they are returned separately via {@link
 *       #extractCteNames(String)} so callers can skip validation for them.
 *   <li>Table names not found in the schema are collected into the {@code unknownTables} list
 *       supplied by the caller (pass {@code null} to discard them).
 * </ul>
 */
public final class SqlAliasResolver {

  /**
   * Matches FROM/JOIN table references at any nesting level.
   *
   * <p>Capture groups:
   *
   * <ol>
   *   <li>Table name (required)
   *   <li>Alias — either {@code tbl alias} or {@code tbl AS alias} (optional)
   * </ol>
   *
   * Examples matched:
   *
   * <pre>
   *   FROM orders
   *   FROM orders o
   *   FROM orders AS o
   *   LEFT JOIN customers c ON ...
   *   INNER JOIN customers AS c ON ...
   * </pre>
   */
  private static final Pattern FROM_JOIN_PATTERN =
      Pattern.compile(
          "(?:FROM|JOIN)\\s+([a-zA-Z_][a-zA-Z0-9_]*)"
              + "(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)(?=\\s|$|,|\\())?",
          Pattern.CASE_INSENSITIVE);

  /** Matches every CTE name: after WITH keyword, or after a comma (for subsequent CTEs). */
  private static final Pattern CTE_PATTERN =
      Pattern.compile(
          "(?i)(?:\\bWITH\\s+(?:RECURSIVE\\s+)?|,\\s*)"
              + "([a-z_][a-z0-9_$]*)\\s*"
              + "(?:\\([^)]*\\)\\s*)?AS\\s*\\(",
          Pattern.CASE_INSENSITIVE);

  /** SQL keywords that must never be interpreted as table aliases. */
  private static final Set<String> SQL_KEYWORDS =
      Set.of(
          "ON",
          "WHERE",
          "SET",
          "AND",
          "OR",
          "NOT",
          "IN",
          "IS",
          "LEFT",
          "RIGHT",
          "INNER",
          "OUTER",
          "FULL",
          "CROSS",
          "GROUP",
          "ORDER",
          "HAVING",
          "LIMIT",
          "OFFSET",
          "UNION",
          "EXCEPT",
          "INTERSECT",
          "SELECT",
          "DISTINCT",
          "WITH",
          "AS");

  private SqlAliasResolver() {}

  /**
   * Builds an alias → schema map by scanning all FROM/JOIN clauses in the SQL.
   *
   * <p>Both the raw table name and any declared alias are registered as keys so that {@code
   * orders.total} and {@code o.total} both resolve to the same schema.
   *
   * @param sql the AI-produced SQL string to scan
   * @param context the allowed schema scope
   * @param unknownTables caller-supplied list that receives any table name not found in the schema;
   *     pass {@code null} to ignore
   * @return lowercase alias/tableName → {@link AiEntitySchema}, never {@code null}
   */
  public static Map<String, AiEntitySchema> resolve(
      String sql, AiSchemaContext context, List<String> unknownTables) {

    Set<String> cteNames = extractCteNames(sql);
    Map<String, AiEntitySchema> aliasMap = new LinkedHashMap<>();

    Matcher m = FROM_JOIN_PATTERN.matcher(sql);
    while (m.find()) {
      String tableName = m.group(1);
      String alias = m.group(2);

      String lowerTable = tableName.toLowerCase(Locale.ROOT);

      if (cteNames.contains(lowerTable)) continue;

      if (SQL_KEYWORDS.contains(tableName.toUpperCase(Locale.ROOT))) continue;

      Optional<AiEntitySchema> schema = context.findByTable(tableName);
      if (schema.isEmpty()) {
        if (unknownTables != null) unknownTables.add(tableName);
        continue;
      }

      AiEntitySchema entitySchema = schema.get();

      aliasMap.put(lowerTable, entitySchema);

      if (alias != null && !alias.isBlank()) {
        String lowerAlias = alias.toLowerCase(Locale.ROOT);
        if (!SQL_KEYWORDS.contains(lowerAlias.toUpperCase(Locale.ROOT))) {
          aliasMap.put(lowerAlias, entitySchema);
        }
      }
    }

    return aliasMap;
  }

  /**
   * Extracts all CTE names declared in {@code WITH} clauses (lowercase).
   *
   * <p>Used by both consumers to identify references that must not be validated against the schema
   * (CTE columns are only knowable at runtime).
   *
   * @param sql the SQL string to scan
   * @return lowercase set of CTE names; empty if none
   */
  public static Set<String> extractCteNames(String sql) {
    Set<String> names = new HashSet<>();
    Matcher m = CTE_PATTERN.matcher(sql);
    while (m.find()) {
      names.add(m.group(1).toLowerCase(Locale.ROOT));
    }
    return names;
  }
}
