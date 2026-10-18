package com.aytmatech.querier.ai.sql;

import com.aytmatech.querier.Select;
import com.aytmatech.querier.ai.model.AiEntitySchema;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import java.util.Map;

/**
 * Converts a validated SQL string into a Querier {@link Select} anchored to the correct entity
 * class.
 *
 * <p>Must only be called <em>after</em> {@link SqlSchemaValidator#validate} returns an empty issues
 * list — this class trusts that the SQL is schema-valid.
 *
 * <p>Entity resolution is delegated to {@link SqlAliasResolver} (same logic used by the validator)
 * so both classes always agree on which entity is primary.
 */
public final class SqlToSelectParser {

  private SqlToSelectParser() {}

  /**
   * Parses the SQL and constructs a {@link Select} anchored to the primary entity.
   *
   * <p>The primary entity is the first schema table encountered in the SQL's FROM clause (insertion
   * order of the {@link SqlAliasResolver} result). This gives the executor the entity class it
   * needs for typed result mapping.
   *
   * <p>If no schema entity can be matched (only possible for pure expression queries like {@code
   * SELECT 1+1} — not expected in normal AI usage), the method falls back to a raw untyped query.
   *
   * @param sql validated SQL string
   * @param context schema context for entity class resolution
   * @return a {@link Select} ready to execute
   */
  public static Select parse(String sql, AiSchemaContext context) {
    Map<String, AiEntitySchema> aliasMap = SqlAliasResolver.resolve(sql, context, null);

    AiEntitySchema primaryEntity = aliasMap.values().stream().findFirst().orElse(null);

    if (primaryEntity != null) {
      return Select.builder().from(primaryEntity.entityClass()).rawSql(sql).build();
    }

    return Select.rawQuery(sql);
  }
}
