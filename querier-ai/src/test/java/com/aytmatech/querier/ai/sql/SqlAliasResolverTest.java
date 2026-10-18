package com.aytmatech.querier.ai.sql;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.model.AiEntitySchema;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import com.aytmatech.querier.annotation.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SqlAliasResolverTest {

  @Table("customers")
  private static class Customer {
    public String getName() {
      return null;
    }

    public String getEmail() {
      return null;
    }
  }

  private static final AiSchemaContext CTX = AiSchemaInspector.inspect(Customer.class);

  @Test
  void resolve_fromClauseNoAlias_mapsTableName() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve("SELECT * FROM customers", CTX, null);

    assertTrue(map.containsKey("customers"));
  }

  @Test
  void resolve_fromClauseWithAlias_mapsBothTableAndAlias() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve("SELECT c.name FROM customers c", CTX, null);

    assertTrue(map.containsKey("customers"));
    assertTrue(map.containsKey("c"));
  }

  @Test
  void resolve_fromClauseWithAsAlias_mapsBothTableAndAlias() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve("SELECT c.name FROM customers AS c", CTX, null);

    assertTrue(map.containsKey("customers"));
    assertTrue(map.containsKey("c"));
  }

  @Test
  void resolve_joinClause_mapsJoinedTable() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve(
            "SELECT c.name FROM customers c JOIN customers c2 ON c.name = c2.name", CTX, null);

    assertTrue(map.containsKey("c2"));
  }

  @Test
  void resolve_unknownTable_addedToUnknownList() {
    List<String> unknown = new ArrayList<>();
    SqlAliasResolver.resolve("SELECT o.id FROM orders o", CTX, unknown);

    assertTrue(unknown.contains("orders"));
  }

  @Test
  void resolve_cteNameExcluded_notInAliasMap() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve(
            "WITH mycte AS (SELECT name FROM customers) SELECT mycte.name FROM mycte", CTX, null);

    assertFalse(map.containsKey("mycte"));
  }

  @Test
  void resolve_nullUnknownTablesList_doesNotThrow() {
    assertDoesNotThrow(() -> SqlAliasResolver.resolve("SELECT * FROM unknown_table", CTX, null));
  }

  @Test
  void resolve_keysAreLowercase() {
    Map<String, AiEntitySchema> map =
        SqlAliasResolver.resolve("SELECT c.name FROM customers c", CTX, null);

    map.keySet().forEach(k -> assertEquals(k, k.toLowerCase()));
  }

  @Test
  void extractCteNames_singleCte_returnsName() {
    Set<String> names =
        SqlAliasResolver.extractCteNames(
            "WITH summary AS (SELECT name FROM customers) SELECT * FROM summary");

    assertTrue(names.contains("summary"));
  }

  @Test
  void extractCteNames_noCte_returnsEmptySet() {
    Set<String> names = SqlAliasResolver.extractCteNames("SELECT * FROM customers");
    assertTrue(names.isEmpty());
  }

  @Test
  void extractCteNames_multipleCtes_returnsAllNames() {
    String sql = "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b ON true";
    Set<String> names = SqlAliasResolver.extractCteNames(sql);

    assertTrue(names.contains("a"));
    assertTrue(names.contains("b"));
  }
}
