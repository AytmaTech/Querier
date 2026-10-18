package com.aytmatech.querier.ai.sql;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.Select;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import com.aytmatech.querier.annotation.Table;
import org.junit.jupiter.api.Test;

class SqlToSelectParserTest {

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
  void parse_simpleSelect_returnsNonNullSelect() {
    Select select = SqlToSelectParser.parse("SELECT * FROM customers", CTX);
    assertNotNull(select);
  }

  @Test
  void parse_simpleSelect_rawSqlIsSameAsInput() {
    String sql = "SELECT name FROM customers";
    Select select = SqlToSelectParser.parse(sql, CTX);

    assertEquals(sql, select.toSqlAndParams().sql());
  }

  @Test
  void parse_selectWithAlias_rawSqlIsSameAsInput() {
    String sql = "SELECT c.name FROM customers c";
    Select select = SqlToSelectParser.parse(sql, CTX);

    assertEquals(sql, select.toSqlAndParams().sql());
  }

  @Test
  void parse_pureExpressionQuery_returnsRawSelect() {
    // No known entity — falls back to raw untyped query
    Select select = SqlToSelectParser.parse("SELECT 1 + 1", CTX);
    assertNotNull(select);
    assertEquals("SELECT 1 + 1", select.toSqlAndParams().sql());
  }

  @Test
  void parse_rawSql_paramsAreEmpty() {
    Select select = SqlToSelectParser.parse("SELECT name FROM customers", CTX);
    assertTrue(select.toSqlAndParams().params().isEmpty());
  }
}
