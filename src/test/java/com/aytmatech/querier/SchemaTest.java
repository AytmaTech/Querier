package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.*;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/** Tests for schema and catalog support in table names. */
class SchemaTest {

  @Test
  void testCustomTableWithSchemaOnly() {
    Select select =
        Select.builder()
            .select(SchemaOrder::getId)
            .select(SchemaOrder::getTotal)
            .from(SchemaOrder.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT sales.orders.id, sales.orders.total FROM sales.orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCustomTableWithCatalogAndSchema() {
    Select select =
        Select.builder()
            .select(CatalogSchemaOrder::getId)
            .select(CatalogSchemaOrder::getTotal)
            .from(CatalogSchemaOrder.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT mydb.sales.orders.id, mydb.sales.orders.total FROM mydb.sales.orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCustomTableWithCatalogOnly() {
    Select select =
        Select.builder()
            .select(CatalogOnlyOrder::getId)
            .select(CatalogOnlyOrder::getTotal)
            .from(CatalogOnlyOrder.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT mydb.orders.id, mydb.orders.total FROM mydb.orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCustomTableWithNameOnlyBackwardCompat() {
    Select select =
        Select.builder().select(Order::getId).select(Order::getTotal).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, orders.total FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJpaTableWithSchema() {
    Select select =
        Select.builder()
            .select(JpaSchemaOrder::getId)
            .select(JpaSchemaOrder::getTotal)
            .from(JpaSchemaOrder.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT reporting.orders.id, reporting.orders.total FROM reporting.orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJpaTableWithCatalogAndSchema() {
    Select select =
        Select.builder()
            .select(JpaCatalogSchemaOrder::getId)
            .select(JpaCatalogSchemaOrder::getTotal)
            .from(JpaCatalogSchemaOrder.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT analytics_db.reporting.orders.id, analytics_db.reporting.orders.total FROM analytics_db.reporting.orders",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testNoAnnotationDefaultBehavior() {
    Select select =
        Select.builder()
            .select(Product::getId)
            .select(Product::getName)
            .from(Product.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT products.id, products.name FROM products", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSchemaPropagationInSelectWithCondition() {
    Select select =
        Select.builder()
            .select(SchemaOrder::getId)
            .select(SchemaOrder::getTotal)
            .from(SchemaOrder.class)
            .where(Condition.gt(SchemaOrder::getTotal, new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .startsWith(
                "SELECT sales.orders.id, sales.orders.total FROM sales.orders WHERE sales.orders.total > :param"));
    assertEquals(1, sp.params().size());
  }

  @Test
  void testSchemaPropagationInJoin() {
    Select select =
        Select.builder()
            .select(SchemaOrder::getId)
            .select(Customer::getName)
            .from(SchemaOrder.class)
            .join(Customer.class, Condition.eq(SchemaOrder::getId, Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("FROM sales.orders"));
    assertTrue(sp.sql().contains("JOIN customers"));
    assertTrue(sp.sql().contains("ON sales.orders.id = customers.id"));
  }

  @Test
  void testMultipleSchemasInJoin() {
    Select select =
        Select.builder()
            .select(SchemaOrder::getId)
            .select(JpaSchemaOrder::getTotal)
            .from(SchemaOrder.class)
            .join(JpaSchemaOrder.class, Condition.eq(SchemaOrder::getId, JpaSchemaOrder::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("FROM sales.orders"));
    assertTrue(sp.sql().contains("JOIN reporting.orders"));
    assertTrue(sp.sql().contains("ON sales.orders.id = reporting.orders.id"));
  }

  @Test
  void testSchemaWithOrderBy() {
    Select select =
        Select.builder()
            .select(SchemaOrder::getId)
            .select(SchemaOrder::getTotal)
            .from(SchemaOrder.class)
            .orderBy(OrderBy.desc(SchemaOrder::getTotal))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT sales.orders.id, sales.orders.total FROM sales.orders ORDER BY sales.orders.total DESC",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }
}
