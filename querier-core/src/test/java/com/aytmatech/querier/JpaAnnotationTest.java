package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.JpaProduct;
import org.junit.jupiter.api.Test;

/** Tests for JPA annotation support. */
class JpaAnnotationTest {

  @Test
  void testJpaTableAnnotation() {
    Select select =
        Select.builder()
            .select(JpaProduct::getId)
            .select(JpaProduct::getProductName)
            .from(JpaProduct.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT jpa_products.id, jpa_products.product_name FROM jpa_products", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJpaAnnotationWithCondition() {
    Select select =
        Select.builder()
            .select(JpaProduct::getId)
            .select(JpaProduct::getProductName)
            .from(JpaProduct.class)
            .where(Condition.eq(JpaProduct::getProductName, "Test Product"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("WHERE jpa_products.product_name = :param0"));
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue("Test Product"));
  }

  @Test
  void testJpaAnnotationFallbackToSnakeCase() {
    Select select =
        Select.builder().select(JpaProduct::getDescription).from(JpaProduct.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT jpa_products.description FROM jpa_products", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJpaAnnotationWithAggregate() {
    Select select =
        Select.builder()
            .select(Aggregate.sum(JpaProduct::getPrice).as("total_price"))
            .from(JpaProduct.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT SUM(jpa_products.price) AS total_price FROM jpa_products", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJpaAnnotationWithOrderBy() {
    Select select =
        Select.builder()
            .select(JpaProduct::getId)
            .select(JpaProduct::getProductName)
            .from(JpaProduct.class)
            .orderBy(OrderBy.desc(JpaProduct::getProductName))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT jpa_products.id, jpa_products.product_name FROM jpa_products ORDER BY jpa_products.product_name DESC",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }
}
