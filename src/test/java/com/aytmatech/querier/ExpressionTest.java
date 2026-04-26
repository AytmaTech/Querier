package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import com.aytmatech.querier.model.Product;
import org.junit.jupiter.api.Test;

/** Tests for raw expressions. */
class ExpressionTest {

  @Test
  void testRawExpression() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Expression.raw("UPPER(orders.status)").as("status_upper"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql().contains("SELECT orders.id, UPPER(orders.status) AS status_upper FROM orders"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCTE() {
    Select cte =
        Select.builder()
            .select(Order::getCustomerId)
            .select(Aggregate.sum(Order::getTotal).as("total_spent"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .build();

    Select main =
        Select.builder()
            .with("customer_totals", cte)
            .select(Expression.raw("customer_totals.customer_id"))
            .select(Expression.raw("customer_totals.total_spent"))
            .from(Expression.tableRef("customer_totals"))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(
        sp.sql()
            .startsWith(
                "WITH customer_totals AS (SELECT orders.customer_id, SUM(orders.total) AS total_spent FROM orders GROUP BY orders.customer_id)"));
    assertTrue(
        sp.sql()
            .contains(
                "SELECT customer_totals.customer_id, customer_totals.total_spent FROM customer_totals"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCTEWithWhere() {
    Select cte =
        Select.builder()
            .select(Order::getCustomerId)
            .select(Aggregate.sum(Order::getTotal).as("total_spent"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .build();

    Select main =
        Select.builder()
            .with("customer_totals", cte)
            .select(Expression.raw("customer_totals.customer_id"))
            .select(Expression.raw("customer_totals.total_spent"))
            .from(Expression.tableRef("customer_totals"))
            .where(Condition.gt(Expression.raw("customer_totals.total_spent"), 5000))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(sp.sql().contains("WITH customer_totals AS"));
    assertTrue(sp.sql().contains("WHERE customer_totals.total_spent > :param0"));
    assertEquals(1, sp.params().size());
  }

  @Test
  void testUnion() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select select2 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.SHIPPED))
            .union(select1)
            .build();

    Select unionSelect =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PENDING))
            .union(select2)
            .build();

    Select.SqlAndParams sp = unionSelect.toSqlAndParams();

    assertTrue(sp.sql().contains("SELECT orders.id, orders.total FROM orders"));
    assertTrue(sp.sql().contains("UNION"));
    assertTrue(sp.sql().contains("WHERE"));

    String sql = sp.sql();
    int firstWhere = sql.indexOf("WHERE");
    int unionPos = sql.indexOf("UNION");
    int secondWhere = sql.indexOf("WHERE", unionPos);
    assertTrue(firstWhere > 0 && firstWhere < unionPos);
    assertTrue(secondWhere > unionPos);
    assertEquals(3, sp.params().size());
  }

  @Test
  void testUnionAll() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, 1000))
            .build();

    Select unionAllSelect =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.lt(Order::getTotal, 100))
            .unionAll(select1)
            .build();

    Select.SqlAndParams sp = unionAllSelect.toSqlAndParams();

    assertTrue(sp.sql().contains("SELECT orders.id, orders.total FROM orders"));
    assertTrue(sp.sql().contains("UNION ALL"));
    assertTrue(sp.sql().contains("WHERE"));

    String sql = sp.sql();
    int firstWhere = sql.indexOf("WHERE");
    int unionPos = sql.indexOf("UNION ALL");
    int secondWhere = sql.indexOf("WHERE", unionPos);
    assertTrue(firstWhere > 0 && firstWhere < unionPos);
    assertTrue(secondWhere > unionPos);
    assertEquals(2, sp.params().size());
    assertTrue(sp.params().containsValue(100));
    assertTrue(sp.params().containsValue(1000));
  }

  @Test
  void testIntersect() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select intersectSelect =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, 500))
            .intersect(select1)
            .build();

    Select.SqlAndParams sp = intersectSelect.toSqlAndParams();

    assertTrue(sp.sql().contains("SELECT orders.id, orders.total FROM orders"));
    assertTrue(sp.sql().contains("INTERSECT"));
    assertTrue(sp.sql().contains("WHERE"));

    String sql = sp.sql();
    int firstWhere = sql.indexOf("WHERE");
    int intersectPos = sql.indexOf("INTERSECT");
    int secondWhere = sql.indexOf("WHERE", intersectPos);
    assertTrue(firstWhere > 0 && firstWhere < intersectPos);
    assertTrue(secondWhere > intersectPos);
    assertEquals(2, sp.params().size());
  }

  @Test
  void testExcept() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.CANCELLED))
            .build();

    Select exceptSelect =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PENDING))
            .except(select1)
            .build();

    Select.SqlAndParams sp = exceptSelect.toSqlAndParams();

    assertTrue(sp.sql().contains("SELECT orders.id, orders.total FROM orders"));
    assertTrue(sp.sql().contains("EXCEPT"));
    assertTrue(sp.sql().contains("WHERE"));

    String sql = sp.sql();
    int firstWhere = sql.indexOf("WHERE");
    int exceptPos = sql.indexOf("EXCEPT");
    int secondWhere = sql.indexOf("WHERE", exceptPos);
    assertTrue(firstWhere > 0 && firstWhere < exceptPos);
    assertTrue(secondWhere > exceptPos);
    assertEquals(2, sp.params().size());
  }

  @Test
  void testCaseWhenExpression() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                Expression.raw("CASE WHEN orders.total > 100 THEN 'high' ELSE 'low' END")
                    .as("price_category"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains("CASE WHEN orders.total > 100 THEN 'high' ELSE 'low' END AS price_category"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testExpressionAlias() {
    Expression expr = Expression.raw("COUNT(*)").as("total_count");

    assertEquals("COUNT(*) AS total_count", expr.toSql());
    assertEquals("total_count", expr.getAlias());
  }

  @Test
  void testTableRefExpression() {
    Select select =
        Select.builder()
            .select(Expression.raw("t.id"))
            .select(Expression.raw("t.name"))
            .from(Expression.tableRef("my_table AS t"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT t.id, t.name FROM my_table AS t", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testMultipleCTEs() {
    Select cte1 =
        Select.builder()
            .select(Product::getId)
            .from(Product.class)
            .where(Condition.lte(Product::getPrice, 10))
            .build();

    Select cte2 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getCustomerId)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.DELIVERED),
                    Condition.in(
                        Order::getProductId,
                        Select.builder()
                            .select(Expression.raw("*"))
                            .from(Expression.tableRef("cheap_products"))
                            .build())))
            .build();

    Select main =
        Select.builder()
            .with("cheap_products", cte1)
            .with("cheap_delivered_orders", cte2)
            .select(Expression.raw("*"))
            .from(Customer.class)
            .where(
                Condition.in(
                    Customer::getId,
                    Select.builder()
                        .select(Expression.raw("customer_id"))
                        .from(Expression.tableRef("cheap_delivered_orders"))
                        .build()))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(sp.sql().startsWith("WITH cheap_products AS ("));
    assertTrue(sp.sql().contains(", cheap_delivered_orders AS ("));
    assertTrue(
        sp.sql()
            .contains(
                "SELECT * FROM customers WHERE customers.id IN (SELECT customer_id FROM cheap_delivered_orders)"));
    assertTrue(!sp.params().isEmpty());
  }

  @Test
  void testSetOperationWithWhereClause() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, 100))
            .build();

    Select unionSelect =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.lt(Order::getTotal, 50))
            .union(select1)
            .build();

    Select.SqlAndParams sp = unionSelect.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, orders.total FROM orders WHERE orders.total < :param0"
            + " UNION SELECT orders.id, orders.total FROM orders WHERE orders.total > :param1",
        sp.sql());
    assertEquals(2, sp.params().size());
    assertTrue(sp.params().containsValue(50));
    assertTrue(sp.params().containsValue(100));
  }

  @Test
  void testToPlainSql() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    String plainSql1 = select1.toPlainSql();
    assertTrue(plainSql1.contains("'PAID'"));
    assertFalse(plainSql1.contains(":param"));

    Select select2 =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, 100))
            .build();

    String plainSql2 = select2.toPlainSql();
    assertTrue(plainSql2.contains(" > 100"));
    assertFalse(plainSql2.contains(":param"));

    Select select3 =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.in(
                    Order::getStatus,
                    java.util.Arrays.asList(OrderStatus.PAID, OrderStatus.SHIPPED)))
            .build();

    String plainSql3 = select3.toPlainSql();
    assertTrue(plainSql3.contains("'PAID', 'SHIPPED'"));
    assertFalse(plainSql3.contains(":param"));

    Select select4 =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .where(Condition.like(Customer::getName, "O'Brien%"))
            .build();

    String plainSql4 = select4.toPlainSql();
    assertTrue(plainSql4.contains("'O''Brien%'"));
    assertFalse(plainSql4.contains(":param"));
  }
}
