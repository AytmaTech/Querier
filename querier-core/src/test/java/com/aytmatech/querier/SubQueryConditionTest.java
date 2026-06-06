package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for new Condition overloads: subquery and expression variants. */
class SubQueryConditionTest {

  @Test
  void testInColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("1000")))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.in(Order::getId, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.id IN (SELECT orders.id FROM orders WHERE orders.total > :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(new BigDecimal("1000")));
  }

  @Test
  void testNotInColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.CANCELLED))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.notIn(Order::getId, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.id NOT IN (SELECT orders.id FROM orders WHERE orders.status = :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.CANCELLED));
  }

  @Test
  void testNotInColumnRefCollection() {
    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.notIn(
                    Order::getStatus, List.of(OrderStatus.CANCELLED, OrderStatus.SHIPPED)))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status NOT IN (:param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testEqColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total = (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testNeColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.ne(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total != (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testLtColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.lt(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total < (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testGteColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gte(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total >= (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testLteColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.lte(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total <= (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testGtColumnRefSubquery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total > (SELECT AVG(orders.total) AS avg_total FROM orders)",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testInExpressionSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("500")))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.in(Expression.raw("orders.id"), subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.id IN (SELECT orders.id FROM orders WHERE orders.total > :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(new BigDecimal("500")));
  }

  @Test
  void testNotInExpressionSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.CANCELLED))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.notIn(Expression.raw("orders.id"), subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.id NOT IN (SELECT orders.id FROM orders WHERE orders.status = :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.CANCELLED));
  }

  @Test
  void testInExpressionCollection() {
    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.in(
                    Expression.raw("orders.status"),
                    List.of(OrderStatus.PAID, OrderStatus.SHIPPED)))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status IN (:param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testNotInExpressionCollection() {
    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.notIn(Expression.raw("orders.status"), List.of(OrderStatus.CANCELLED)))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status NOT IN (:param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testCteWithInCondition() {
    Select expensiveIds =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("1000")))
            .build();

    Select recentIds =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getCreatedAt, LocalDateTime.of(2024, 1, 1, 0, 0)))
            .build();

    Select main =
        Select.builder()
            .with("expensive_orders_ids", expensiveIds)
            .with("recent_orders_ids", recentIds)
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.or(
                    Condition.and(
                        Condition.eq(Order::getStatus, OrderStatus.PAID),
                        Condition.gt(Order::getTotal, new BigDecimal("500"))),
                    Condition.and(
                        Condition.in(
                            Order::getId,
                            Select.builder()
                                .select(Expression.raw("*"))
                                .from(Expression.tableRef("expensive_orders_ids"))
                                .build()),
                        Condition.in(
                            Order::getId,
                            Select.builder()
                                .select(Expression.raw("*"))
                                .from(Expression.tableRef("recent_orders_ids"))
                                .build()))))
            .orderBy(OrderBy.desc(Order::getTotal), OrderBy.asc(Order::getId))
            .limit(10)
            .offset(5)
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(
        sp.sql()
            .startsWith(
                "WITH expensive_orders_ids AS (SELECT orders.id FROM orders WHERE orders.total > :param0), recent_orders_ids AS (SELECT orders.id FROM orders WHERE orders.created_at > :param1)"));
    assertTrue(sp.sql().contains("orders.id IN (SELECT * FROM expensive_orders_ids)"));
    assertTrue(sp.sql().contains("orders.id IN (SELECT * FROM recent_orders_ids)"));
    assertTrue(sp.sql().contains("ORDER BY orders.total DESC, orders.id ASC"));
    assertTrue(sp.sql().contains("LIMIT 10 OFFSET 5"));
    assertEquals(4, sp.params().size());
  }

  @Test
  void testCombinedAndOrWithSubqueryIn() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("200")))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.in(Order::getId, subquery)))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE (orders.status = :param0 AND orders.id IN (SELECT orders.id FROM orders WHERE orders.total > :param1))",
        sp.sql());
    assertEquals(2, sp.params().size());
  }

  @Test
  void testPlainSqlWithSubqueryIn() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("300")))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.in(Order::getId, subquery))
            .build();

    String plain = main.toPlainSql();
    assertTrue(
        plain.contains("orders.id IN (SELECT orders.id FROM orders WHERE orders.total > 300)"));
  }

  @Test
  void testBackwardCompatibilityInCollection() {
    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.in(Order::getStatus, List.of(OrderStatus.PAID, OrderStatus.SHIPPED)))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status IN (:param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testBackwardCompatibilityExistsSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getCustomerId, 42L))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.exists(subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE EXISTS (SELECT orders.id FROM orders WHERE orders.customer_id = :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testBackwardCompatibilityGtSubQuery() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(
        sp.sql().contains("orders.total > (SELECT AVG(orders.total) AS avg_total FROM orders)"));
    assertTrue(sp.params().isEmpty());
  }
}
