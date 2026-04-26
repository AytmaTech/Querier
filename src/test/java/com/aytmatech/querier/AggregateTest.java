package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/** Tests for aggregate functions and GROUP BY. */
class AggregateTest {

  @Test
  void testCount() {
    Select select =
        Select.builder().select(Aggregate.count(Order::getId)).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT COUNT(orders.id) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCountAll() {
    Select select = Select.builder().select(Aggregate.countAll()).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT COUNT(*) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCountDistinct() {
    Select select =
        Select.builder()
            .select(Aggregate.countDistinct(Order::getCustomerId))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT COUNT(DISTINCT orders.customer_id) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSum() {
    Select select =
        Select.builder().select(Aggregate.sum(Order::getTotal)).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT SUM(orders.total) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testAvg() {
    Select select =
        Select.builder().select(Aggregate.avg(Order::getTotal)).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT AVG(orders.total) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testMin() {
    Select select =
        Select.builder().select(Aggregate.min(Order::getTotal)).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT MIN(orders.total) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testMax() {
    Select select =
        Select.builder().select(Aggregate.max(Order::getTotal)).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT MAX(orders.total) FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testAggregateWithAlias() {
    Select select =
        Select.builder()
            .select(Aggregate.sum(Order::getTotal).as("total_revenue"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT SUM(orders.total) AS total_revenue FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testGroupBy() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getStatus)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.status, SUM(orders.total) AS total FROM orders GROUP BY orders.status",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testGroupByMultipleColumns() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(Order::getCustomerId)
            .select(Aggregate.count(Order::getId))
            .from(Order.class)
            .groupBy(Order::getStatus, Order::getCustomerId)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("GROUP BY orders.status, orders.customer_id"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testHaving() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getStatus)
            .having(Condition.gt(Order::getTotal, new BigDecimal("1000")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("GROUP BY orders.status"));
    assertEquals(
        "SELECT orders.status, SUM(orders.total) AS total FROM orders"
            + " GROUP BY orders.status HAVING orders.total > :param0",
        sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testOrderByAggregate() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getStatus)
            .orderBy(OrderBy.desc("SUM(orders.total)"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("GROUP BY orders.status"));
    assertTrue(sp.sql().contains("ORDER BY SUM(orders.total) DESC"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testComplexAggregateQuery() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(Aggregate.count(Order::getId).as("count"))
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .where(Condition.ne(Order::getStatus, OrderStatus.CANCELLED))
            .groupBy(Order::getStatus)
            .having(Condition.gt(Order::getTotal, new BigDecimal("500")))
            .orderBy(OrderBy.desc("SUM(orders.total)"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains(
                "SELECT orders.status, COUNT(orders.id) AS count, SUM(orders.total) AS total"));
    assertTrue(sp.sql().contains("WHERE orders.status"));
    assertTrue(sp.sql().contains("GROUP BY orders.status"));
    assertTrue(sp.sql().contains("HAVING orders.total"));
    assertTrue(sp.sql().contains("ORDER BY SUM(orders.total) DESC"));
    assertEquals(2, sp.params().size());
  }
}
