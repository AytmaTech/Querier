package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import org.junit.jupiter.api.Test;

/** Tests for CaseWhen used with Aggregate functions and ORDER BY. */
class CaseWhenAggregateOrderByTest {

  @Test
  void testSumWithCaseWhen() {
    Select select =
        Select.builder()
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 100)
                            .orElse(0))
                    .as("paid_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("SUM(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" THEN :"));
    assertTrue(sp.sql().contains(" ELSE :"));
    assertTrue(sp.sql().contains(" END) AS paid_total"));
    assertEquals(3, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue(100));
    assertTrue(sp.params().containsValue(0));
  }

  @Test
  void testMultipleAggregatesWithCaseWhen() {
    Select select =
        Select.builder()
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 1)
                            .orElse(0))
                    .as("paid_total"))
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), 1)
                            .orElse(0))
                    .as("shipped_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("SUM(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS paid_total"));
    assertTrue(sp.sql().contains(" END) AS shipped_total"));
    assertEquals(6, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue(OrderStatus.SHIPPED));
  }

  @Test
  void testCountWithCaseWhen() {
    Select select =
        Select.builder()
            .select(
                Aggregate.count(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 1))
                    .as("paid_count"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("COUNT(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS paid_count"));
    assertFalse(sp.sql().contains(" ELSE "));
    assertEquals(2, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue(1));
  }

  @Test
  void testCaseWhenInOrderByAscending() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .select(Customer::getName)
            .from(Customer.class)
            .orderBy(
                OrderBy.asc(
                    CaseWhen.builder()
                        .when(Condition.isNull(Customer::getName), "zzz")
                        .orElse("aaa")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("ORDER BY CASE WHEN customers.name IS NULL THEN :"));
    assertTrue(sp.sql().contains(" ELSE :"));
    assertTrue(sp.sql().contains(" END ASC"));
    assertEquals(2, sp.params().size());
    assertTrue(sp.params().containsValue("zzz"));
    assertTrue(sp.params().containsValue("aaa"));
  }

  @Test
  void testCaseWhenInOrderByDescending() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .orderBy(
                OrderBy.desc(
                    CaseWhen.builder()
                        .when(Condition.isNull(Customer::getName), "zzz")
                        .orElse("aaa")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("ORDER BY CASE WHEN customers.name IS NULL THEN :"));
    assertTrue(sp.sql().contains(" END DESC"));
    assertEquals(2, sp.params().size());
    assertTrue(sp.params().containsValue("zzz"));
    assertTrue(sp.params().containsValue("aaa"));
  }

  @Test
  void testCaseWhenInOrderByWithOtherOrderByItems() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .orderBy(
                OrderBy.asc(Customer::getId),
                OrderBy.desc(
                    CaseWhen.builder()
                        .when(Condition.isNull(Customer::getName), "zzz")
                        .orElse("aaa")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql().contains("ORDER BY customers.id ASC, CASE WHEN customers.name IS NULL THEN :"));
    assertTrue(sp.sql().contains(" END DESC"));
    assertEquals(2, sp.params().size());
  }

  @Test
  void testCombinedAggregateWithCaseWhenAndOrderByCaseWhen() {
    Select select =
        Select.builder()
            .select(Order::getStatus)
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 1)
                            .orElse(0))
                    .as("paid_count"))
            .from(Order.class)
            .groupBy(Order::getStatus)
            .orderBy(
                OrderBy.desc(
                    CaseWhen.builder()
                        .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 1)
                        .orElse(0)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("SUM(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS paid_count"));
    assertTrue(sp.sql().contains("ORDER BY CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END DESC"));
    assertEquals(6, sp.params().size());
  }

  @Test
  void testPositionalSqlWithCaseWhenInAggregate() {
    Select select =
        Select.builder()
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 100)
                            .orElse(0))
                    .as("paid_total"))
            .from(Order.class)
            .build();

    Select.PositionalSqlAndParams psp = select.toPositionalSql();

    assertTrue(psp.sql().contains("SUM(CASE WHEN orders.status = ?"));
    assertTrue(psp.sql().contains(" THEN ?"));
    assertTrue(psp.sql().contains(" ELSE ?"));
    assertTrue(psp.sql().contains(" END) AS paid_total"));
    assertEquals(3, psp.params().size());
    assertTrue(psp.params().contains(OrderStatus.PAID));
    assertTrue(psp.params().contains(100));
    assertTrue(psp.params().contains(0));
  }

  @Test
  void testIndexedSqlWithCaseWhenInOrderBy() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .orderBy(
                OrderBy.asc(
                    CaseWhen.builder()
                        .when(Condition.isNull(Customer::getName), "zzz")
                        .orElse("aaa")))
            .build();

    Select.IndexedSqlAndParams isp = select.toIndexedSql();

    assertTrue(isp.sql().contains("ORDER BY CASE WHEN customers.name IS NULL THEN $"));
    assertTrue(isp.sql().contains(" ELSE $"));
    assertTrue(isp.sql().contains(" END ASC"));
    assertEquals(2, isp.params().size());
    assertTrue(isp.params().contains("zzz"));
    assertTrue(isp.params().contains("aaa"));
  }

  @Test
  void testPlainSqlWithCaseWhenInAggregate() {
    Select select =
        Select.builder()
            .select(
                Aggregate.sum(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 100)
                            .orElse(0))
                    .as("paid_total"))
            .from(Order.class)
            .build();

    String plainSql = select.toPlainSql();

    assertTrue(plainSql.contains("SUM(CASE WHEN orders.status = 'PAID'"));
    assertTrue(plainSql.contains(" THEN 100"));
    assertTrue(plainSql.contains(" ELSE 0"));
    assertTrue(plainSql.contains(" END) AS paid_total"));
  }

  @Test
  void testAvgMinMaxWithCaseWhen() {
    Select select =
        Select.builder()
            .select(
                Aggregate.avg(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 10)
                            .orElse(0))
                    .as("avg_val"))
            .select(
                Aggregate.min(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 10)
                            .orElse(0))
                    .as("min_val"))
            .select(
                Aggregate.max(
                        CaseWhen.builder()
                            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 10)
                            .orElse(0))
                    .as("max_val"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("AVG(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS avg_val"));
    assertTrue(sp.sql().contains("MIN(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS min_val"));
    assertTrue(sp.sql().contains("MAX(CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END) AS max_val"));
    assertEquals(9, sp.params().size());
  }

  @Test
  void testExistingAggregateBackwardCompatibility() {
    Select select =
        Select.builder()
            .select(Aggregate.sum(Order::getTotal).as("total_sum"))
            .select(Aggregate.count(Order::getId).as("order_count"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT SUM(orders.total) AS total_sum, COUNT(orders.id) AS order_count FROM orders",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testExistingOrderByBackwardCompatibility() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .orderBy(OrderBy.asc(Customer::getId), OrderBy.desc(Customer::getName))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("ORDER BY customers.id ASC, customers.name DESC"));
    assertTrue(sp.params().isEmpty());
  }
}
