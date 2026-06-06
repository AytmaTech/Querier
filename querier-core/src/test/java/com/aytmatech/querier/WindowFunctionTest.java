package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import org.junit.jupiter.api.Test;

/** Tests for window functions. */
class WindowFunctionTest {

  @Test
  void testWindowFunctionBasic() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .select(
                WindowFunction.of(Aggregate.sum(Order::getTotal))
                    .partitionBy(Order::getCustomerId)
                    .as("customer_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains(
                "SUM(orders.total) OVER (PARTITION BY orders.customer_id) AS customer_total"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testWindowFunctionWithOrderBy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .select(
                WindowFunction.of(Aggregate.sum(Order::getTotal))
                    .partitionBy(Order::getCustomerId)
                    .orderBy(OrderBy.asc(Order::getCreatedAt))
                    .as("running_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains(
                "SUM(orders.total) OVER (PARTITION BY orders.customer_id ORDER BY orders.created_at ASC) AS running_total"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testWindowFunctionCount() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                WindowFunction.of(Aggregate.count(Order::getId))
                    .partitionBy(Order::getCustomerId)
                    .as("customer_order_count"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains(
                "COUNT(orders.id) OVER (PARTITION BY orders.customer_id) AS customer_order_count"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testWindowFunctionAvg() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                WindowFunction.of(Aggregate.avg(Order::getTotal))
                    .partitionBy(Order::getCustomerId)
                    .as("avg_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql().contains("AVG(orders.total) OVER (PARTITION BY orders.customer_id) AS avg_total"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testMultipleWindowFunctions() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .select(
                WindowFunction.of(Aggregate.sum(Order::getTotal))
                    .partitionBy(Order::getCustomerId)
                    .as("customer_total"))
            .select(
                WindowFunction.of(Aggregate.count(Order::getId))
                    .partitionBy(Order::getCustomerId)
                    .as("customer_order_count"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains(
                "SUM(orders.total) OVER (PARTITION BY orders.customer_id) AS customer_total"));
    assertTrue(
        sp.sql()
            .contains(
                "COUNT(orders.id) OVER (PARTITION BY orders.customer_id) AS customer_order_count"));
    assertTrue(sp.params().isEmpty());
  }
}
