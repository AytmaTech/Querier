package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import org.junit.jupiter.api.Test;

/** Tests for subqueries. */
class SubQueryTest {

  @Test
  void testSubqueryInWhere() {
    Select subquery =
        Select.builder()
            .select(Aggregate.avg(Order::getTotal).as("avg_total"))
            .from(Order.class)
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertTrue(
        sp.sql()
            .contains("WHERE orders.total > (SELECT AVG(orders.total) AS avg_total FROM orders)"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testExistsSubquery() {
    Select subquery =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getCustomerId, 123L))
            .build();

    Select main =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.exists(subquery))
            .build();

    Select.SqlAndParams sp = main.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders"
            + " WHERE EXISTS (SELECT orders.id FROM orders WHERE orders.customer_id = :param0)",
        sp.sql());
    assertEquals(1, sp.params().size());
  }
}
