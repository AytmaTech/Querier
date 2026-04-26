package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/** Tests for basic SELECT queries. */
class SelectTest {

  @Test
  void testBasicSelect() {
    Select select =
        Select.builder().select(Order::getId).select(Order::getTotal).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, orders.total FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSelectAll() {
    Select select = Select.builder().from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT * FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSelectWithWhere() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, orders.total FROM orders WHERE orders.status = :param0", sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
  }

  @Test
  void testSelectWithOrderBy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .orderBy(OrderBy.desc(Order::getTotal))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, orders.total FROM orders ORDER BY orders.total DESC", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSelectWithLimit() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .limit(10)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, orders.total FROM orders LIMIT 10", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSelectWithOffset() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .limit(10)
            .offset(20)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, orders.total FROM orders LIMIT 10 OFFSET 20", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testSelectDistinct() {
    Select select = Select.builder().distinct().select(Order::getStatus).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT DISTINCT orders.status FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testComplexQuery() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .orderBy(OrderBy.desc(Order::getTotal), OrderBy.asc(Order::getId))
            .limit(10)
            .offset(5)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, orders.total FROM orders"
            + " WHERE (orders.status = :param0 AND orders.total > :param1)"
            + " ORDER BY orders.total DESC, orders.id ASC LIMIT 10 OFFSET 5",
        sp.sql());
    assertEquals(2, sp.params().size());
  }
}
