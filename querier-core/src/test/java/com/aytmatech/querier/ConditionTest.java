package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for Condition class. */
class ConditionTest {

  @Test
  void testEqualCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status = :param0", sp.sql());
    assertEquals(1, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
  }

  @Test
  void testNotEqualCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.ne(Order::getStatus, OrderStatus.CANCELLED))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status != :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testGreaterThanCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gt(Order::getTotal, new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.total > :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testLessThanCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.lt(Order::getTotal, new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.total < :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testGreaterThanOrEqualCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.gte(Order::getTotal, new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.total >= :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testLessThanOrEqualCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.lte(Order::getTotal, new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.total <= :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testLikeCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.like(Order::getStatus, "%PAID%"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status LIKE :param0", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testInCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.in(Order::getStatus, List.of(OrderStatus.PAID, OrderStatus.SHIPPED)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status IN (:param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testIsNullCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.isNull(Order::getStatus))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status IS NULL", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testIsNotNullCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.isNotNull(Order::getStatus))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE orders.status IS NOT NULL", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testBetweenCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.between(Order::getTotal, new BigDecimal("10"), new BigDecimal("100")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE orders.total BETWEEN :param0 AND :param1", sp.sql());
    assertEquals(2, sp.params().size());
  }

  @Test
  void testAndCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE (orders.status = :param0 AND orders.total > :param1)",
        sp.sql());
    assertEquals(2, sp.params().size());
  }

  @Test
  void testOrCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.or(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.eq(Order::getStatus, OrderStatus.SHIPPED)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE (orders.status = :param0 OR orders.status = :param1)",
        sp.sql());
    assertEquals(2, sp.params().size());
  }

  @Test
  void testNotCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.not(Condition.eq(Order::getStatus, OrderStatus.CANCELLED)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id FROM orders WHERE NOT (orders.status = :param0)", sp.sql());
    assertEquals(1, sp.params().size());
  }

  @Test
  void testComplexCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.or(
                        Condition.eq(Order::getStatus, OrderStatus.PAID),
                        Condition.eq(Order::getStatus, OrderStatus.SHIPPED)),
                    Condition.gt(Order::getTotal, new BigDecimal("50"))))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id FROM orders WHERE ((orders.status = :param0 OR orders.status = :param1) AND orders.total > :param2)",
        sp.sql());
    assertEquals(3, sp.params().size());
  }
}
