package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for different SQL output formats (positional, indexed). */
class OutputFormatTest {

  @Test
  void testToPositionalSqlBasic() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select.PositionalSqlAndParams result = select.toPositionalSql();

    assertTrue(
        result
            .sql()
            .contains("SELECT orders.id, orders.total FROM orders WHERE orders.status = ?"));
    assertEquals(1, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
  }

  @Test
  void testToPositionalSqlMultipleParams() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .build();

    Select.PositionalSqlAndParams result = select.toPositionalSql();

    assertTrue(result.sql().contains("?"));
    assertFalse(result.sql().contains(":param"));
    assertEquals(2, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
    assertEquals(new BigDecimal("100"), result.params().get(1));
  }

  @Test
  void testToPositionalSqlNoParams() {
    Select select =
        Select.builder().select(Order::getId).select(Order::getTotal).from(Order.class).build();

    Select.PositionalSqlAndParams result = select.toPositionalSql();

    assertEquals("SELECT orders.id, orders.total FROM orders", result.sql());
    assertTrue(result.params().isEmpty());
  }

  @Test
  void testToPositionalSqlWithJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select.PositionalSqlAndParams result = select.toPositionalSql();

    assertTrue(result.sql().contains("?"));
    assertFalse(result.sql().contains(":param"));
    assertEquals(1, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
  }

  @Test
  void testToPositionalSqlWithIn() {
    List<OrderStatus> statuses = Arrays.asList(OrderStatus.PAID, OrderStatus.SHIPPED);
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.in(Order::getStatus, statuses))
            .build();

    Select.PositionalSqlAndParams result = select.toPositionalSql();

    assertTrue(result.sql().contains("?"));
    assertFalse(result.sql().contains(":param"));
    assertEquals(1, result.params().size());
    assertEquals(statuses, result.params().get(0));
  }

  @Test
  void testToIndexedSqlBasic() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .build();

    Select.IndexedSqlAndParams result = select.toIndexedSql();

    assertTrue(
        result
            .sql()
            .contains("SELECT orders.id, orders.total FROM orders WHERE orders.status = $1"));
    assertEquals(1, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
  }

  @Test
  void testToIndexedSqlMultipleParams() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .build();

    Select.IndexedSqlAndParams result = select.toIndexedSql();

    assertTrue(result.sql().contains("$1"));
    assertTrue(result.sql().contains("$2"));
    assertFalse(result.sql().contains(":param"));
    assertEquals(2, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
    assertEquals(new BigDecimal("100"), result.params().get(1));
  }

  @Test
  void testToIndexedSqlNoParams() {
    Select select =
        Select.builder().select(Order::getId).select(Order::getTotal).from(Order.class).build();

    Select.IndexedSqlAndParams result = select.toIndexedSql();

    assertEquals("SELECT orders.id, orders.total FROM orders", result.sql());
    assertTrue(result.params().isEmpty());
  }

  @Test
  void testToIndexedSqlComplexQuery() {
    Aggregate totalRevenue = Aggregate.sum(Order::getTotal);
    Select select =
        Select.builder()
            .select(Customer::getId)
            .select(Customer::getName)
            .select(Customer::getEmail)
            .select(Order::getStatus)
            .select(totalRevenue.as("total_revenue"))
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("50"))))
            .groupBy(Order::getStatus)
            .groupBy(Customer::getId)
            .having(Condition.gt(totalRevenue, new BigDecimal("1000")))
            .build();

    Select.IndexedSqlAndParams result = select.toIndexedSql();

    System.out.println(select.toPlainSql());

    assertTrue(result.sql().contains("$1"));
    assertTrue(result.sql().contains("$2"));
    assertTrue(result.sql().contains("$3"));
    assertFalse(result.sql().contains(":param"));
    assertEquals(3, result.params().size());
    assertEquals(OrderStatus.PAID, result.params().get(0));
    assertEquals(new BigDecimal("50"), result.params().get(1));
    assertEquals(new BigDecimal("1000"), result.params().get(2));
  }

  @Test
  void testPositionalAndIndexedParamOrder() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .build();

    Select.PositionalSqlAndParams positional = select.toPositionalSql();
    Select.IndexedSqlAndParams indexed = select.toIndexedSql();

    assertEquals(positional.params(), indexed.params());
  }

  @Test
  void testAllOutputFormatsConsistency() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, new BigDecimal("100"))))
            .build();

    Select.SqlAndParams named = select.toSqlAndParams();
    Select.PositionalSqlAndParams positional = select.toPositionalSql();
    Select.IndexedSqlAndParams indexed = select.toIndexedSql();
    String plain = select.toPlainSql();

    assertEquals(2, named.params().size());
    assertEquals(2, positional.params().size());
    assertEquals(2, indexed.params().size());

    assertTrue(named.params().containsValue(OrderStatus.PAID));
    assertTrue(named.params().containsValue(new BigDecimal("100")));
    assertEquals(OrderStatus.PAID, positional.params().get(0));
    assertEquals(new BigDecimal("100"), positional.params().get(1));
    assertEquals(OrderStatus.PAID, indexed.params().get(0));
    assertEquals(new BigDecimal("100"), indexed.params().get(1));

    assertTrue(plain.contains("'PAID'"));
    assertTrue(plain.contains("100"));
    assertFalse(plain.contains(":param"));
    assertFalse(plain.contains("?"));
    assertFalse(plain.contains("$1"));
  }
}
