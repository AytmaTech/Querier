package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.Product;
import org.junit.jupiter.api.Test;

/** Tests for JOIN operations. */
class JoinTest {

  @Test
  void testInnerJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, customers.name FROM orders INNER JOIN customers ON orders.customer_id = customers.id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testLeftJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .leftJoin(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, customers.name FROM orders LEFT JOIN customers ON orders.customer_id = customers.id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testRightJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .rightJoin(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, customers.name FROM orders RIGHT JOIN customers ON orders.customer_id = customers.id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testFullOuterJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .fullJoin(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals(
        "SELECT orders.id, customers.name FROM orders FULL OUTER JOIN customers ON orders.customer_id = customers.id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testCrossJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Product::getName)
            .from(Order.class)
            .crossJoin(Product.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertEquals("SELECT orders.id, products.name FROM orders CROSS JOIN products", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testMultipleJoins() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .select(Product::getName)
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .join(Product.class, Condition.eq(Order::getId, Product::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("FROM orders"));
    assertTrue(sp.sql().contains("INNER JOIN customers ON orders.customer_id = customers.id"));
    assertTrue(sp.sql().contains("INNER JOIN products ON orders.id = products.id"));
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void testJoinWithWhere() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .where(Condition.like(Customer::getName, "John%"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("INNER JOIN customers ON orders.customer_id = customers.id"));
    assertEquals(
        "SELECT orders.id, customers.name FROM orders"
            + " INNER JOIN customers ON orders.customer_id = customers.id"
            + " WHERE customers.name LIKE :param0",
        sp.sql());
    assertEquals(1, sp.params().size());
  }
}
