package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import org.junit.jupiter.api.Test;

/** Tests for QuoteStrategy functionality. */
class QuoteStrategyTest {

  @Test
  void testNoneStrategy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.NONE)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT orders.id, orders.total FROM orders", sp.sql());
  }

  @Test
  void testAnsiStrategy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT \"orders\".\"id\", \"orders\".\"total\" FROM \"orders\"", sp.sql());
  }

  @Test
  void testMySqlStrategy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT `orders`.`id`, `orders`.`total` FROM `orders`", sp.sql());
  }

  @Test
  void testSqlServerStrategy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.SQL_SERVER)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT [orders].[id], [orders].[total] FROM [orders]", sp.sql());
  }

  @Test
  void testQuotingWithJoins() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .join(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("\"orders\".\"id\""));
    assertTrue(sp.sql().contains("\"customers\".\"name\""));
    assertTrue(sp.sql().contains("FROM \"orders\""));
    assertTrue(sp.sql().contains("INNER JOIN \"customers\""));
    assertTrue(sp.sql().contains("\"orders\".\"customer_id\" = \"customers\".\"id\""));
  }

  @Test
  void testQuotingWithWhere() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT \"orders\".\"id\" FROM \"orders\" WHERE \"orders\".\"status\" = :param0", sp.sql());
  }

  @Test
  void testQuotingWithGroupBy() {
    Select select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("GROUP BY \"orders\".\"customer_id\""));
  }

  @Test
  void testQuotingWithOrderBy() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .orderBy(OrderBy.asc(Order::getTotal))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("ORDER BY \"orders\".\"total\" ASC"));
  }

  @Test
  void testQuotingWithHaving() {
    Select select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .having(Condition.gt(Order::getTotal, 1000))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT \"orders\".\"customer_id\", SUM(orders.total) AS total FROM \"orders\""
            + " GROUP BY \"orders\".\"customer_id\" HAVING \"orders\".\"total\" > :param0",
        sp.sql());
  }

  @Test
  void testQuotingWithMultipleConditions() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.and(
                    Condition.eq(Order::getStatus, OrderStatus.PAID),
                    Condition.gt(Order::getTotal, 100)))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT \"orders\".\"id\" FROM \"orders\""
            + " WHERE (\"orders\".\"status\" = :param0 AND \"orders\".\"total\" > :param1)",
        sp.sql());
  }

  @Test
  void testQuotingWithInCondition() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.in(
                    Order::getStatus,
                    java.util.Arrays.asList(OrderStatus.PAID, OrderStatus.SHIPPED)))
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT `orders`.`id` FROM `orders` WHERE `orders`.`status` IN (:param0)", sp.sql());
  }

  @Test
  void testQuotingWithLike() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .from(Customer.class)
            .where(Condition.like(Customer::getName, "John%"))
            .quoteStrategy(QuoteStrategy.SQL_SERVER)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT [customers].[id] FROM [customers] WHERE [customers].[name] LIKE :param0", sp.sql());
  }

  @Test
  void testQuotingWithBetween() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.between(Order::getTotal, 100, 500))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT \"orders\".\"id\" FROM \"orders\""
            + " WHERE \"orders\".\"total\" BETWEEN :param0 AND :param1",
        sp.sql());
  }

  @Test
  void testQuotingWithIsNull() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.isNull(Order::getCreatedAt))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("WHERE \"orders\".\"created_at\" IS NULL"));
  }

  @Test
  void testQuotingWithLeftJoin() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Customer::getName)
            .from(Order.class)
            .leftJoin(Customer.class, Condition.eq(Order::getCustomerId, Customer::getId))
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("LEFT JOIN `customers`"));
    assertTrue(sp.sql().contains("`orders`.`customer_id` = `customers`.`id`"));
  }

  @Test
  void testQuotingEscaping() {
    assertEquals("\"test\"", QuoteStrategy.ANSI.quote("test"));
    assertEquals("\"test\"\"quote\"", QuoteStrategy.ANSI.quote("test\"quote"));

    assertEquals("`test`", QuoteStrategy.MYSQL.quote("test"));
    assertEquals("`test``tick`", QuoteStrategy.MYSQL.quote("test`tick"));

    assertEquals("[test]", QuoteStrategy.SQL_SERVER.quote("test"));
    assertEquals("[test]]bracket]", QuoteStrategy.SQL_SERVER.quote("test]bracket"));
  }

  @Test
  void testQuoteQualifiedName() {
    assertEquals("sales.orders", QuoteStrategy.NONE.quoteQualified("sales.orders"));
    assertEquals("\"sales\".\"orders\"", QuoteStrategy.ANSI.quoteQualified("sales.orders"));
    assertEquals("`sales`.`orders`", QuoteStrategy.MYSQL.quoteQualified("sales.orders"));
    assertEquals("[sales].[orders]", QuoteStrategy.SQL_SERVER.quoteQualified("sales.orders"));
    assertEquals(
        "\"mydb\".\"sales\".\"orders\"", QuoteStrategy.ANSI.quoteQualified("mydb.sales.orders"));
  }

  @Test
  void testDefaultQuoteStrategyIsNone() {
    Select select = Select.builder().select(Order::getId).from(Order.class).build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT orders.id FROM orders", sp.sql());
    assertFalse(sp.sql().contains("\""));
    assertFalse(sp.sql().contains("`"));
    assertFalse(sp.sql().contains("["));
  }

  @Test
  void testQuotingWithDistinct() {
    Select select =
        Select.builder()
            .distinct()
            .select(Order::getStatus)
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT DISTINCT \"orders\".\"status\" FROM \"orders\"", sp.sql());
  }

  @Test
  void testQuotingWithLimitOffset() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .limit(10)
            .offset(5)
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("SELECT `orders`.`id` FROM `orders` LIMIT 10 OFFSET 5"));
  }

  @Test
  void testQuotingWithSetOperations() {
    Select select1 =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select select2 =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(Order::getStatus, OrderStatus.SHIPPED))
            .union(select1)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select2.toSqlAndParams();

    assertTrue(sp.sql().contains("\"orders\".\"id\""));
    assertTrue(sp.sql().contains("UNION"));
    assertTrue(sp.sql().contains("WHERE \"orders\".\"status\""));
  }
}
