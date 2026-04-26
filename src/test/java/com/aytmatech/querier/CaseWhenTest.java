package com.aytmatech.querier;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.model.Customer;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for CASE WHEN expressions. */
class CaseWhenTest {

  @Test
  void testBasicCaseWhenWithStringResults() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" THEN :"));
    assertTrue(sp.sql().contains(" ELSE :"));
    assertTrue(sp.sql().contains(" END AS status_label"));
    assertEquals(3, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue("Completed"));
    assertTrue(sp.params().containsValue("Pending"));
  }

  @Test
  void testMultipleWhenClauses() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), "In Transit")
                    .when(Condition.eq(Order::getStatus, OrderStatus.DELIVERED), "Delivered")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" END AS status_label"));
    assertEquals(7, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue(OrderStatus.SHIPPED));
    assertTrue(sp.params().containsValue(OrderStatus.DELIVERED));
    assertTrue(sp.params().containsValue("Completed"));
    assertTrue(sp.params().containsValue("In Transit"));
    assertTrue(sp.params().containsValue("Delivered"));
    assertTrue(sp.params().containsValue("Pending"));
  }

  @Test
  void testWithoutElseClause() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), 1)
                    .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), 2)
                    .as("status_code"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("CASE WHEN orders.status = :"));
    assertFalse(sp.sql().contains(" ELSE "));
    assertTrue(sp.sql().contains(" END AS status_code"));
    assertEquals(4, sp.params().size());
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue(OrderStatus.SHIPPED));
    assertTrue(sp.params().containsValue(1));
    assertTrue(sp.params().containsValue(2));
  }

  @Test
  void testWithNumericResults() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(
                        Condition.gt(Order::getTotal, new BigDecimal("1000")),
                        new BigDecimal("100"))
                    .when(
                        Condition.gt(Order::getTotal, new BigDecimal("100")), new BigDecimal("10"))
                    .orElse(new BigDecimal("1"))
                    .as("discount"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("CASE WHEN orders.total > :"));
    assertTrue(sp.sql().contains(" END AS discount"));
    assertEquals(5, sp.params().size());
    assertTrue(sp.params().containsValue(new BigDecimal("1000")));
    assertTrue(sp.params().containsValue(new BigDecimal("100")));
    assertTrue(sp.params().containsValue(new BigDecimal("10")));
    assertTrue(sp.params().containsValue(new BigDecimal("1")));
  }

  @Test
  void testWithAlias() {
    CaseWhen caseWhen =
        CaseWhen.builder()
            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
            .orElse("Pending")
            .as("status_label");

    assertEquals("status_label", caseWhen.getAlias());
    assertTrue(caseWhen.toSql().contains(" END AS status_label"));
  }

  @Test
  void testWithoutAlias() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("CASE WHEN orders.status = :"));
    assertTrue(sp.sql().contains(" THEN :"));
    assertTrue(sp.sql().contains(" ELSE :"));
    assertTrue(sp.sql().contains(" END"));
    assertFalse(sp.sql().contains(" AS "));
  }

  @Test
  void testInComplexQuery() {
    Select select =
        Select.builder()
            .select(Customer::getId)
            .select(Customer::getName)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Customer::getName, "John"), "VIP")
                    .when(Condition.like(Customer::getName, "A%"), "Regular")
                    .orElse("New")
                    .as("customer_tier"))
            .from(Customer.class)
            .join(Order.class, Condition.eq(Customer::getId, Order::getCustomerId))
            .where(Condition.eq(Order::getStatus, OrderStatus.PAID))
            .groupBy(Customer::getId, Customer::getName)
            .orderBy(OrderBy.desc(Customer::getId))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("SELECT customers.id, customers.name, CASE WHEN"));
    assertTrue(sp.sql().contains("FROM customers"));
    assertTrue(sp.sql().contains("INNER JOIN orders ON"));
    assertTrue(sp.sql().contains("WHERE orders.status = :"));
    assertTrue(sp.sql().contains("GROUP BY customers.id, customers.name"));
    assertTrue(sp.sql().contains("ORDER BY customers.id DESC"));
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue("VIP"));
    assertTrue(sp.params().containsValue("Regular"));
    assertTrue(sp.params().containsValue("New"));
  }

  @Test
  void testWithDifferentConditionTypes() {
    CaseWhen gtCase =
        CaseWhen.builder()
            .when(Condition.gt(Order::getTotal, new BigDecimal("100")), "High")
            .orElse("Low");
    assertTrue(gtCase.toSql().contains("CASE WHEN orders.total > :"));

    CaseWhen ltCase =
        CaseWhen.builder()
            .when(Condition.lt(Order::getTotal, new BigDecimal("50")), "Low")
            .orElse("High");
    assertTrue(ltCase.toSql().contains("CASE WHEN orders.total < :"));

    CaseWhen likeCase =
        CaseWhen.builder().when(Condition.like(Customer::getName, "A%"), "A-names").orElse("Other");
    assertTrue(likeCase.toSql().contains("CASE WHEN customers.name LIKE :"));

    CaseWhen inCase =
        CaseWhen.builder()
            .when(
                Condition.in(
                    Order::getStatus, java.util.List.of(OrderStatus.PAID, OrderStatus.DELIVERED)),
                "Done")
            .orElse("Pending");
    assertTrue(inCase.toSql().contains("CASE WHEN orders.status IN (:"));

    CaseWhen isNullCase =
        CaseWhen.builder().when(Condition.isNull(Order::getStatus), "Unknown").orElse("Known");
    assertTrue(isNullCase.toSql().contains("CASE WHEN orders.status IS NULL"));
  }

  @Test
  void testParameterHandling() {
    CaseWhen caseWhen =
        CaseWhen.builder()
            .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
            .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), "In Transit")
            .orElse("Pending");

    Map<String, Object> params = caseWhen.getParams();

    assertEquals(5, params.size());
    assertTrue(params.containsValue(OrderStatus.PAID));
    assertTrue(params.containsValue(OrderStatus.SHIPPED));
    assertTrue(params.containsValue("Completed"));
    assertTrue(params.containsValue("In Transit"));
    assertTrue(params.containsValue("Pending"));
  }

  @Test
  void testMultipleCaseWhenInSameQuery() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .select(
                CaseWhen.builder()
                    .when(Condition.gt(Order::getTotal, new BigDecimal("100")), "High")
                    .orElse("Low")
                    .as("value_category"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();

    assertTrue(sp.sql().contains("status_label"));
    assertTrue(sp.sql().contains("value_category"));
    assertTrue(sp.params().containsValue(OrderStatus.PAID));
    assertTrue(sp.params().containsValue("Completed"));
    assertTrue(sp.params().containsValue("Pending"));
    assertTrue(sp.params().containsValue(new BigDecimal("100")));
    assertTrue(sp.params().containsValue("High"));
    assertTrue(sp.params().containsValue("Low"));
  }

  @Test
  void testWithPositionalSqlOutput() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), "In Transit")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .build();

    Select.PositionalSqlAndParams psp = select.toPositionalSql();

    assertTrue(psp.sql().contains("CASE WHEN orders.status = ?"));
    assertTrue(psp.sql().contains(" THEN ?"));
    assertTrue(psp.sql().contains(" ELSE ?"));
    assertTrue(psp.sql().contains(" END AS status_label"));
    assertEquals(5, psp.params().size());
    assertTrue(psp.params().contains(OrderStatus.PAID));
    assertTrue(psp.params().contains(OrderStatus.SHIPPED));
    assertTrue(psp.params().contains("Completed"));
    assertTrue(psp.params().contains("In Transit"));
    assertTrue(psp.params().contains("Pending"));
  }

  @Test
  void testWithIndexedSqlOutput() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .build();

    Select.IndexedSqlAndParams isp = select.toIndexedSql();

    assertTrue(isp.sql().contains("CASE WHEN orders.status = $"));
    assertTrue(isp.sql().contains(" THEN $"));
    assertTrue(isp.sql().contains(" ELSE $"));
    assertTrue(isp.sql().contains(" END AS status_label"));
    assertEquals(3, isp.params().size());
    assertTrue(isp.params().contains(OrderStatus.PAID));
    assertTrue(isp.params().contains("Completed"));
    assertTrue(isp.params().contains("Pending"));
  }

  @Test
  void testWithPlainSqlOutput() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .build();

    String plainSql = select.toPlainSql();

    assertTrue(plainSql.contains("CASE WHEN orders.status = 'PAID'"));
    assertTrue(plainSql.contains(" THEN 'Completed'"));
    assertTrue(plainSql.contains(" ELSE 'Pending'"));
    assertTrue(plainSql.contains(" END AS status_label"));
  }

  @Test
  void testToSqlThrowsExceptionWithoutWhenClauses() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          CaseWhen.builder().orElse("Default").toSql();
        });
  }

  @Test
  void testCaseWhenWithQuoteStrategy() {
    Select selectAnsi =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams spAnsi = selectAnsi.toSqlAndParams();

    assertTrue(spAnsi.sql().contains("CASE WHEN \"orders\".\"status\" = :"));
    assertTrue(spAnsi.sql().contains(" END AS status_label"));

    Select selectMysql =
        Select.builder()
            .select(Order::getId)
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams spMysql = selectMysql.toSqlAndParams();

    assertTrue(spMysql.sql().contains("CASE WHEN `orders`.`status` = :"));
    assertTrue(spMysql.sql().contains(" END AS status_label"));
  }
}
