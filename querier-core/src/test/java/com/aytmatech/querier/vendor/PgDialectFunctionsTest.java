package com.aytmatech.querier.vendor;

import static com.aytmatech.querier.vendor.PgDialectFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.*;
import com.aytmatech.querier.model.Order;
import com.aytmatech.querier.model.OrderStatus;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class PgDialectFunctionsTest {

  @Test
  void dateTrunc_standalone() {
    DbFunction fn = dateTrunc("month", Order::getCreatedAt);
    assertEquals("DATE_TRUNC('month', orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void dateTrunc_withAlias() {
    DbFunction fn = dateTrunc("month", Order::getCreatedAt).as("month");
    assertEquals("DATE_TRUNC('month', orders.created_at) AS month", fn.toSql());
    assertEquals("DATE_TRUNC('month', orders.created_at)", fn.toSqlWithoutAlias());
  }

  @Test
  void extract_customSyntax() {
    DbFunction fn = PgDialectFunctions.extract("epoch", Order::getCreatedAt);
    assertEquals("EXTRACT(EPOCH FROM orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void toChar_standalone() {
    DbFunction fn = PgDialectFunctions.toChar(Order::getCreatedAt, "YYYY-MM");
    assertEquals("TO_CHAR(orders.created_at, 'YYYY-MM')", fn.toSql());
  }

  @Test
  void coalesce_columnAndDefault() {
    DbFunction fn = PgDialectFunctions.coalesce(Order::getStatus, "PENDING");
    assertEquals("COALESCE(orders.status, :param0)", fn.toSql());
    assertEquals("PENDING", fn.getParams().get("param0"));
  }

  @Test
  void nullif_standalone() {
    DbFunction fn = PgDialectFunctions.nullif(Order::getTotal, BigDecimal.ZERO);
    assertEquals("NULLIF(orders.total, :param0)", fn.toSql());
    assertEquals(BigDecimal.ZERO, fn.getParams().get("param0"));
  }

  @Test
  void cast_customSyntax() {
    DbFunction fn = PgDialectFunctions.cast(Order::getId, "TEXT");
    assertEquals("CAST(orders.id AS TEXT)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void jsonbExtractPathText_multiKey() {
    DbFunction fn = jsonbExtractPathText(Order::getStatus, "address", "city");
    assertEquals("jsonb_extract_path_text(orders.status, 'address', 'city')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void regexpReplace_parameterized() {
    DbFunction fn = PgDialectFunctions.regexpReplace(Order::getStatus, "\\d", "X");
    assertEquals("REGEXP_REPLACE(orders.status, :param0, :param1)", fn.toSql());
    assertEquals("\\d", fn.getParams().get("param0"));
    assertEquals("X", fn.getParams().get("param1"));
  }

  @Test
  void select_withDateTrunc_inQuery() {
    Select select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(dateTrunc("month", Order::getCreatedAt).as("month"))
            .select(Aggregate.sum(Order::getTotal).as("monthly_total"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.customer_id, DATE_TRUNC('month', orders.created_at) AS month, "
            + "SUM(orders.total) AS monthly_total FROM orders GROUP BY orders.customer_id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void select_withCoalesce_hasParams() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(PgDialectFunctions.coalesce(Order::getStatus, "UNKNOWN").as("safe_status"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id, COALESCE(orders.status, :param0) AS safe_status FROM orders", sp.sql());
    assertEquals("UNKNOWN", sp.params().get("param0"));
  }

  @Test
  void where_withDateTrunc_eq() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(dateTrunc("month", Order::getCreatedAt), "2024-01-01"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE DATE_TRUNC('month', orders.created_at) = :param0",
        sp.sql());
    assertEquals("2024-01-01", sp.params().get("param0"));
  }

  @Test
  void where_withDatePart_between() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.between(
                    PgDialectFunctions.datePart("year", Order::getCreatedAt), 2022, 2025))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE DATE_PART('year', orders.created_at) BETWEEN :param0 AND :param1",
        sp.sql());
    assertEquals(2022, sp.params().get("param0"));
    assertEquals(2025, sp.params().get("param1"));
  }

  @Test
  void where_withJsonbExtractPathText_eq() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(jsonbExtractPathText(Order::getStatus, "type"), "premium"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE jsonb_extract_path_text(orders.status, 'type') = :param0",
        sp.sql());
    assertEquals("premium", sp.params().get("param0"));
  }

  @Test
  void where_withIsNull() {
    Select select =
        Select.builder()
            .from(Order.class)
            .where(Condition.isNull(PgDialectFunctions.coalesce(Order::getStatus, "default")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("IS NULL"));
    assertTrue(sp.sql().contains("COALESCE(orders.status,"));
  }

  @Test
  void orderBy_withDateTrunc() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .orderBy(OrderBy.asc(dateTrunc("month", Order::getCreatedAt)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders ORDER BY DATE_TRUNC('month', orders.created_at) ASC",
        sp.sql());
  }

  @Test
  void windowFunction_withLag() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .select(
                WindowFunction.of(lag(Order::getTotal, 1, BigDecimal.ZERO))
                    .partitionBy(Order::getCustomerId)
                    .orderBy(OrderBy.asc(Order::getCreatedAt))
                    .as("prev_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("LAG(orders.total, 1, :param0)"));
    assertTrue(
        sp.sql().contains("OVER (PARTITION BY orders.customer_id ORDER BY orders.created_at ASC)"));
    assertTrue(sp.sql().contains("AS prev_total"));
    assertEquals(BigDecimal.ZERO, sp.params().get("param0"));
  }

  @Test
  void quoteStrategy_ansi_appliedToDbFunction() {
    Select select =
        Select.builder()
            .select(dateTrunc("month", Order::getCreatedAt).as("month"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("DATE_TRUNC('month', \"orders\".\"created_at\")"));
  }

  @Test
  void paramNamesAreUniqueAcrossMultipleFunctions() {
    Select select =
        Select.builder()
            .select(PgDialectFunctions.coalesce(Order::getStatus, "PENDING").as("s1"))
            .select(PgDialectFunctions.coalesce(Order::getTotal, BigDecimal.ZERO).as("s2"))
            .from(Order.class)
            .where(Condition.eq(PgDialectFunctions.nullif(Order::getId, 0L), 42L))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.params().containsKey("param0"));
    assertTrue(sp.params().containsKey("param1"));
    assertTrue(sp.params().containsKey("param2"));
    assertTrue(sp.params().containsKey("param3"));
    assertEquals(4, sp.params().size());
  }

  @Test
  void test() {
    var select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(dateTrunc("month", Order::getCreatedAt).as("month"))
            .select(Aggregate.sum(Order::getTotal).as("revenue"))
            .select(
                WindowFunction.of(lag(Order::getTotal, 1, BigDecimal.ZERO))
                    .partitionBy(Order::getCustomerId)
                    .orderBy(OrderBy.asc(Order::getCreatedAt))
                    .as("prev_month"))
            .select(
                CaseWhen.builder()
                    .when(Condition.eq(Order::getStatus, OrderStatus.PAID), "Completed")
                    .when(Condition.eq(Order::getStatus, OrderStatus.SHIPPED), "In Transit")
                    .orElse("Pending")
                    .as("status_label"))
            .from(Order.class)
            .where(Condition.gte(dateTrunc("month", Order::getCreatedAt), LocalDateTime.now()))
            .groupBy(Order::getCustomerId)
            .groupBy(Order::getTotal)
            .groupBy(Order::getCreatedAt)
            .groupBy(Order::getStatus)
            .orderBy(OrderBy.asc(dateTrunc("month", Order::getCreatedAt)))
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    System.out.println(select.toPlainSql());
  }
}
