package com.aytmatech.querier.vendor;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.*;
import com.aytmatech.querier.model.Order;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class MsSqlDialectFunctionsTest {

  @Test
  void isNull_parameterized() {
    DbFunction fn = MsSqlDialectFunctions.isNull(Order::getStatus, "PENDING");
    assertEquals("ISNULL(orders.status, :param0)", fn.toSql());
    assertEquals("PENDING", fn.getParams().get("param0"));
  }

  @Test
  void datePart_rawUnit() {
    DbFunction fn = MsSqlDialectFunctions.datePart("month", Order::getCreatedAt);
    assertEquals("DATEPART(month, orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void dateDiff_twoColumns() {
    DbFunction fn = MsSqlDialectFunctions.dateDiff("day", Order::getCreatedAt, Order::getCreatedAt);
    assertEquals("DATEDIFF(day, orders.created_at, orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void dateAdd_customSyntax_withParam() {
    DbFunction fn = MsSqlDialectFunctions.dateAdd("day", 30, Order::getCreatedAt);
    assertEquals("DATEADD(day, :param0, orders.created_at)", fn.toSql());
    assertEquals(30, fn.getParams().get("param0"));
  }

  @Test
  void format_standalone() {
    DbFunction fn = MsSqlDialectFunctions.format(Order::getCreatedAt, "yyyy-MM");
    assertEquals("FORMAT(orders.created_at, 'yyyy-MM')", fn.toSql());
  }

  @Test
  void jsonValue_pathAsLiteral() {
    DbFunction fn = MsSqlDialectFunctions.jsonValue(Order::getStatus, "$.type");
    assertEquals("JSON_VALUE(orders.status, '$.type')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void cast_customSyntax() {
    DbFunction fn = MsSqlDialectFunctions.cast(Order::getId, "NVARCHAR(MAX)");
    assertEquals("CAST(orders.id AS NVARCHAR(MAX))", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void tryCast_customSyntax() {
    DbFunction fn = MsSqlDialectFunctions.tryCast(Order::getId, "INT");
    assertEquals("TRY_CAST(orders.id AS INT)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void convert_withStyle() {
    DbFunction fn = MsSqlDialectFunctions.convert("VARCHAR(10)", Order::getCreatedAt, 120);
    assertEquals("CONVERT(VARCHAR(10), orders.created_at, 120)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void ntile_windowFunction() {
    DbFunction fn = MsSqlDialectFunctions.ntile(4);
    assertEquals("NTILE(4)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void choose_multipleOptions() {
    DbFunction fn = MsSqlDialectFunctions.choose(Order::getStatus, "Low", "Medium", "High");
    assertEquals("CHOOSE(orders.status, :param0, :param1, :param2)", fn.toSql());
    assertEquals("Low", fn.getParams().get("param0"));
    assertEquals("Medium", fn.getParams().get("param1"));
    assertEquals("High", fn.getParams().get("param2"));
  }

  @Test
  void select_withIsNull() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(MsSqlDialectFunctions.isNull(Order::getStatus, "UNKNOWN").as("safe_status"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id, ISNULL(orders.status, :param0) AS safe_status FROM orders", sp.sql());
    assertEquals("UNKNOWN", sp.params().get("param0"));
  }

  @Test
  void where_withDatePart_in() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.in(
                    MsSqlDialectFunctions.datePart("month", Order::getCreatedAt), List.of(1, 2, 3)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE DATEPART(month, orders.created_at) IN (:param0)",
        sp.sql());
  }

  @Test
  void windowFunction_withLag() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                WindowFunction.of(MsSqlDialectFunctions.lag(Order::getTotal, 1, BigDecimal.ZERO))
                    .partitionBy(Order::getCustomerId)
                    .orderBy(OrderBy.asc(Order::getCreatedAt))
                    .as("prev_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("LAG(orders.total, 1, :param0)"));
    assertTrue(sp.sql().contains("OVER (PARTITION BY orders.customer_id"));
    assertEquals(BigDecimal.ZERO, sp.params().get("param0"));
  }

  @Test
  void quoteStrategy_sqlServer_appliedToDbFunction() {
    Select select =
        Select.builder()
            .select(MsSqlDialectFunctions.datePart("year", Order::getCreatedAt).as("yr"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.SQL_SERVER)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("DATEPART(year, [orders].[created_at])"));
  }
}
