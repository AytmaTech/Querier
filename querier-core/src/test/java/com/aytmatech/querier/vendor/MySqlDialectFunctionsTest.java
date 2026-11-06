package com.aytmatech.querier.vendor;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.*;
import com.aytmatech.querier.model.Order;
import org.junit.jupiter.api.Test;

class MySqlDialectFunctionsTest {

  @Test
  void dateFormat_standalone() {
    DbFunction fn = MySqlDialectFunctions.dateFormat(Order::getCreatedAt, "%Y-%m");
    assertEquals("DATE_FORMAT(orders.created_at, '%Y-%m')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void year_month_day_standalone() {
    assertEquals(
        "YEAR(orders.created_at)", MySqlDialectFunctions.year(Order::getCreatedAt).toSql());
    assertEquals(
        "MONTH(orders.created_at)", MySqlDialectFunctions.month(Order::getCreatedAt).toSql());
    assertEquals("DAY(orders.created_at)", MySqlDialectFunctions.day(Order::getCreatedAt).toSql());
  }

  @Test
  void dateAdd_customSyntax_withParam() {
    DbFunction fn = MySqlDialectFunctions.dateAdd(Order::getCreatedAt, 30, "DAY");
    assertEquals("DATE_ADD(orders.created_at, INTERVAL :param0 DAY)", fn.toSql());
    assertEquals(30, fn.getParams().get("param0"));
  }

  @Test
  void dateSub_customSyntax_withParam() {
    DbFunction fn = MySqlDialectFunctions.dateSub(Order::getCreatedAt, 7, "DAY");
    assertEquals("DATE_SUB(orders.created_at, INTERVAL :param0 DAY)", fn.toSql());
    assertEquals(7, fn.getParams().get("param0"));
  }

  @Test
  void groupConcat_withSeparator() {
    DbFunction fn = MySqlDialectFunctions.groupConcat(Order::getStatus, " | ");
    assertEquals("GROUP_CONCAT(orders.status SEPARATOR ' | ')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void ifNull_parameterized() {
    DbFunction fn = MySqlDialectFunctions.ifNull(Order::getStatus, "PENDING");
    assertEquals("IFNULL(orders.status, :param0)", fn.toSql());
    assertEquals("PENDING", fn.getParams().get("param0"));
  }

  @Test
  void jsonExtract_multiPath() {
    DbFunction fn = MySqlDialectFunctions.jsonExtract(Order::getStatus, "$.address", "$.city");
    assertEquals("JSON_EXTRACT(orders.status, '$.address', '$.city')", fn.toSql());
  }

  @Test
  void cast_customSyntax() {
    DbFunction fn = MySqlDialectFunctions.cast(Order::getId, "CHAR");
    assertEquals("CAST(orders.id AS CHAR)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void select_withDateFormat_inQuery() {
    Select select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(MySqlDialectFunctions.dateFormat(Order::getCreatedAt, "%Y-%m").as("month"))
            .select(Aggregate.sum(Order::getTotal).as("total"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.customer_id, DATE_FORMAT(orders.created_at, '%Y-%m') AS month, "
            + "SUM(orders.total) AS total FROM orders GROUP BY orders.customer_id",
        sp.sql());
  }

  @Test
  void where_withYear_eq() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(MySqlDialectFunctions.year(Order::getCreatedAt), 2024))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT orders.id FROM orders WHERE YEAR(orders.created_at) = :param0", sp.sql());
    assertEquals(2024, sp.params().get("param0"));
  }

  @Test
  void orderBy_withDateFormat() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .orderBy(OrderBy.desc(MySqlDialectFunctions.dateFormat(Order::getCreatedAt, "%Y-%m")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders ORDER BY DATE_FORMAT(orders.created_at, '%Y-%m') DESC",
        sp.sql());
  }

  @Test
  void quoteStrategy_mysql_appliedToDbFunction() {
    Select select =
        Select.builder()
            .select(MySqlDialectFunctions.year(Order::getCreatedAt).as("yr"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.MYSQL)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("YEAR(`orders`.`created_at`)"));
  }
}
