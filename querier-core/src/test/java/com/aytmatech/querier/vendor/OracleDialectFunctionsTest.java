package com.aytmatech.querier.vendor;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.*;
import com.aytmatech.querier.model.Order;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class OracleDialectFunctionsTest {

  @Test
  void truncDate_standalone() {
    DbFunction fn = OracleDialectFunctions.trunc(Order::getCreatedAt, "MM");
    assertEquals("TRUNC(orders.created_at, 'MM')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void truncDate_withAlias() {
    DbFunction fn = OracleDialectFunctions.trunc(Order::getCreatedAt, "MM").as("month");
    assertEquals("TRUNC(orders.created_at, 'MM') AS month", fn.toSql());
    assertEquals("TRUNC(orders.created_at, 'MM')", fn.toSqlWithoutAlias());
  }

  @Test
  void truncNumeric_standalone() {
    DbFunction fn = OracleDialectFunctions.trunc(Order::getTotal, 2);
    assertEquals("TRUNC(orders.total, 2)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void addMonths_parameterized() {
    DbFunction fn = OracleDialectFunctions.addMonths(Order::getCreatedAt, 3);
    assertEquals("ADD_MONTHS(orders.created_at, :param0)", fn.toSql());
    assertEquals(3, fn.getParams().get("param0"));
  }

  @Test
  void monthsBetween_twoColumns() {
    DbFunction fn = OracleDialectFunctions.monthsBetween(Order::getCreatedAt, Order::getCreatedAt);
    assertEquals("MONTHS_BETWEEN(orders.created_at, orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void lastDay_standalone() {
    DbFunction fn = OracleDialectFunctions.lastDay(Order::getCreatedAt);
    assertEquals("LAST_DAY(orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void nextDay_parameterized() {
    DbFunction fn = OracleDialectFunctions.nextDay(Order::getCreatedAt, "MONDAY");
    assertEquals("NEXT_DAY(orders.created_at, :param0)", fn.toSql());
    assertEquals("MONDAY", fn.getParams().get("param0"));
  }

  @Test
  void extract_customSyntax() {
    DbFunction fn = OracleDialectFunctions.extract("year", Order::getCreatedAt);
    assertEquals("EXTRACT(YEAR FROM orders.created_at)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void extract_uppercasesField() {
    DbFunction fn = OracleDialectFunctions.extract("month", Order::getCreatedAt);
    assertEquals("EXTRACT(MONTH FROM orders.created_at)", fn.toSql());
  }

  @Test
  void toChar_standalone() {
    DbFunction fn = OracleDialectFunctions.toChar(Order::getCreatedAt, "YYYY-MM");
    assertEquals("TO_CHAR(orders.created_at, 'YYYY-MM')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void toDate_standalone() {
    DbFunction fn = OracleDialectFunctions.toDate(Order::getCreatedAt, "YYYY-MM-DD");
    assertEquals("TO_DATE(orders.created_at, 'YYYY-MM-DD')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void toTimestamp_standalone() {
    DbFunction fn =
        OracleDialectFunctions.toTimestamp(Order::getCreatedAt, "YYYY-MM-DD HH24:MI:SS");
    assertEquals("TO_TIMESTAMP(orders.created_at, 'YYYY-MM-DD HH24:MI:SS')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void sysdate_noParentheses() {
    DbFunction fn = OracleDialectFunctions.sysdate();
    assertEquals("SYSDATE", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void sysdate_withAlias() {
    DbFunction fn = OracleDialectFunctions.sysdate().as("today");
    assertEquals("SYSDATE AS today", fn.toSql());
    assertEquals("SYSDATE", fn.toSqlWithoutAlias());
  }

  @Test
  void systimestamp_noParentheses() {
    DbFunction fn = OracleDialectFunctions.systimestamp();
    assertEquals("SYSTIMESTAMP", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void nvl_standalone() {
    DbFunction fn = OracleDialectFunctions.nvl(Order::getStatus, "PENDING");
    assertEquals("NVL(orders.status, :param0)", fn.toSql());
    assertEquals("PENDING", fn.getParams().get("param0"));
  }

  @Test
  void nvl2_standalone() {
    DbFunction fn = OracleDialectFunctions.nvl2(Order::getStatus, "Has value", "No value");
    assertEquals("NVL2(orders.status, :param0, :param1)", fn.toSql());
    assertEquals("Has value", fn.getParams().get("param0"));
    assertEquals("No value", fn.getParams().get("param1"));
  }

  @Test
  void coalesce_standalone() {
    DbFunction fn = OracleDialectFunctions.coalesce(Order::getStatus, "UNKNOWN");
    assertEquals("COALESCE(orders.status, :param0)", fn.toSql());
    assertEquals("UNKNOWN", fn.getParams().get("param0"));
  }

  @Test
  void nullif_standalone() {
    DbFunction fn = OracleDialectFunctions.nullif(Order::getTotal, BigDecimal.ZERO);
    assertEquals("NULLIF(orders.total, :param0)", fn.toSql());
    assertEquals(BigDecimal.ZERO, fn.getParams().get("param0"));
  }

  @Test
  void decode_searchResultPairsWithDefault() {
    DbFunction fn =
        OracleDialectFunctions.decode(
            Order::getStatus, "PAID", "Completed", "SHIPPED", "In Transit", "Pending");
    assertEquals("DECODE(orders.status, :param0, :param1, :param2, :param3, :param4)", fn.toSql());
    assertEquals("PAID", fn.getParams().get("param0"));
    assertEquals("Completed", fn.getParams().get("param1"));
    assertEquals("SHIPPED", fn.getParams().get("param2"));
    assertEquals("In Transit", fn.getParams().get("param3"));
    assertEquals("Pending", fn.getParams().get("param4"));
    assertEquals(5, fn.getParams().size());
  }

  @Test
  void decode_singlePairNoDefault() {
    DbFunction fn = OracleDialectFunctions.decode(Order::getStatus, "PAID", "Yes");
    assertEquals("DECODE(orders.status, :param0, :param1)", fn.toSql());
    assertEquals("PAID", fn.getParams().get("param0"));
    assertEquals("Yes", fn.getParams().get("param1"));
  }

  @Test
  void substr_standalone() {
    DbFunction fn = OracleDialectFunctions.substr(Order::getStatus, 1, 3);
    assertEquals("SUBSTR(orders.status, 1, 3)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void instr_parameterized() {
    DbFunction fn = OracleDialectFunctions.instr(Order::getStatus, "PAID");
    assertEquals("INSTR(orders.status, :param0)", fn.toSql());
    assertEquals("PAID", fn.getParams().get("param0"));
  }

  @Test
  void replace_parameterized() {
    DbFunction fn = OracleDialectFunctions.replace(Order::getStatus, "PAID", "SETTLED");
    assertEquals("REPLACE(orders.status, :param0, :param1)", fn.toSql());
    assertEquals("PAID", fn.getParams().get("param0"));
    assertEquals("SETTLED", fn.getParams().get("param1"));
  }

  @Test
  void regexpReplace_parameterized() {
    DbFunction fn = OracleDialectFunctions.regexpReplace(Order::getStatus, "\\d", "X");
    assertEquals("REGEXP_REPLACE(orders.status, :param0, :param1)", fn.toSql());
    assertEquals("\\d", fn.getParams().get("param0"));
    assertEquals("X", fn.getParams().get("param1"));
  }

  @Test
  void regexpSubstr_parameterized() {
    DbFunction fn = OracleDialectFunctions.regexpSubstr(Order::getStatus, "[A-Z]+");
    assertEquals("REGEXP_SUBSTR(orders.status, :param0)", fn.toSql());
    assertEquals("[A-Z]+", fn.getParams().get("param0"));
  }

  @Test
  void concat_twoColumns() {
    DbFunction fn = OracleDialectFunctions.concat(Order::getStatus, Order::getStatus);
    assertEquals("CONCAT(orders.status, orders.status)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void listagg_customSyntax_ascending() {
    DbFunction fn =
        OracleDialectFunctions.listagg(Order::getStatus, ", ", Order::getCreatedAt, true);
    assertEquals(
        "LISTAGG(orders.status, ', ') WITHIN GROUP (ORDER BY orders.created_at ASC)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void listagg_customSyntax_descending() {
    DbFunction fn =
        OracleDialectFunctions.listagg(Order::getStatus, " | ", Order::getCreatedAt, false);
    assertEquals(
        "LISTAGG(orders.status, ' | ') WITHIN GROUP (ORDER BY orders.created_at DESC)", fn.toSql());
  }

  @Test
  void mod_standalone() {
    DbFunction fn = OracleDialectFunctions.mod(Order::getId, 10);
    assertEquals("MOD(orders.id, :param0)", fn.toSql());
    assertEquals(10, fn.getParams().get("param0"));
  }

  @Test
  void cast_customSyntax() {
    DbFunction fn = OracleDialectFunctions.cast(Order::getId, "VARCHAR2(50)");
    assertEquals("CAST(orders.id AS VARCHAR2(50))", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void toNumber_standalone() {
    DbFunction fn = OracleDialectFunctions.toNumber(Order::getTotal);
    assertEquals("TO_NUMBER(orders.total)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void toNumber_withFormat() {
    DbFunction fn = OracleDialectFunctions.toNumber(Order::getTotal, "999G999D99");
    assertEquals("TO_NUMBER(orders.total, '999G999D99')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void jsonValue_pathAsLiteral() {
    DbFunction fn = OracleDialectFunctions.jsonValue(Order::getStatus, "$.type");
    assertEquals("JSON_VALUE(orders.status, '$.type')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void jsonQuery_pathAsLiteral() {
    DbFunction fn = OracleDialectFunctions.jsonQuery(Order::getStatus, "$.address");
    assertEquals("JSON_QUERY(orders.status, '$.address')", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void ntile_standalone() {
    DbFunction fn = OracleDialectFunctions.ntile(4);
    assertEquals("NTILE(4)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void nthValue_standalone() {
    DbFunction fn = OracleDialectFunctions.nthValue(Order::getTotal, 2);
    assertEquals("NTH_VALUE(orders.total, 2)", fn.toSql());
    assertTrue(fn.getParams().isEmpty());
  }

  @Test
  void select_withToChar_groupBy() {
    Select select =
        Select.builder()
            .select(Order::getCustomerId)
            .select(OracleDialectFunctions.toChar(Order::getCreatedAt, "YYYY-MM").as("month"))
            .select(Aggregate.sum(Order::getTotal).as("monthly_total"))
            .from(Order.class)
            .groupBy(Order::getCustomerId)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.customer_id, TO_CHAR(orders.created_at, 'YYYY-MM') AS month, "
            + "SUM(orders.total) AS monthly_total FROM orders GROUP BY orders.customer_id",
        sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void select_withNvl_hasParam() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(OracleDialectFunctions.nvl(Order::getStatus, "UNKNOWN").as("safe_status"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id, NVL(orders.status, :param0) AS safe_status FROM orders", sp.sql());
    assertEquals("UNKNOWN", sp.params().get("param0"));
  }

  @Test
  void select_withDecode_hasParams() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                OracleDialectFunctions.decode(
                        Order::getStatus, "PAID", "Completed", "SHIPPED", "In Transit", "Pending")
                    .as("label"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(
        sp.sql().contains("DECODE(orders.status, :param0, :param1, :param2, :param3, :param4)"));
    assertEquals("PAID", sp.params().get("param0"));
    assertEquals("Pending", sp.params().get("param4"));
  }

  @Test
  void select_withSysdate_noParams() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(OracleDialectFunctions.sysdate().as("now"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals("SELECT orders.id, SYSDATE AS now FROM orders", sp.sql());
    assertTrue(sp.params().isEmpty());
  }

  @Test
  void where_withExtract_eq() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.eq(OracleDialectFunctions.extract("year", Order::getCreatedAt), 2024))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE EXTRACT(YEAR FROM orders.created_at) = :param0",
        sp.sql());
    assertEquals(2024, sp.params().get("param0"));
  }

  @Test
  void where_withTruncDate_gte() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.gte(
                    OracleDialectFunctions.trunc(Order::getCreatedAt, "MM"), "2024-01-01"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE TRUNC(orders.created_at, 'MM') >= :param0", sp.sql());
    assertEquals("2024-01-01", sp.params().get("param0"));
  }

  @Test
  void where_withToChar_in() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.in(
                    OracleDialectFunctions.toChar(Order::getCreatedAt, "MM"),
                    List.of("01", "02", "03")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE TO_CHAR(orders.created_at, 'MM') IN (:param0)",
        sp.sql());
  }

  @Test
  void where_withJsonValue_eq() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(
                Condition.eq(
                    OracleDialectFunctions.jsonValue(Order::getStatus, "$.tier"), "premium"))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders WHERE JSON_VALUE(orders.status, '$.tier') = :param0",
        sp.sql());
    assertEquals("premium", sp.params().get("param0"));
  }

  @Test
  void where_withNvl_isNotNull() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .where(Condition.isNotNull(OracleDialectFunctions.nvl(Order::getStatus, "PENDING")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("NVL(orders.status,"));
    assertTrue(sp.sql().contains("IS NOT NULL"));
  }

  @Test
  void orderBy_withTruncDate_asc() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .orderBy(OrderBy.asc(OracleDialectFunctions.trunc(Order::getCreatedAt, "MM")))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(
        "SELECT orders.id FROM orders ORDER BY TRUNC(orders.created_at, 'MM') ASC", sp.sql());
  }

  @Test
  void orderBy_withNvl_desc_hasParam() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .from(Order.class)
            .orderBy(OrderBy.desc(OracleDialectFunctions.nvl(Order::getTotal, BigDecimal.ZERO)))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("ORDER BY NVL(orders.total, :param0) DESC"));
    assertEquals(BigDecimal.ZERO, sp.params().get("param0"));
  }

  @Test
  void windowFunction_withLag() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(Order::getTotal)
            .select(
                WindowFunction.of(OracleDialectFunctions.lag(Order::getTotal, 1, BigDecimal.ZERO))
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
  void windowFunction_withLead() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                WindowFunction.of(OracleDialectFunctions.lead(Order::getTotal, 1, BigDecimal.ZERO))
                    .partitionBy(Order::getCustomerId)
                    .orderBy(OrderBy.asc(Order::getCreatedAt))
                    .as("next_total"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("LEAD(orders.total, 1, :param0)"));
    assertTrue(sp.sql().contains("AS next_total"));
    assertEquals(BigDecimal.ZERO, sp.params().get("param0"));
  }

  @Test
  void windowFunction_withNtile() {
    Select select =
        Select.builder()
            .select(Order::getId)
            .select(
                WindowFunction.of(OracleDialectFunctions.ntile(4))
                    .orderBy(OrderBy.desc(Order::getTotal))
                    .as("quartile"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("NTILE(4)"));
    assertTrue(sp.sql().contains("ORDER BY orders.total DESC"));
    assertTrue(sp.sql().contains("AS quartile"));
  }

  @Test
  void quoteStrategy_ansi_appliedToTrunc() {
    Select select =
        Select.builder()
            .select(OracleDialectFunctions.trunc(Order::getCreatedAt, "MM").as("month"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("TRUNC(\"orders\".\"created_at\", 'MM')"));
  }

  @Test
  void quoteStrategy_ansi_appliedToExtract() {
    Select select =
        Select.builder()
            .select(OracleDialectFunctions.extract("month", Order::getCreatedAt).as("m"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("EXTRACT(MONTH FROM \"orders\".\"created_at\")"));
  }

  @Test
  void quoteStrategy_ansi_appliedToNvl() {
    Select select =
        Select.builder()
            .select(OracleDialectFunctions.nvl(Order::getStatus, "PENDING").as("s"))
            .from(Order.class)
            .quoteStrategy(QuoteStrategy.ANSI)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.sql().contains("NVL(\"orders\".\"status\", :param0)"));
  }

  @Test
  void paramNamesAreUniqueAcrossMultipleFunctions() {
    Select select =
        Select.builder()
            .select(OracleDialectFunctions.nvl(Order::getStatus, "PENDING").as("s1"))
            .select(OracleDialectFunctions.nvl(Order::getTotal, BigDecimal.ZERO).as("s2"))
            .from(Order.class)
            .where(Condition.eq(OracleDialectFunctions.extract("year", Order::getCreatedAt), 2024))
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertTrue(sp.params().containsKey("param0"));
    assertTrue(sp.params().containsKey("param1"));
    assertTrue(sp.params().containsKey("param2"));
    assertEquals(3, sp.params().size());
    assertEquals("PENDING", sp.params().get("param0"));
    assertEquals(BigDecimal.ZERO, sp.params().get("param1"));
    assertEquals(2024, sp.params().get("param2"));
  }

  @Test
  void paramNamesAreUniqueWithDecode() {
    Select select =
        Select.builder()
            .select(OracleDialectFunctions.nvl(Order::getStatus, "DEFAULT").as("s"))
            .select(
                OracleDialectFunctions.decode(Order::getStatus, "PAID", "Yes", "No")
                    .as("paid_flag"))
            .from(Order.class)
            .build();

    Select.SqlAndParams sp = select.toSqlAndParams();
    assertEquals(4, sp.params().size());
    assertEquals("DEFAULT", sp.params().get("param0"));
    assertEquals("PAID", sp.params().get("param1"));
    assertEquals("Yes", sp.params().get("param2"));
    assertEquals("No", sp.params().get("param3"));
  }
}
