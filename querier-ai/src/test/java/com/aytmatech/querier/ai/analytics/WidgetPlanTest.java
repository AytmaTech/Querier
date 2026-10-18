package com.aytmatech.querier.ai.analytics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WidgetPlanTest {

  private static WidgetPlan.Builder validBuilder() {
    return WidgetPlan.builder()
        .title("Revenue")
        .chartType("BAR")
        .labelColumn("month")
        .rawSql("SELECT month, SUM(total) AS revenue FROM orders GROUP BY month")
        .dataset(new WidgetPlan.DatasetSpec("revenue", "Revenue"));
  }

  @Test
  void build_setsTitle() {
    assertEquals("Revenue", validBuilder().build().title());
  }

  @Test
  void build_setsChartType() {
    assertEquals("BAR", validBuilder().build().chartType());
  }

  @Test
  void build_setsLabelColumn() {
    assertEquals("month", validBuilder().build().labelColumn());
  }

  @Test
  void build_setsRawSql() {
    WidgetPlan wp = validBuilder().build();
    assertTrue(wp.rawSql().isPresent());
  }

  @Test
  void build_setsDatasets() {
    WidgetPlan wp = validBuilder().build();
    assertEquals(1, wp.datasets().size());
  }

  @Test
  void build_missingTitle_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () ->
            WidgetPlan.builder()
                .chartType("BAR")
                .labelColumn("x")
                .dataset(new WidgetPlan.DatasetSpec("c", "L"))
                .build());
  }

  @Test
  void build_missingChartType_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () ->
            WidgetPlan.builder()
                .title("T")
                .labelColumn("x")
                .dataset(new WidgetPlan.DatasetSpec("c", "L"))
                .build());
  }

  @Test
  void build_missingLabelColumn_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () ->
            WidgetPlan.builder()
                .title("T")
                .chartType("BAR")
                .dataset(new WidgetPlan.DatasetSpec("c", "L"))
                .build());
  }

  @Test
  void hasRawSql_whenRawSqlSet_returnsTrue() {
    assertTrue(validBuilder().build().hasRawSql());
  }

  @Test
  void hasRawSql_whenRawSqlNotSet_returnsFalse() {
    WidgetPlan wp =
        WidgetPlan.builder()
            .title("T")
            .chartType("BAR")
            .labelColumn("x")
            .dataset(new WidgetPlan.DatasetSpec("c", "L"))
            .build();
    assertFalse(wp.hasRawSql());
  }

  @Test
  void rawSql_whenSet_wrappedInOptional() {
    WidgetPlan wp = validBuilder().build();
    assertTrue(wp.rawSql().isPresent());
    assertTrue(wp.rawSql().get().contains("SELECT"));
  }

  @Test
  void datasets_areImmutable() {
    WidgetPlan wp = validBuilder().build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> wp.datasets().add(new WidgetPlan.DatasetSpec("x", "y")));
  }

  @Test
  void toPromptText_containsTitle() {
    assertTrue(validBuilder().build().toPromptText().contains("Revenue"));
  }

  @Test
  void toPromptText_containsChartType() {
    assertTrue(validBuilder().build().toPromptText().contains("BAR"));
  }

  @Test
  void toPromptText_containsRawSql() {
    assertTrue(validBuilder().build().toPromptText().contains("SELECT"));
  }

  @Test
  void datasetSpec_record_storesColumnAndLabel() {
    WidgetPlan.DatasetSpec spec = new WidgetPlan.DatasetSpec("revenue", "Revenue USD");
    assertEquals("revenue", spec.column());
    assertEquals("Revenue USD", spec.label());
  }
}
