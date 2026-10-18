package com.aytmatech.querier.ai.analytics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class DashboardPlanTest {

  private static WidgetPlan minimalWidget(String title) {
    return WidgetPlan.builder()
        .title(title)
        .chartType("BAR")
        .labelColumn("col")
        .rawSql("SELECT 1")
        .dataset(new WidgetPlan.DatasetSpec("col", "Label"))
        .build();
  }

  @Test
  void build_setsTitle() {
    DashboardPlan plan =
        DashboardPlan.builder().title("My Dashboard").widget(minimalWidget("W")).build();

    assertEquals("My Dashboard", plan.title());
  }

  @Test
  void build_setsLayout() {
    DashboardPlan plan =
        DashboardPlan.builder().title("T").layout("SINGLE_COL").widget(minimalWidget("W")).build();

    assertEquals("SINGLE_COL", plan.layout());
  }

  @Test
  void build_defaultLayout_isGrid2Cols() {
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(minimalWidget("W")).build();

    assertEquals("GRID_2_COLS", plan.layout());
  }

  @Test
  void build_setsWidgets() {
    DashboardPlan plan =
        DashboardPlan.builder()
            .title("T")
            .widget(minimalWidget("W1"))
            .widget(minimalWidget("W2"))
            .build();

    assertEquals(2, plan.widgets().size());
  }

  @Test
  void build_noWidgets_throwsIllegalState() {
    assertThrows(IllegalStateException.class, () -> DashboardPlan.builder().title("T").build());
  }

  @Test
  void widgets_areImmutable() {
    DashboardPlan plan = DashboardPlan.builder().title("T").widget(minimalWidget("W")).build();

    assertThrows(
        UnsupportedOperationException.class, () -> plan.widgets().add(minimalWidget("extra")));
  }

  @Test
  void toPromptText_containsTitle() {
    DashboardPlan plan =
        DashboardPlan.builder().title("Revenue Report").widget(minimalWidget("W")).build();

    assertTrue(plan.toPromptText().contains("Revenue Report"));
  }

  @Test
  void toPromptText_containsLayout() {
    DashboardPlan plan =
        DashboardPlan.builder().title("T").layout("GRID_3_COLS").widget(minimalWidget("W")).build();

    assertTrue(plan.toPromptText().contains("GRID_3_COLS"));
  }

  @Test
  void toPromptText_containsWidgetTitle() {
    DashboardPlan plan =
        DashboardPlan.builder().title("T").widget(minimalWidget("My Widget")).build();

    assertTrue(plan.toPromptText().contains("My Widget"));
  }
}
