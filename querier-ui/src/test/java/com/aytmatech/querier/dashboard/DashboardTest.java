package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.Select;
import org.junit.jupiter.api.Test;

class DashboardTest {

  private static final DashboardWidget WIDGET =
      DashboardWidget.builder()
          .title("Revenue")
          .chartType(ChartType.BAR)
          .query(Select.builder().from(Fixture.class).build())
          .labelColumn("status")
          .dataset(ChartDataset.of("total").label("Revenue").build())
          .build();

  @Test
  void build_setsTitle() {
    Dashboard d = Dashboard.builder().title("Sales").addWidget(WIDGET).build();
    assertEquals("Sales", d.getTitle());
  }

  @Test
  void build_defaultTitle_whenNull() {
    Dashboard d = Dashboard.builder().title(null).addWidget(WIDGET).build();
    assertEquals("Dashboard", d.getTitle());
  }

  @Test
  void build_defaultLayout_isGrid2Cols() {
    Dashboard d = Dashboard.builder().addWidget(WIDGET).build();
    assertEquals(DashboardLayout.GRID_2_COLS, d.getDashboardLayout());
  }

  @Test
  void build_customLayout_isStored() {
    Dashboard d = Dashboard.builder().addWidget(WIDGET).layout(DashboardLayout.SINGLE_COL).build();
    assertEquals(DashboardLayout.SINGLE_COL, d.getDashboardLayout());
  }

  @Test
  void build_multipleWidgets_allStored() {
    Dashboard d = Dashboard.builder().addWidget(WIDGET).addWidget(WIDGET).build();
    assertEquals(2, d.getWidgets().size());
  }

  @Test
  void widgets_preserveInsertionOrder() {
    DashboardWidget w2 =
        DashboardWidget.builder()
            .title("Orders")
            .chartType(ChartType.PIE)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("count").build())
            .build();

    Dashboard d = Dashboard.builder().addWidget(WIDGET).addWidget(w2).build();

    assertEquals("Revenue", d.getWidgets().get(0).getTitle());
    assertEquals("Orders", d.getWidgets().get(1).getTitle());
  }

  @Test
  void widgets_areImmutable() {
    Dashboard d = Dashboard.builder().addWidget(WIDGET).build();
    assertThrows(UnsupportedOperationException.class, () -> d.getWidgets().add(WIDGET));
  }

  @Test
  void build_noWidgets_throwsIllegalState() {
    assertThrows(IllegalStateException.class, () -> Dashboard.builder().title("Empty").build());
  }

  @Test
  void addWidget_null_isIgnored() {
    Dashboard d = Dashboard.builder().addWidget(WIDGET).addWidget(null).build();
    assertEquals(1, d.getWidgets().size());
  }

  @com.aytmatech.querier.annotation.Table("fixture")
  private static class Fixture {
    public Long getId() {
      return 1L;
    }
  }
}
