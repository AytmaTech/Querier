package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.Select;
import org.junit.jupiter.api.Test;

class DashboardWidgetTest {

  private static final Select QUERY = Select.builder().from(Fixture.class).build();

  private static final ChartDataset DATASET =
      ChartDataset.of("total").label("Revenue").backgroundColor("#4e79a7").build();

  /** Minimal valid widget builder. */
  private static DashboardWidget.Builder validBuilder() {
    return DashboardWidget.builder()
        .title("Revenue by Status")
        .chartType(ChartType.BAR)
        .query(QUERY)
        .labelColumn("status")
        .dataset(DATASET);
  }

  @Test
  void build_setsAllFields() {
    DashboardWidget widget = validBuilder().build();

    assertEquals("Revenue by Status", widget.getTitle());
    assertEquals(ChartType.BAR, widget.getChartType());
    assertSame(QUERY, widget.getQuery());
    assertEquals("status", widget.getLabelColumn());
    assertEquals(1, widget.getDatasets().size());
  }

  @Test
  void build_defaultChartOptions_areApplied() {
    DashboardWidget widget = validBuilder().build();
    assertNotNull(widget.getChartOptions());
    assertTrue(widget.getChartOptions().isResponsive());
  }

  @Test
  void build_customChartOptions_areStored() {
    ChartOptions opts = ChartOptions.builder().stacked(true).build();
    DashboardWidget widget = validBuilder().chartOptions(opts).build();
    assertTrue(widget.getChartOptions().isStacked());
  }

  @Test
  void build_multipleDatasets_areAllStored() {
    ChartDataset ds2 = ChartDataset.of("orders").label("Orders").build();
    DashboardWidget widget = validBuilder().dataset(ds2).build();
    assertEquals(2, widget.getDatasets().size());
  }

  @Test
  void datasets_areImmutable() {
    DashboardWidget widget = validBuilder().build();
    assertThrows(UnsupportedOperationException.class, () -> widget.getDatasets().add(DATASET));
  }

  @Test
  void build_missingTitle_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .chartType(ChartType.BAR)
                .query(QUERY)
                .labelColumn("status")
                .dataset(DATASET)
                .build());
  }

  @Test
  void build_blankTitle_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .title("  ")
                .chartType(ChartType.BAR)
                .query(QUERY)
                .labelColumn("status")
                .dataset(DATASET)
                .build());
  }

  @Test
  void build_missingChartType_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .title("Widget")
                .query(QUERY)
                .labelColumn("status")
                .dataset(DATASET)
                .build());
  }

  @Test
  void build_missingQuery_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .title("Widget")
                .chartType(ChartType.BAR)
                .labelColumn("status")
                .dataset(DATASET)
                .build());
  }

  @Test
  void build_missingLabelColumn_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .title("Widget")
                .chartType(ChartType.BAR)
                .query(QUERY)
                .dataset(DATASET)
                .build());
  }

  @Test
  void build_noDataset_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () ->
            DashboardWidget.builder()
                .title("Widget")
                .chartType(ChartType.BAR)
                .query(QUERY)
                .labelColumn("status")
                .build());
  }

  @Test
  void addDataset_null_isIgnored() {
    DashboardWidget widget = validBuilder().dataset(null).build();
    assertEquals(1, widget.getDatasets().size());
  }

  @com.aytmatech.querier.annotation.Table("fixture")
  private static class Fixture {
    public Long getId() {
      return 1L;
    }
  }
}
