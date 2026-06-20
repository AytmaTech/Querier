package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.Select;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DashboardRendererTest {

  /** QueryRunner that always returns two rows without touching a database. */
  private static final QueryRunner STUB_RUNNER =
      select ->
          List.of(
              Map.of("status", "PAID", "total", 45000),
              Map.of("status", "SHIPPED", "total", 12000));

  private Dashboard dashboard;

  @BeforeEach
  void setUp() {
    DashboardWidget widget =
        DashboardWidget.builder()
            .title("Revenue by Status")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").label("Revenue").backgroundColor("#4e79a7").build())
            .build();

    dashboard =
        Dashboard.builder()
            .title("Sales Dashboard")
            .layout(DashboardLayout.GRID_2_COLS)
            .addWidget(widget)
            .build();
  }

  private DashboardRenderer renderer() {
    return DashboardRenderer.builder().dashboard(dashboard).queryRunner(STUB_RUNNER).build();
  }

  @Test
  void renderFragment_doesNotContainHtmlTag() {
    String fragment = renderer().renderFragment();
    assertFalse(fragment.contains("<html"));
    assertFalse(fragment.contains("<!DOCTYPE"));
  }

  @Test
  void renderFragment_containsChartJsScriptTag() {
    assertTrue(renderer().renderFragment().contains("<script src="));
  }

  @Test
  void renderFragment_omitScriptElement() {
    String html =
        DashboardRenderer.builder()
            .dashboard(dashboard)
            .queryRunner(STUB_RUNNER)
            .omitChartJsScriptElement(true)
            .build()
            .renderFragment();
    assertFalse(html.contains("https://cdn.jsdelivr.net/npm/chart.js"));
  }

  @Test
  void renderFragment_containsCanvasElement() {
    assertTrue(renderer().renderFragment().contains("<canvas id=\"querier-chart-0\""));
  }

  @Test
  void renderFragment_containsDashboardTitleComment() {
    assertTrue(renderer().renderFragment().contains("Sales Dashboard"));
  }

  @Test
  void renderHtml_barChart_typeIsBar() {
    assertTrue(renderer().renderHtml().contains("type: 'bar'"));
  }

  @Test
  void renderHtml_horizontalBarChart_typeIsBarWithIndexAxis() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("H-Bar")
            .chartType(ChartType.HORIZONTAL_BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("type: 'bar'"));
    assertTrue(html.contains("indexAxis: 'y'"));
  }

  @Test
  void renderHtml_pieChart_noScalesBlock() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("Pie")
            .chartType(ChartType.PIE)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertFalse(html.contains("scales:"));
  }

  @Test
  void renderHtml_lineChart_typeIsLine() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("Line")
            .chartType(ChartType.LINE)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("type: 'line'"));
  }

  @Test
  void renderHtml_xAxisLabel_appearsInScales() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().xAxisLabel("Status").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("'Status'"));
  }

  @Test
  void build_missingDashboard_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () -> DashboardRenderer.builder().queryRunner(STUB_RUNNER).build());
  }

  @Test
  void build_missingQueryRunner_throwsIllegalState() {
    assertThrows(
        IllegalStateException.class,
        () -> DashboardRenderer.builder().dashboard(dashboard).build());
  }

  @Test
  void renderHtml_containsDocTypeDeclaration() {
    assertTrue(renderer().renderHtml().startsWith("<!DOCTYPE html>"));
  }

  @Test
  void renderHtml_containsDashboardTitle() {
    assertTrue(renderer().renderHtml().contains("Sales Dashboard"));
  }

  @Test
  void renderHtml_containsDefaultChartJsCdnUrl() {
    assertTrue(renderer().renderHtml().contains("https://cdn.jsdelivr.net/npm/chart.js"));
  }

  @Test
  void renderHtml_containsCustomChartJsUrl() {
    String html =
        DashboardRenderer.builder()
            .dashboard(dashboard)
            .queryRunner(STUB_RUNNER)
            .chartJsUrl("/static/js/chart.umd.min.js")
            .build()
            .renderHtml();
    assertTrue(html.contains("/static/js/chart.umd.min.js"));
    assertFalse(html.contains("cdn.jsdelivr.net"));
  }

  @Test
  void renderHtml_blankChartJsUrl_fallsBackToCdn() {
    String html =
        DashboardRenderer.builder()
            .dashboard(dashboard)
            .queryRunner(STUB_RUNNER)
            .chartJsUrl("   ")
            .build()
            .renderHtml();
    assertTrue(html.contains("cdn.jsdelivr.net"));
  }

  @Test
  void renderHtml_omitScriptElement() {
    String html =
        DashboardRenderer.builder()
            .dashboard(dashboard)
            .queryRunner(STUB_RUNNER)
            .omitChartJsScriptElement(true)
            .build()
            .renderHtml();
    assertFalse(html.contains("https://cdn.jsdelivr.net/npm/chart.js"));
  }

  @Test
  void renderHtml_containsCanvasForEachWidget() {
    assertTrue(renderer().renderHtml().contains("<canvas id=\"querier-chart-0\""));
  }

  @Test
  void renderHtml_containsWidgetTitle() {
    assertTrue(renderer().renderHtml().contains("Revenue by Status"));
  }

  @Test
  void renderHtml_containsChartJsInitCall() {
    assertTrue(renderer().renderHtml().contains("new Chart("));
  }

  @Test
  void renderHtml_containsLabelsFromQueryResult() {
    String html = renderer().renderHtml();
    assertTrue(html.contains("'PAID'"));
    assertTrue(html.contains("'SHIPPED'"));
  }

  @Test
  void renderHtml_containsDataValuesFromQueryResult() {
    String html = renderer().renderHtml();
    assertTrue(html.contains("45000"));
    assertTrue(html.contains("12000"));
  }

  @Test
  void renderHtml_containsGrid2ColsClass() {
    assertTrue(renderer().renderHtml().contains("querier-grid--2"));
  }

  @Test
  void renderHtml_singleColLayout_containsGrid1Class() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();

    Dashboard d =
        Dashboard.builder().title("T").layout(DashboardLayout.SINGLE_COL).addWidget(w).build();

    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("querier-grid--1"));
  }

  @Test
  void renderHtml_stacked_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().stacked(true).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("stacked: true"));
  }

  @Test
  void renderHtml_twoWidgets_twoCanvasElements() {
    DashboardWidget w2 =
        DashboardWidget.builder()
            .title("Orders")
            .chartType(ChartType.PIE)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();

    Dashboard d =
        Dashboard.builder()
            .addWidget(
                DashboardWidget.builder()
                    .title("Revenue")
                    .chartType(ChartType.BAR)
                    .query(Select.builder().from(Fixture.class).build())
                    .labelColumn("status")
                    .dataset(ChartDataset.of("total").build())
                    .build())
            .addWidget(w2)
            .build();

    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("querier-chart-0"));
    assertTrue(html.contains("querier-chart-1"));
  }

  @Test
  void renderHtml_titleWithSpecialChars_isEscaped() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("Revenue <Test> & \"Quotes\"")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .build();
    Dashboard d = Dashboard.builder().title("My <Dashboard>").addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("My &lt;Dashboard&gt;"));
    assertFalse(html.contains("My <Dashboard>"));
  }

  @Test
  void renderHtml_maintainAspectRatio_defaultTrue_appearsInOptions() {
    assertTrue(renderer().renderHtml().contains("maintainAspectRatio: true"));
  }

  @Test
  void renderHtml_maintainAspectRatio_false_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().maintainAspectRatio(false).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("maintainAspectRatio: false"));
  }

  @Test
  void renderHtml_aspectRatio_customValue_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().aspectRatio(3.0d).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("aspectRatio: 3.0"));
  }

  @Test
  void renderHtml_aspectRatio_null_barChart_defaultsTo2() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().aspectRatio(null).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("aspectRatio: 2"));
  }

  @Test
  void renderHtml_aspectRatio_null_pieChart_defaultsTo1() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("Pie")
            .chartType(ChartType.PIE)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().aspectRatio(null).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("aspectRatio: 1"));
  }

  @Test
  void renderHtml_aspectRatio_null_doughnutChart_defaultsTo1() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("Doughnut")
            .chartType(ChartType.DOUGHNUT)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().aspectRatio(null).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("aspectRatio: 1"));
  }

  @Test
  void renderHtml_resizeDelay_defaultZero_appearsInOptions() {
    assertTrue(renderer().renderHtml().contains("resizeDelay: 0"));
  }

  @Test
  void renderHtml_resizeDelay_customValue_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().resizeDelay(150).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("resizeDelay: 150"));
  }

  @Test
  void renderHtml_autoPadding_defaultTrue_appearsInOptions() {
    assertTrue(renderer().renderHtml().contains("autoPadding: true"));
  }

  @Test
  void renderHtml_autoPadding_false_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().autoPadding(false).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("autoPadding: false"));
  }

  @Test
  void renderHtml_padding_defaultZero_appearsInOptions() {
    assertTrue(renderer().renderHtml().contains("padding: 0"));
  }

  @Test
  void renderHtml_padding_customValue_appearsInOptions() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().padding(20).build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("padding: 20"));
  }

  @Test
  void renderHtml_subtitle_whenSet_appearsInPlugins() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().subtitle("Q1 2026").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertTrue(html.contains("subtitle: { display: true"));
    assertTrue(html.contains("'Q1 2026'"));
  }

  @Test
  void renderHtml_subtitle_whenEmpty_subtitleBlockAbsent() {
    DashboardWidget w =
        DashboardWidget.builder()
            .title("W")
            .chartType(ChartType.BAR)
            .query(Select.builder().from(Fixture.class).build())
            .labelColumn("status")
            .dataset(ChartDataset.of("total").build())
            .chartOptions(ChartOptions.builder().subtitle("").build())
            .build();
    Dashboard d = Dashboard.builder().addWidget(w).build();
    String html =
        DashboardRenderer.builder().dashboard(d).queryRunner(STUB_RUNNER).build().renderHtml();

    assertFalse(html.contains("subtitle: { display: true"));
  }

  @com.aytmatech.querier.annotation.Table("fixture")
  private static class Fixture {
    public Long getId() {
      return 1L;
    }
  }
}
