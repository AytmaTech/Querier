package com.aytmatech.querier.ai.analytics;

import com.aytmatech.querier.Select;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.sql.SqlToSelectParser;
import com.aytmatech.querier.dashboard.ChartDataset;
import com.aytmatech.querier.dashboard.ChartType;
import com.aytmatech.querier.dashboard.Dashboard;
import com.aytmatech.querier.dashboard.DashboardLayout;
import com.aytmatech.querier.dashboard.DashboardWidget;

/**
 * Compiles a validated {@link DashboardPlan} into a Querier {@link Dashboard} using the real {@code
 * querier-ui} builder API.
 *
 * <p>Must only be called after {@link DashboardPlanValidator#validate} returns an empty issues
 * list.
 *
 * <p>Each widget's query is compiled in one of two ways depending on how it was built:
 *
 * <ul>
 *   <li>If {@link WidgetPlan#hasRawSql()} is {@code true} — the validated SQL string is wrapped
 *       directly via {@link Select#rawQuery(String)}. This is the SQL-first AI path.
 * </ul>
 */
public final class DashboardPlanCompiler {

  private DashboardPlanCompiler() {}

  /**
   * Compiles the plan into a {@link Dashboard}.
   *
   * @param plan a previously validated dashboard plan
   * @param context the schema context used during validation
   * @return a fully constructed {@link Dashboard} ready for rendering
   */
  public static Dashboard compile(DashboardPlan plan, AiSchemaContext context) {
    Dashboard.Builder dashboardBuilder =
        Dashboard.builder().title(plan.title()).layout(resolveLayout(plan.layout()));

    for (WidgetPlan widgetPlan : plan.widgets()) {
      dashboardBuilder.addWidget(compileWidget(widgetPlan, context));
    }

    return dashboardBuilder.build();
  }

  /**
   * Compiles a single {@link WidgetPlan} into a {@link DashboardWidget}.
   *
   * @param wp the widget plan to compile
   * @param context the schema context used during validation
   * @return a fully constructed {@link DashboardWidget} ready for rendering
   */
  private static DashboardWidget compileWidget(WidgetPlan wp, AiSchemaContext context) {
    Select query = SqlToSelectParser.parse(wp.rawSql().orElseThrow(), context);

    ChartType chartType = resolveChartType(wp.chartType());

    DashboardWidget.Builder widgetBuilder =
        DashboardWidget.builder()
            .title(wp.title())
            .chartType(chartType)
            .query(query)
            .labelColumn(wp.labelColumn());

    for (WidgetPlan.DatasetSpec ds : wp.datasets()) {
      ChartDataset.Builder dsBuilder = ChartDataset.of(ds.column()).label(ds.label());
      var dataset = dsBuilder.build();
      widgetBuilder.dataset(dataset);
    }

    return widgetBuilder.build();
  }

  /**
   * Resolves the chart type string from the plan into a {@link ChartType} enum.
   *
   * @param type the widget plan chart type string
   * @return the corresponding {@link ChartType} enum value
   */
  private static ChartType resolveChartType(String type) {
    return switch (type.toUpperCase()) {
      case "BAR" -> ChartType.BAR;
      case "HORIZONTAL_BAR" -> ChartType.HORIZONTAL_BAR;
      case "LINE" -> ChartType.LINE;
      case "PIE" -> ChartType.PIE;
      case "DOUGHNUT" -> ChartType.DOUGHNUT;
      case "RADAR" -> ChartType.RADAR;
      case "POLAR_AREA" -> ChartType.POLAR_AREA;
      case "SCATTER" -> ChartType.SCATTER;
      case "BUBBLE" -> ChartType.BUBBLE;
      default -> throw new IllegalArgumentException("Unsupported chart type: " + type);
    };
  }

  /**
   * Resolves the layout string from the plan into a {@link DashboardLayout} enum.
   *
   * @param layout the widget plan layout string
   * @return the corresponding {@link DashboardLayout} enum value
   */
  private static DashboardLayout resolveLayout(String layout) {
    return switch (layout.toUpperCase()) {
      case "SINGLE_COL" -> DashboardLayout.SINGLE_COL;
      case "GRID_3_COLS" -> DashboardLayout.GRID_3_COLS;
      case "MASONRY" -> DashboardLayout.MASONRY;
      default -> DashboardLayout.GRID_2_COLS;
    };
  }
}
