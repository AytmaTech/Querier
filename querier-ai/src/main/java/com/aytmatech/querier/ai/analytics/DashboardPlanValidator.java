package com.aytmatech.querier.ai.analytics;

import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.sql.SqlSchemaValidator;
import com.aytmatech.querier.dashboard.ChartType;
import com.aytmatech.querier.dashboard.DashboardLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates a {@link DashboardPlan} and each of its {@link WidgetPlan}s against the allowed {@link
 * AiSchemaContext}.
 *
 * <p>Per-widget query validation uses two paths:
 *
 * <ul>
 *   <li>Raw SQL widgets ({@link WidgetPlan#hasRawSql()}) — validated by {@link SqlSchemaValidator}.
 * </ul>
 */
public final class DashboardPlanValidator {

  private static final Set<String> VALID_CHART_TYPES =
      Arrays.stream(ChartType.values()).map(ChartType::name).collect(Collectors.toSet());

  private static final Set<String> VALID_LAYOUTS =
      Arrays.stream(DashboardLayout.values())
          .map(DashboardLayout::name)
          .collect(Collectors.toSet());

  private DashboardPlanValidator() {}

  /**
   * Validates the entire dashboard plan.
   *
   * @param plan the dashboard plan to validate
   * @param context the allowed schema scope
   * @return a list of human-readable issues; empty means the plan is valid
   */
  public static List<String> validate(DashboardPlan plan, AiSchemaContext context) {
    List<String> issues = new ArrayList<>();

    if (plan.title() == null || plan.title().isBlank()) {
      issues.add("Dashboard title must not be blank");
    }

    if (!VALID_LAYOUTS.contains(plan.layout().toUpperCase())) {
      issues.add("Unknown dashboard layout: " + plan.layout());
    }

    if (plan.widgets().isEmpty()) {
      issues.add("Dashboard must contain at least one widget");
    }

    for (int i = 0; i < plan.widgets().size(); i++) {
      WidgetPlan widget = plan.widgets().get(i);
      String prefix = "Widget[" + i + "] '" + widget.title() + "': ";

      if (!VALID_CHART_TYPES.contains(widget.chartType().toUpperCase())) {
        issues.add(prefix + "unknown chart type '" + widget.chartType() + "'");
      }

      if (widget.labelColumn() == null || widget.labelColumn().isBlank()) {
        issues.add(prefix + "labelColumn must not be blank");
      }

      if (widget.datasets().isEmpty()) {
        issues.add(prefix + "must have at least one dataset");
      } else {
        validateWidgetDatasets(widget.datasets(), prefix, issues);
      }

      validateChartResultCompatibility(widget, prefix, issues);

      if (widget.hasRawSql()) {
        List<String> sqlIssues =
            SqlSchemaValidator.validate(widget.rawSql().orElseThrow(), context);
        for (String si : sqlIssues) {
          issues.add(prefix + si);
        }
      }
    }

    return issues;
  }

  /**
   * Validates that the datasets of a widget are compatible with the chart type.
   *
   * @param datasets the datasets to validate
   * @param prefix a string to prepend to each issue for context
   * @param issues the list to which issues will be added
   */
  private static void validateWidgetDatasets(
      List<WidgetPlan.DatasetSpec> datasets, String prefix, List<String> issues) {
    for (int j = 0; j < datasets.size(); j++) {
      WidgetPlan.DatasetSpec dataset = datasets.get(j);
      String datasetPrefix = prefix + "Dataset[" + j + "]: ";

      if (dataset.column() == null || dataset.column().isBlank()) {
        issues.add(datasetPrefix + "column must not be blank");
      }

      if (dataset.label() == null || dataset.label().isBlank()) {
        issues.add(datasetPrefix + "label must not be blank");
      }
    }
  }

  /**
   * Validates that the datasets of a widget are compatible with the chart type.
   *
   * @param widget the widget to validate
   * @param prefix a string to prepend to each issue for context
   * @param issues the list to which issues will be added
   */
  private static void validateChartResultCompatibility(
      WidgetPlan widget, String prefix, List<String> issues) {
    String type = widget.chartType().toUpperCase();
    int datasetCount = widget.datasets().size();

    switch (type) {
      case "PIE", "DOUGHNUT", "POLAR_AREA" -> {
        if (datasetCount != 1) {
          issues.add(prefix + type + " chart supports exactly one dataset, got " + datasetCount);
        }
      }
      case "SCATTER", "BUBBLE" -> {
        if (!widget.hasRawSql()) {
          issues.add(prefix + type + " chart requires at least two numeric fields");
        }
      }
      default -> {
        // BAR, HORIZONTAL_BAR, LINE, RADAR — one or more datasets are fine
      }
    }
  }
}
