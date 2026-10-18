package com.aytmatech.querier.ai.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * AI-facing plan for a single dashboard widget.
 *
 * <p>Each widget has a purpose, a resolved chart type, a query for its data, and display
 * configuration (label column, dataset columns, dataset labels).
 *
 * <p>The widget's data query is held in one of two forms — exactly one must be non-null:
 *
 * <ul>
 *   <li>{@link #rawSql()} — a complete, validated SQL string (SQL-first AI path). When present,
 *       {@link com.aytmatech.querier.ai.analytics.DashboardPlanCompiler} will call {@link
 *       com.aytmatech.querier.Select#rawQuery(String)} directly.
 * </ul>
 */
public final class WidgetPlan {

  private final String title;
  private final String chartType;
  private final String rawSql;
  private final String labelColumn;
  private final List<DatasetSpec> datasets;

  private WidgetPlan(Builder b) {
    this.title = Objects.requireNonNull(b.title, "title");
    this.chartType = Objects.requireNonNull(b.chartType, "chartType");
    this.labelColumn = Objects.requireNonNull(b.labelColumn, "labelColumn");
    this.datasets = Collections.unmodifiableList(new ArrayList<>(b.datasets));
    this.rawSql = b.rawSql;
  }

  /**
   * Creates a new {@link Builder} for constructing a {@link WidgetPlan}.
   *
   * @return A new {@link Builder} instance for constructing a {@link WidgetPlan}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Widget title, for display in the dashboard.
   *
   * @return Widget title shown in the dashboard card header.
   */
  public String title() {
    return title;
  }

  /**
   * Widget chart type, for display in the dashboard.
   *
   * @return Chart type name: one of {@code BAR}, {@code HORIZONTAL_BAR}, {@code LINE}, {@code PIE},
   *     {@code DOUGHNUT}, {@code SCATTER}.
   * @see com.aytmatech.querier.dashboard.ChartType
   */
  public String chartType() {
    return chartType;
  }

  /**
   * The raw SQL query for this widget, if the AI produced a valid SQL string. If present, this is
   * used directly to fetch the widget's data.
   *
   * @return The raw, validated SQL string powering this widget
   */
  public Optional<String> rawSql() {
    return Optional.ofNullable(rawSql);
  }

  /**
   * Returns weather the widget has a raw SQL query.
   *
   * @return Returns {@code true} if this widget carries a raw SQL string rather than a QueryPlan.
   */
  public boolean hasRawSql() {
    return rawSql != null;
  }

  /**
   * The result column that provides the labels for the chart's X-axis or categories.
   *
   * @return The result column used as the chart X-axis / label axis.
   */
  public String labelColumn() {
    return labelColumn;
  }

  /**
   * Widget's datasets, each with a column and a label for display in the chart legend.
   *
   * @return Dataset specifications — each corresponds to one data series in the chart.
   */
  public List<DatasetSpec> datasets() {
    return datasets;
  }

  /**
   * One data series in the chart.
   *
   * @param column the result column providing numeric values
   * @param label legend label for this series
   */
  public record DatasetSpec(String column, String label) {}

  /** Builder for constructing a {@link WidgetPlan} instance. */
  public static final class Builder {
    private String title;
    private String chartType;
    private String rawSql;
    private String labelColumn;
    private final List<DatasetSpec> datasets = new ArrayList<>();

    private Builder() {}

    /**
     * Sets the widget title.
     *
     * @param title the widget title shown in the dashboard card header
     * @return this builder
     */
    public Builder title(String title) {
      this.title = title;
      return this;
    }

    /**
     * Sets the widget chart type.
     *
     * @param chartType the chart type name: one of {@code BAR}, {@code HORIZONTAL_BAR}, {@code
     *     LINE}, {@code PIE}, {@code DOUGHNUT}, {@code SCATTER}.
     * @see com.aytmatech.querier.dashboard.ChartType
     * @return this builder
     */
    public Builder chartType(String chartType) {
      this.chartType = chartType;
      return this;
    }

    /**
     * Sets the raw SQL query for this widget. If present, this is used directly to fetch the
     * widget's data.
     *
     * @param rawSql the raw, validated SQL string powering this widget
     * @return this builder
     */
    public Builder rawSql(String rawSql) {
      this.rawSql = rawSql;
      return this;
    }

    /**
     * Sets lablel column for the chart's X-axis or categories.
     *
     * @param labelColumn the result column used as the chart X-axis / label axis
     * @return this builder
     */
    public Builder labelColumn(String labelColumn) {
      this.labelColumn = labelColumn;
      return this;
    }

    /**
     * Adds a dataset specification for this widget.
     *
     * @param dataset specification for one data series in the chart
     * @return this builder
     */
    public Builder dataset(DatasetSpec dataset) {
      this.datasets.add(dataset);
      return this;
    }

    /**
     * Builds the WidgetPlan instance.
     *
     * @return a new {@link WidgetPlan} instance
     */
    public WidgetPlan build() {
      return new WidgetPlan(this);
    }
  }

  /**
   * Builds a textual representation of the widget plan for AI prompt generation.
   *
   * @return a textual representation of this widget plan, suitable for prompting an AI model
   */
  public String toPromptText() {
    StringBuilder sb = new StringBuilder();
    sb.append("Widget Title: ").append(title).append("\n");
    sb.append("Chart Type: ").append(chartType).append("\n");
    sb.append("Label Column: ").append(labelColumn).append("\n");
    sb.append("Datasets:\n");
    for (DatasetSpec dataset : datasets) {
      sb.append("  - Column: ")
          .append(dataset.column())
          .append(", Label: ")
          .append(dataset.label())
          .append("\n");
    }
    if (rawSql != null) {
      sb.append("Raw SQL: ").append(rawSql).append("\n");
    }
    return sb.toString();
  }
}
