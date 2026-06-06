package com.aytmatech.querier.dashboard;

import com.aytmatech.querier.Select;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines a single chart widget within a {@link Dashboard}.
 *
 * <p>A widget binds a Querier {@link Select} query to a Chart.js chart type and describes how the
 * result-set columns map to axis labels and datasets.
 *
 * <p>Single-dataset example:
 *
 * <pre>
 *   DashboardWidget widget = DashboardWidget.builder()
 *       .title("Revenue by Status")
 *       .chartType(ChartType.BAR)
 *       .query(Select.builder()
 *           .select(Order::getStatus)
 *           .select(Aggregate.sum(Order::getTotal).as("total_revenue"))
 *           .from(Order.class)
 *           .groupBy(Order::getStatus)
 *           .build())
 *       .labelColumn("status")
 *       .dataset(ChartDataset.of("total_revenue")
 *           .label("Total Revenue")
 *           .backgroundColor("#4e79a7")
 *           .build())
 *       .build();
 * </pre>
 *
 * <p>Multi-dataset example:
 *
 * <pre>
 *   DashboardWidget widget = DashboardWidget.builder()
 *       .title("Revenue vs Orders per Month")
 *       .chartType(ChartType.BAR)
 *       .query(monthlyQuery)
 *       .labelColumn("month")
 *       .dataset(ChartDataset.of("revenue").label("Revenue").backgroundColor("#4e79a7").build())
 *       .dataset(ChartDataset.of("order_count").label("Orders").backgroundColor("#f28e2b").build())
 *       .build();
 * </pre>
 */
public class DashboardWidget {

  private final String title;
  private final ChartType chartType;
  private final Select query;
  private final String labelColumn;
  private final List<ChartDataset> datasets;
  private final ChartOptions chartOptions;

  private DashboardWidget(Builder builder) {
    this.title = builder.title;
    this.chartType = builder.chartType;
    this.query = builder.query;
    this.labelColumn = builder.labelColumn;
    this.datasets = List.copyOf(builder.datasets);
    this.chartOptions = builder.chartOptions;
  }

  /**
   * Returns the widget title displayed above the chart.
   *
   * @return the widget title displayed above the chart
   */
  public String getTitle() {
    return title;
  }

  /**
   * Returns the Chart.js chart type.
   *
   * @return the Chart.js chart type
   */
  public ChartType getChartType() {
    return chartType;
  }

  /**
   * Returns the Querier {@link Select} query used to fetch data for this chart.
   *
   * @return the Querier Select query used to fetch data for this chart
   */
  public Select getQuery() {
    return query;
  }

  /**
   * Returns the name of the result-set column whose values become the chart's axis labels.
   *
   * @return the result-set column used as the axis labels
   */
  public String getLabelColumn() {
    return labelColumn;
  }

  /**
   * Returns the dataset definitions that map result-set columns to Chart.js datasets. For
   * single-series charts, this list contains one entry; for multi-series charts, it contains one
   * entry per series.
   *
   * @return the list of dataset definitions (one per data series)
   */
  public List<ChartDataset> getDatasets() {
    return datasets;
  }

  /**
   * Returns the Chart.js options for this widget, controlling aspects like responsiveness, legend
   * position, axis titles, and stacking.
   *
   * @return the Chart.js options for this widget
   */
  public ChartOptions getChartOptions() {
    return chartOptions;
  }

  /**
   * Creates a new builder for DashboardWidget.
   *
   * @return a new Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link DashboardWidget}. */
  public static class Builder {

    private String title;
    private ChartType chartType;
    private Select query;
    private String labelColumn;
    private final List<ChartDataset> datasets = new ArrayList<>();
    private ChartOptions chartOptions = ChartOptions.builder().build();

    /** Creates a new Builder for DashboardWidget. */
    public Builder() {}

    /**
     * Sets the widget title displayed above the chart.
     *
     * @param title the widget title
     * @return this builder
     */
    public Builder title(String title) {
      this.title = title;
      return this;
    }

    /**
     * Sets the Chart.js chart type.
     *
     * @param chartType the chart type
     * @return this builder
     */
    public Builder chartType(ChartType chartType) {
      this.chartType = chartType;
      return this;
    }

    /**
     * Sets the Querier {@link Select} query that provides the data for this widget.
     *
     * @param query the Select query
     * @return this builder
     */
    public Builder query(Select query) {
      this.query = query;
      return this;
    }

    /**
     * Sets the result-set column whose values become the chart's axis labels.
     *
     * @param labelColumn the column name
     * @return this builder
     */
    public Builder labelColumn(String labelColumn) {
      this.labelColumn = labelColumn;
      return this;
    }

    /**
     * Adds a dataset definition. Call multiple times for multi-series charts.
     *
     * @param dataset the dataset to add
     * @return this builder
     */
    public Builder dataset(ChartDataset dataset) {
      if (dataset != null) this.datasets.add(dataset);
      return this;
    }

    /**
     * Sets custom Chart.js options for this widget. If not called, sensible defaults are applied
     * via {@link ChartOptions#builder()}.
     *
     * @param chartOptions the chart options
     * @return this builder
     */
    public Builder chartOptions(ChartOptions chartOptions) {
      this.chartOptions = chartOptions != null ? chartOptions : ChartOptions.builder().build();
      return this;
    }

    /**
     * Builds the {@link DashboardWidget}.
     *
     * @return a new DashboardWidget instance
     * @throws IllegalStateException if any required field is missing
     */
    public DashboardWidget build() {
      if (title == null || title.isBlank())
        throw new IllegalStateException("DashboardWidget.title is required");
      if (chartType == null)
        throw new IllegalStateException("DashboardWidget.chartType is required");
      if (query == null) throw new IllegalStateException("DashboardWidget.query is required");
      if (labelColumn == null || labelColumn.isBlank())
        throw new IllegalStateException("DashboardWidget.labelColumn is required");
      if (datasets.isEmpty())
        throw new IllegalStateException(
            "At least one dataset is required in DashboardWidget '" + title + "'");
      return new DashboardWidget(this);
    }
  }
}
