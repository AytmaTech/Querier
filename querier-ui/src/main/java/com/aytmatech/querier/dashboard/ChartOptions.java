package com.aytmatech.querier.dashboard;

/**
 * Represents the Chart.js {@code options} object for a single chart widget.
 *
 * <p>Supported options map directly to Chart.js configuration:
 *
 * <ul>
 *   <li>{@code responsive} → {@code options.responsive}
 *   <li>{@code legendPosition} → {@code options.plugins.legend.position}
 *   <li>{@code xAxisLabel} → {@code options.scales.x.title.text}
 *   <li>{@code yAxisLabel} → {@code options.scales.y.title.text}
 *   <li>{@code stacked} → {@code options.scales.x.stacked} / {@code options.scales.y.stacked}
 * </ul>
 *
 * <p>Note: {@code scales} are omitted for chart types that do not use them (PIE, DOUGHNUT, RADAR,
 * POLAR_AREA).
 */
public class ChartOptions {

  private final boolean responsive;
  private final LegendPosition legendPosition;
  private final String xAxisLabel;
  private final String yAxisLabel;
  private final boolean stacked;

  private ChartOptions(Builder builder) {
    this.responsive = builder.responsive;
    this.legendPosition = builder.legendPosition;
    this.xAxisLabel = builder.xAxisLabel;
    this.yAxisLabel = builder.yAxisLabel;
    this.stacked = builder.stacked;
  }

  /**
   * Creates a new builder for ChartOptions with sensible defaults:
   *
   * @return a new Builder with sensible defaults
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Whether the chart should be responsive and resize with its container
   *
   * @return whether the chart should resize with its container
   */
  public boolean isResponsive() {
    return responsive;
  }

  /**
   * Returns the position of the chart legend
   *
   * @return the legend position
   */
  public LegendPosition getLegendPosition() {
    return legendPosition;
  }

  /**
   * Returns the X-axis title text, or empty string if none
   *
   * @return the X-axis title text, or empty string if none
   */
  public String getXAxisLabel() {
    return xAxisLabel;
  }

  /**
   * Returns the Y-axis title text, or empty string if none
   *
   * @return the Y-axis title text, or empty string if none
   */
  public String getYAxisLabel() {
    return yAxisLabel;
  }

  /**
   * Controls whether datasets should be stacked on both axes. Supported by BAR and LINE charts.
   *
   * @return whether both axes should be stacked
   */
  public boolean isStacked() {
    return stacked;
  }

  /**
   * Chart.js legend position values.
   *
   * @see <a
   *     href="https://www.chartjs.org/docs/latest/configuration/legend.html#legend-options">Chart.js
   *     Legend Options</a>
   */
  public enum LegendPosition {
    /** Top position of the chart legend. */
    TOP("top"),
    /** Bottom position of the chart legend. */
    BOTTOM("bottom"),
    /** Left position of the chart legend. */
    LEFT("left"),
    /** Right position of the chart legend. */
    RIGHT("right");

    private final String value;

    LegendPosition(String value) {
      this.value = value;
    }

    /**
     * Returns the corresponding Chart.js string value for this legend position.
     *
     * @see <a
     *     href="https://www.chartjs.org/docs/latest/configuration/legend.html#legend-options">Chart.js
     *     Legend Options</a>
     * @return the Chart.js string value
     */
    public String getValue() {
      return value;
    }
  }

  /** Builder for {@link ChartOptions}. */
  public static class Builder {

    private boolean responsive = true;
    private LegendPosition legendPosition = LegendPosition.TOP;
    private String xAxisLabel = "";
    private String yAxisLabel = "";
    private boolean stacked = false;

    /** Creates a new Builder with sensible defaults: */
    public Builder() {}

    /**
     * Controls whether the chart resizes responsively.
     *
     * @param responsive true to enable responsive mode (default: {@code true})
     * @return this builder
     */
    public Builder responsive(boolean responsive) {
      this.responsive = responsive;
      return this;
    }

    /**
     * Sets the legend position.
     *
     * @param legendPosition the position for the chart legend (default: {@code TOP})
     * @return this builder
     */
    public Builder legendPosition(LegendPosition legendPosition) {
      this.legendPosition = legendPosition != null ? legendPosition : LegendPosition.TOP;
      return this;
    }

    /**
     * Sets the X-axis title text. Displayed only when non-empty.
     *
     * @param xAxisLabel the X-axis label
     * @return this builder
     */
    public Builder xAxisLabel(String xAxisLabel) {
      this.xAxisLabel = xAxisLabel != null ? xAxisLabel : "";
      return this;
    }

    /**
     * Sets the Y-axis title text. Displayed only when non-empty.
     *
     * @param yAxisLabel the Y-axis label
     * @return this builder
     */
    public Builder yAxisLabel(String yAxisLabel) {
      this.yAxisLabel = yAxisLabel != null ? yAxisLabel : "";
      return this;
    }

    /**
     * Enables stacked mode on both axes.
     *
     * @param stacked true to stack datasets (default: {@code false})
     * @return this builder
     */
    public Builder stacked(boolean stacked) {
      this.stacked = stacked;
      return this;
    }

    /**
     * Builds the {@link ChartOptions} instance.
     *
     * @return a new {@link ChartOptions} instance
     */
    public ChartOptions build() {
      return new ChartOptions(this);
    }
  }
}
