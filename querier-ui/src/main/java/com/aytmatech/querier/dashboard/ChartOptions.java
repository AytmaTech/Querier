package com.aytmatech.querier.dashboard;

/**
 * Represents the Chart.js {@code options} object for a single chart widget.
 *
 * <p>Supported options map directly to Chart.js configuration:
 *
 * <ul>
 *   <li>{@code responsive} → {@code options.responsive}
 *   <li>{@code maintainAspectRatio} → {@code options.maintainAspectRatio}
 *   <li>{@code aspectRatio} → {@code options.aspectRatio}
 *   <li>{@code resizeDelay} → {@code options.resizeDelay}
 *   <li>{@code autoPadding} → {@code options.layout.autoPadding}
 *   <li>{@code padding} → {@code options.layout.padding}
 *   <li>{@code subtitle} → {@code options.plugins.subtitle}
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
  private final boolean maintainAspectRatio;
  private final Double aspectRatio;
  private final Integer resizeDelay;
  private final boolean autoPadding;
  private final Integer padding;
  private final String subtitle;
  private final LegendPosition legendPosition;
  private final String xAxisLabel;
  private final String yAxisLabel;
  private final boolean stacked;

  private ChartOptions(Builder builder) {
    this.responsive = builder.responsive;
    this.maintainAspectRatio = builder.maintainAspectRatio;
    this.aspectRatio = builder.aspectRatio;
    this.resizeDelay = builder.resizeDelay;
    this.autoPadding = builder.autoPadding;
    this.padding = builder.padding;
    this.subtitle = builder.subtitle;
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
   * Whether to maintain the original canvas aspect ratio (width / height) when resizing.
   *
   * @return whether to maintain the original aspect ratio when resizing
   */
  public boolean isMaintainAspectRatio() {
    return maintainAspectRatio;
  }

  /**
   * The aspect ratio (width / height) of the chart.
   *
   * @return the aspect ratio (width / height) of the chart.
   */
  public Double getAspectRatio() {
    return aspectRatio;
  }

  /**
   * The delay in milliseconds after a resize event before the chart is redrawn.
   *
   * @return the delay in milliseconds after a resize event before the chart is redrawn (default:
   *     0).
   */
  public Integer getResizeDelay() {
    return resizeDelay;
  }

  /**
   * Controls whether to automatically adjust padding and maintain the same distance between chart
   * elements and the edge of the canvas when resizing.
   *
   * @return whether to automatically adjust padding and maintain the same distance between chart
   *     elements and the edge of the canvas when resizing (default: true).
   */
  public boolean isAutoPadding() {
    return autoPadding;
  }

  /**
   * The padding in pixels to apply on all sides of the chart.
   *
   * @return the padding in pixels to apply on all sides of the chart (default: 0).
   */
  public Integer getPadding() {
    return padding;
  }

  /**
   * The chart subtitle text. Displayed only when non-empty.
   *
   * @return the chart subtitle text.
   */
  public String getSubtitle() {
    return subtitle;
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
    private boolean maintainAspectRatio = true;
    private Double aspectRatio = null;
    private Integer resizeDelay = 0;
    private boolean autoPadding = true;
    private Integer padding = 0;
    private String subtitle = "";
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
     * Controls whether to maintain the original canvas aspect ratio (width / height) when resizing.
     *
     * @param maintainAspectRatio Whether to maintain the original canvas aspect ratio (width /
     *     height) when resizing.
     * @return this builder
     */
    public Builder maintainAspectRatio(boolean maintainAspectRatio) {
      this.maintainAspectRatio = maintainAspectRatio;
      return this;
    }

    /**
     * Sets the aspect ratio (width / height) of the chart. If null, the default aspect ratio is
     * used (1 for doughnut, pie, polarArea, radar charts and 2 for the other chart types).
     *
     * @param aspectRatio The aspect ratio (width / height) of the chart. If null, the default
     *     aspect ratio is used.
     * @return this builder
     */
    public Builder aspectRatio(Double aspectRatio) {
      this.aspectRatio = aspectRatio;
      return this;
    }

    /**
     * Controls the delay in milliseconds after a resize event before the chart is redrawn. This can
     * be useful to avoid excessive redraws during rapid resize events.
     *
     * @param resizeDelay The delay in milliseconds after a resize event before the chart is redrawn
     *     (default: 0).
     * @return this builder
     */
    public Builder resizeDelay(Integer resizeDelay) {
      this.resizeDelay = resizeDelay;
      return this;
    }

    /**
     * Controls whether to automatically adjust padding and maintain the same distance between chart
     * elements and the edge of the canvas when resizing.
     *
     * @param autoPadding Controls whether to automatically adjust padding and maintain the same
     *     distance between chart elements and the edge of the canvas when resizing (default: true).
     * @return this builder
     */
    public Builder autoPadding(Boolean autoPadding) {
      this.autoPadding = autoPadding;
      return this;
    }

    /**
     * Sets the padding in pixels to apply on all sides of the chart. This can be useful to ensure
     * that chart elements do not touch the edges of the canvas.
     *
     * @param padding The padding in pixels to apply on all sides of the chart (default: 0).
     * @return this builder
     */
    public Builder padding(Integer padding) {
      this.padding = padding;
      return this;
    }

    /**
     * Sets the chart subtitle text. Displayed only when non-empty.
     *
     * @param subtitle The chart subtitle text. Displayed only when non-empty.
     * @return this builder
     */
    public Builder subtitle(String subtitle) {
      this.subtitle = subtitle;
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
