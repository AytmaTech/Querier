package com.aytmatech.querier.dashboard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a single Chart.js dataset definition, mapping a result-set column to its visual
 * properties (label, colors, fill).
 *
 * <p>Usage (single color):
 *
 * <pre>
 *   ChartDataset.of("total_revenue")
 *       .label("Total Revenue")
 *       .backgroundColor("#4e79a7")
 *       .build();
 * </pre>
 *
 * <p>Usage (per-slice colors for PIE/DOUGHNUT):
 *
 * <pre>
 *   ChartDataset.of("revenue")
 *       .label("Revenue by Category")
 *       .backgroundColor("#4e79a7", "#f28e2b", "#e15759", "#76b7b2")
 *       .build();
 * </pre>
 */
public class ChartDataset {

  private final String column;
  private final String label;
  private final List<String> backgroundColor;
  private final List<String> borderColor;
  private final boolean fill;

  private ChartDataset(Builder builder) {
    this.column = builder.column;
    this.label = builder.label;
    this.backgroundColor = List.copyOf(builder.backgroundColor);
    this.borderColor = List.copyOf(builder.borderColor);
    this.fill = builder.fill;
  }

  /**
   * Creates a new builder targeting the given result-set column name.
   *
   * @param column the name of the column in the query result to use as data values
   * @return a new Builder for this dataset
   */
  public static Builder of(String column) {
    if (column == null || column.isBlank()) {
      throw new IllegalArgumentException("column must not be null or blank");
    }
    return new Builder(column);
  }

  /**
   * Returns the result-set column name this dataset is mapped to.
   *
   * @return the result-set column name this dataset is mapped to
   */
  public String getColumn() {
    return column;
  }

  /**
   * Returns the label shown in the chart legend.
   *
   * @return the label shown in the chart legend
   */
  public String getLabel() {
    return label;
  }

  /**
   * Background color(s). A single-element list applies one color to the whole dataset (BAR, LINE).
   * A multi-element list applies per-point colors (PIE, DOUGHNUT).
   *
   * @return the background color(s) for this dataset
   */
  public List<String> getBackgroundColor() {
    return backgroundColor;
  }

  /**
   * Border color(s), following the same single/multi-element convention.
   *
   * @return the border color(s) for this dataset
   */
  public List<String> getBorderColor() {
    return borderColor;
  }

  /**
   * Whether the area under a LINE chart should be filled.
   *
   * @return true if the area under a LINE chart should be filled, false otherwise
   */
  public boolean isFill() {
    return fill;
  }

  /**
   * Builder for {@link ChartDataset}. Use {@link ChartDataset#of(String)} to create a new builder
   * instance.
   */
  public static class Builder {

    private final String column;
    private String label = "";
    private final List<String> backgroundColor = new ArrayList<>();
    private final List<String> borderColor = new ArrayList<>();
    private boolean fill = false;

    private Builder(String column) {
      this.column = column;
    }

    /**
     * Sets the dataset label shown in the chart legend.
     *
     * @param label the legend label
     * @return this builder
     */
    public Builder label(String label) {
      this.label = label != null ? label : "";
      return this;
    }

    /**
     * Sets one or more background colors. Pass a single color for BAR/LINE charts; pass one color
     * per data point for PIE/DOUGHNUT.
     *
     * @param colors one or more CSS color strings (hex, rgb, rgba, named…)
     * @return this builder
     */
    public Builder backgroundColor(String... colors) {
      this.backgroundColor.addAll(Arrays.asList(colors));
      return this;
    }

    /**
     * Sets one or more border colors, following the same convention as backgroundColor.
     *
     * @param colors one or more CSS color strings
     * @return this builder
     */
    public Builder borderColor(String... colors) {
      this.borderColor.addAll(Arrays.asList(colors));
      return this;
    }

    /**
     * Controls whether the area under a LINE dataset is filled.
     *
     * @param fill true to fill the area under the line
     * @return this builder
     */
    public Builder fill(boolean fill) {
      this.fill = fill;
      return this;
    }

    /**
     * Builds the {@link ChartDataset}.
     *
     * @return a new ChartDataset instance
     */
    public ChartDataset build() {
      return new ChartDataset(this);
    }
  }
}
