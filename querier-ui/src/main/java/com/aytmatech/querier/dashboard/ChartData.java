package com.aytmatech.querier.dashboard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the Chart.js {@code data} object, containing the axis labels and one or more datasets.
 *
 * <p>This is the Java model produced by {@link ChartDataMapper} and consumed by {@link
 * DashboardRenderer} when generating the inline JavaScript.
 */
public class ChartData {

  private final List<String> labels;
  private final List<DatasetData> datasets;

  /**
   * Creates a ChartData instance.
   *
   * @param labels the axis labels (e.g., category names, dates)
   * @param datasets one or more datasets whose {@code data} arrays align with {@code labels}
   */
  public ChartData(List<String> labels, List<DatasetData> datasets) {
    this.labels = List.copyOf(labels);
    this.datasets = List.copyOf(datasets);
  }

  /**
   * Returns the axis labels
   *
   * @return the list of axis labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * Returns the datasets
   *
   * @return the list of datasets
   */
  public List<DatasetData> getDatasets() {
    return datasets;
  }

  /**
   * Immutable record representing a single Chart.js dataset entry inside the {@code data.datasets}
   * array.
   *
   * @param label legend label for this dataset
   * @param data ordered list of numeric values aligned to {@link ChartData#getLabels()}
   * @param backgroundColor background color(s); single element for solid, multi for per-point
   * @param borderColor border color(s)
   * @param fill whether to fill the area under a line dataset
   */
  public record DatasetData(
      String label,
      List<Object> data,
      List<String> backgroundColor,
      List<String> borderColor,
      boolean fill) {
    /**
     * Creates a DatasetData instance.
     *
     * @param label legend label for this dataset
     * @param data ordered list of numeric values aligned to {@link ChartData#getLabels()}
     * @param backgroundColor background color(s); single element for solid, multi for per-point
     * @param borderColor border color(s)
     * @param fill whether to fill the area under a line dataset
     */
    public DatasetData {
      data = Collections.unmodifiableList(new ArrayList<>(data));
      backgroundColor = Collections.unmodifiableList(new ArrayList<>(backgroundColor));
      borderColor = Collections.unmodifiableList(new ArrayList<>(borderColor));
    }
  }
}
