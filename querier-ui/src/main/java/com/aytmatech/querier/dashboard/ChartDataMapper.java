package com.aytmatech.querier.dashboard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Maps a raw query result ({@code List<Map<String, Object>>}) into a {@link ChartData} instance
 * ready to be serialised as Chart.js JSON.
 *
 * <p>Both single-dataset and multi-dataset cases are handled uniformly: each {@link ChartDataset}
 * in the widget definition contributes one entry in {@link ChartData#getDatasets()}.
 *
 * <p>Example — single dataset (BAR chart):
 *
 * <pre>
 *   // Query returns: [{status="PAID", total=45000}, {status="SHIPPED", total=12000}]
 *   ChartData data = ChartDataMapper.map(rows, "status",
 *       List.of(ChartDataset.of("total").label("Revenue").backgroundColor("#4e79a7").build()));
 *   // labels  → ["PAID", "SHIPPED"]
 *   // dataset → { label:"Revenue", data:[45000, 12000], backgroundColor:["#4e79a7"] }
 * </pre>
 *
 * <p>Example — multi-dataset (grouped BAR chart):
 *
 * <pre>
 *   // Query returns: [{month="Jan", revenue=1000, orders=20}, ...]
 *   ChartData data = ChartDataMapper.map(rows, "month", List.of(
 *       ChartDataset.of("revenue").label("Revenue").backgroundColor("#4e79a7").build(),
 *       ChartDataset.of("orders").label("Orders").backgroundColor("#f28e2b").build()
 *   ));
 * </pre>
 */
public class ChartDataMapper {

  private ChartDataMapper() {}

  /**
   * Transforms query result rows into a {@link ChartData} object.
   *
   * <p>The {@code labelColumn} value from each row becomes an axis label. For every {@link
   * ChartDataset}, the corresponding column value from each row is collected as the numeric data
   * array.
   *
   * @param rows raw result from {@link QueryRunner#run(com.aytmatech.querier.Select)}
   * @param labelColumn the result-set column whose values become the axis labels
   * @param datasets one or more dataset definitions, each targeting a different column
   * @return a fully populated {@link ChartData} ready for rendering
   * @throws IllegalArgumentException if datasets is null or empty
   */
  public static ChartData map(
      List<Map<String, Object>> rows, String labelColumn, List<ChartDataset> datasets) {
    if (datasets == null || datasets.isEmpty()) {
      throw new IllegalArgumentException("At least one ChartDataset must be provided");
    }

    List<String> labels = new ArrayList<>(rows.size());
    for (Map<String, Object> row : rows) {
      Object labelVal = row.get(labelColumn);
      labels.add(labelVal != null ? labelVal.toString() : "");
    }

    List<ChartData.DatasetData> datasetDataList = new ArrayList<>(datasets.size());
    for (ChartDataset dataset : datasets) {
      List<Object> dataPoints = new ArrayList<>(rows.size());
      for (Map<String, Object> row : rows) {
        dataPoints.add(row.get(dataset.getColumn()));
      }
      datasetDataList.add(
          new ChartData.DatasetData(
              dataset.getLabel(),
              dataPoints,
              dataset.getBackgroundColor(),
              dataset.getBorderColor(),
              dataset.isFill()));
    }

    return new ChartData(labels, datasetDataList);
  }
}
