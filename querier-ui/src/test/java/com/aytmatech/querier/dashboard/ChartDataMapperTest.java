package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChartDataMapperTest {

  private static Map<String, Object> row(String label, Object value) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("status", label);
    row.put("total", value);
    return row;
  }

  private static final List<ChartDataset> SINGLE_DATASET =
      List.of(ChartDataset.of("total").label("Revenue").backgroundColor("#4e79a7").build());

  @Test
  void map_extractsLabelsFromLabelColumn() {
    List<Map<String, Object>> rows = List.of(row("PAID", 1000), row("SHIPPED", 500));

    ChartData result = ChartDataMapper.map(rows, "status", SINGLE_DATASET);

    assertEquals(List.of("PAID", "SHIPPED"), result.getLabels());
  }

  @Test
  void map_extractsDataPointsForDataset() {
    List<Map<String, Object>> rows = List.of(row("PAID", 1000), row("SHIPPED", 500));

    ChartData result = ChartDataMapper.map(rows, "status", SINGLE_DATASET);

    assertEquals(1, result.getDatasets().size());
    assertEquals(List.of(1000, 500), result.getDatasets().get(0).data());
  }

  @Test
  void map_setsDatasetLabelAndColors() {
    List<Map<String, Object>> rows = List.of(row("PAID", 100));

    ChartData result = ChartDataMapper.map(rows, "status", SINGLE_DATASET);
    ChartData.DatasetData ds = result.getDatasets().get(0);

    assertEquals("Revenue", ds.label());
    assertEquals(List.of("#4e79a7"), ds.backgroundColor());
  }

  @Test
  void map_emptyRows_producesEmptyLabelsAndData() {
    ChartData result = ChartDataMapper.map(Collections.emptyList(), "status", SINGLE_DATASET);

    assertTrue(result.getLabels().isEmpty());
    assertTrue(result.getDatasets().get(0).data().isEmpty());
  }

  @Test
  void map_nullLabelValue_usesEmptyString() {
    Map<String, Object> rowWithNull = new LinkedHashMap<>();
    rowWithNull.put("status", null);
    rowWithNull.put("total", 100);

    ChartData result = ChartDataMapper.map(List.of(rowWithNull), "status", SINGLE_DATASET);

    assertEquals("", result.getLabels().get(0));
  }

  @Test
  void map_multiDataset_producesOneDatasetPerDefinition() {
    List<Map<String, Object>> rows =
        List.of(
            Map.of("month", "Jan", "revenue", 1000, "orders", 20),
            Map.of("month", "Feb", "revenue", 1500, "orders", 30));
    List<ChartDataset> datasets =
        List.of(
            ChartDataset.of("revenue").label("Revenue").build(),
            ChartDataset.of("orders").label("Orders").build());

    ChartData result = ChartDataMapper.map(rows, "month", datasets);

    assertEquals(2, result.getDatasets().size());
    assertEquals("Revenue", result.getDatasets().get(0).label());
    assertEquals("Orders", result.getDatasets().get(1).label());
  }

  @Test
  void map_multiDataset_eachDatasetHasCorrectValues() {
    List<Map<String, Object>> rows =
        List.of(
            Map.of("month", "Jan", "revenue", 1000, "orders", 20),
            Map.of("month", "Feb", "revenue", 1500, "orders", 30));
    List<ChartDataset> datasets =
        List.of(
            ChartDataset.of("revenue").label("Revenue").build(),
            ChartDataset.of("orders").label("Orders").build());

    ChartData result = ChartDataMapper.map(rows, "month", datasets);

    assertEquals(List.of(1000, 1500), result.getDatasets().get(0).data());
    assertEquals(List.of(20, 30), result.getDatasets().get(1).data());
  }

  @Test
  void map_nullDatasets_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class, () -> ChartDataMapper.map(List.of(), "status", null));
  }

  @Test
  void map_emptyDatasets_throwsIllegalArgument() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ChartDataMapper.map(List.of(), "status", Collections.emptyList()));
  }

  @Test
  void map_missingDataColumn_producesNullDataPoint() {
    List<Map<String, Object>> rows = List.of(Map.of("status", "PAID"));
    List<ChartDataset> datasets = List.of(ChartDataset.of("missing_col").label("Missing").build());

    ChartData result = ChartDataMapper.map(rows, "status", datasets);

    assertNull(result.getDatasets().get(0).data().get(0));
  }
}
