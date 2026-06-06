package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class ChartDataTest {

  @Test
  void constructor_storesLabelsAndDatasets() {
    List<String> labels = List.of("PAID", "SHIPPED");
    List<ChartData.DatasetData> datasets =
        List.of(
            new ChartData.DatasetData(
                "Revenue", List.of(1000, 500), List.of("#4e79a7"), List.of(), false));

    ChartData chartData = new ChartData(labels, datasets);

    assertEquals(labels, chartData.getLabels());
    assertEquals(datasets, chartData.getDatasets());
  }

  @Test
  void labels_areImmutable() {
    ChartData chartData =
        new ChartData(
            List.of("A"),
            List.of(new ChartData.DatasetData("d", List.of(1), List.of(), List.of(), false)));
    assertThrows(UnsupportedOperationException.class, () -> chartData.getLabels().add("B"));
  }

  @Test
  void datasets_areImmutable() {
    ChartData chartData =
        new ChartData(
            List.of("A"),
            List.of(new ChartData.DatasetData("d", List.of(1), List.of(), List.of(), false)));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            chartData
                .getDatasets()
                .add(new ChartData.DatasetData("x", List.of(), List.of(), List.of(), false)));
  }

  @Test
  void datasetData_dataIsImmutable() {
    ChartData.DatasetData ds =
        new ChartData.DatasetData(
            "Revenue", List.of(100, 200), List.of("#fff"), List.of("#000"), true);
    assertThrows(UnsupportedOperationException.class, () -> ds.data().add(999));
  }

  @Test
  void datasetData_storesAllFields() {
    ChartData.DatasetData ds =
        new ChartData.DatasetData(
            "Revenue", List.of(100), List.of("#4e79a7"), List.of("#000"), true);

    assertEquals("Revenue", ds.label());
    assertEquals(List.of(100), ds.data());
    assertEquals(List.of("#4e79a7"), ds.backgroundColor());
    assertEquals(List.of("#000"), ds.borderColor());
    assertTrue(ds.fill());
  }
}
