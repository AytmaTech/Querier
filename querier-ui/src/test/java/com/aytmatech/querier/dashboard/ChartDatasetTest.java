package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ChartDatasetTest {

  @Test
  void of_setsColumn() {
    ChartDataset ds = ChartDataset.of("revenue").build();
    assertEquals("revenue", ds.getColumn());
  }

  @Test
  void of_nullColumn_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> ChartDataset.of(null));
  }

  @Test
  void of_blankColumn_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> ChartDataset.of("  "));
  }

  @Test
  void label_setsLabel() {
    ChartDataset ds = ChartDataset.of("revenue").label("Total Revenue").build();
    assertEquals("Total Revenue", ds.getLabel());
  }

  @Test
  void label_defaultsToEmpty() {
    ChartDataset ds = ChartDataset.of("revenue").build();
    assertEquals("", ds.getLabel());
  }

  @Test
  void label_null_fallsBackToEmpty() {
    ChartDataset ds = ChartDataset.of("revenue").label(null).build();
    assertEquals("", ds.getLabel());
  }

  @Test
  void backgroundColor_singleColor() {
    ChartDataset ds = ChartDataset.of("revenue").backgroundColor("#4e79a7").build();
    assertEquals(1, ds.getBackgroundColor().size());
    assertEquals("#4e79a7", ds.getBackgroundColor().get(0));
  }

  @Test
  void backgroundColor_multipleColors() {
    ChartDataset ds =
        ChartDataset.of("revenue").backgroundColor("#4e79a7", "#f28e2b", "#e15759").build();
    assertEquals(3, ds.getBackgroundColor().size());
  }

  @Test
  void borderColor_setsColors() {
    ChartDataset ds = ChartDataset.of("revenue").borderColor("#000000").build();
    assertEquals(1, ds.getBorderColor().size());
    assertEquals("#000000", ds.getBorderColor().get(0));
  }

  @Test
  void fill_defaultsFalse() {
    ChartDataset ds = ChartDataset.of("revenue").build();
    assertFalse(ds.isFill());
  }

  @Test
  void fill_setsTrue() {
    ChartDataset ds = ChartDataset.of("revenue").fill(true).build();
    assertTrue(ds.isFill());
  }

  @Test
  void backgroundColor_isImmutable() {
    ChartDataset ds = ChartDataset.of("revenue").backgroundColor("#4e79a7").build();
    assertThrows(UnsupportedOperationException.class, () -> ds.getBackgroundColor().add("#ffffff"));
  }
}
