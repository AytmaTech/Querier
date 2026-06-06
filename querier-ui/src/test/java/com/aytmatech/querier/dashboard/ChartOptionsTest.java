package com.aytmatech.querier.dashboard;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ChartOptionsTest {

  @Test
  void defaults_areCorrect() {
    ChartOptions opts = ChartOptions.builder().build();

    assertTrue(opts.isResponsive());
    assertEquals(ChartOptions.LegendPosition.TOP, opts.getLegendPosition());
    assertEquals("", opts.getXAxisLabel());
    assertEquals("", opts.getYAxisLabel());
    assertFalse(opts.isStacked());
  }

  @Test
  void responsive_canBeDisabled() {
    ChartOptions opts = ChartOptions.builder().responsive(false).build();
    assertFalse(opts.isResponsive());
  }

  @Test
  void legendPosition_setsPosition() {
    ChartOptions opts =
        ChartOptions.builder().legendPosition(ChartOptions.LegendPosition.BOTTOM).build();
    assertEquals(ChartOptions.LegendPosition.BOTTOM, opts.getLegendPosition());
  }

  @Test
  void legendPosition_null_fallsBackToTop() {
    ChartOptions opts = ChartOptions.builder().legendPosition(null).build();
    assertEquals(ChartOptions.LegendPosition.TOP, opts.getLegendPosition());
  }

  @Test
  void xAxisLabel_setsLabel() {
    ChartOptions opts = ChartOptions.builder().xAxisLabel("Month").build();
    assertEquals("Month", opts.getXAxisLabel());
  }

  @Test
  void xAxisLabel_null_fallsBackToEmpty() {
    ChartOptions opts = ChartOptions.builder().xAxisLabel(null).build();
    assertEquals("", opts.getXAxisLabel());
  }

  @Test
  void yAxisLabel_setsLabel() {
    ChartOptions opts = ChartOptions.builder().yAxisLabel("Revenue").build();
    assertEquals("Revenue", opts.getYAxisLabel());
  }

  @Test
  void stacked_canBeEnabled() {
    ChartOptions opts = ChartOptions.builder().stacked(true).build();
    assertTrue(opts.isStacked());
  }

  @Test
  void legendPosition_allValues_haveCorrectJsString() {
    assertEquals("top", ChartOptions.LegendPosition.TOP.getValue());
    assertEquals("bottom", ChartOptions.LegendPosition.BOTTOM.getValue());
    assertEquals("left", ChartOptions.LegendPosition.LEFT.getValue());
    assertEquals("right", ChartOptions.LegendPosition.RIGHT.getValue());
  }
}
