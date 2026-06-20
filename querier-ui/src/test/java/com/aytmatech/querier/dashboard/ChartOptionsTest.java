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

  @Test
  void aspectRatio_defaultIsNull() {
    ChartOptions opts = ChartOptions.builder().build();
    assertNull(opts.getAspectRatio());
  }

  @Test
  void aspectRatio_setsValue() {
    ChartOptions opts = ChartOptions.builder().aspectRatio(1.5d).build();
    assertEquals(1.5d, opts.getAspectRatio());
  }

  @Test
  void aspectRatio_null_remainsNull() {
    ChartOptions opts = ChartOptions.builder().aspectRatio(null).build();
    assertNull(opts.getAspectRatio());
  }

  @Test
  void maintainAspectRatio_defaultIsTrue() {
    ChartOptions opts = ChartOptions.builder().build();
    assertTrue(opts.isMaintainAspectRatio());
  }

  @Test
  void maintainAspectRatio_canBeDisabled() {
    ChartOptions opts = ChartOptions.builder().maintainAspectRatio(false).build();
    assertFalse(opts.isMaintainAspectRatio());
  }

  @Test
  void resizeDelay_defaultIsZero() {
    ChartOptions opts = ChartOptions.builder().build();
    assertEquals(0, opts.getResizeDelay());
  }

  @Test
  void resizeDelay_setsValue() {
    ChartOptions opts = ChartOptions.builder().resizeDelay(200).build();
    assertEquals(200, opts.getResizeDelay());
  }

  @Test
  void autoPadding_defaultIsTrue() {
    ChartOptions opts = ChartOptions.builder().build();
    assertTrue(opts.isAutoPadding());
  }

  @Test
  void autoPadding_canBeDisabled() {
    ChartOptions opts = ChartOptions.builder().autoPadding(false).build();
    assertFalse(opts.isAutoPadding());
  }

  @Test
  void padding_defaultIsZero() {
    ChartOptions opts = ChartOptions.builder().build();
    assertEquals(0, opts.getPadding());
  }

  @Test
  void padding_setsValue() {
    ChartOptions opts = ChartOptions.builder().padding(16).build();
    assertEquals(16, opts.getPadding());
  }

  @Test
  void subtitle_defaultIsEmpty() {
    ChartOptions opts = ChartOptions.builder().build();
    assertEquals("", opts.getSubtitle());
  }

  @Test
  void subtitle_setsText() {
    ChartOptions opts = ChartOptions.builder().subtitle("Q1 2026").build();
    assertEquals("Q1 2026", opts.getSubtitle());
  }

  @Test
  void subtitle_null_storesNull() {
    ChartOptions opts = ChartOptions.builder().subtitle(null).build();
    assertNull(opts.getSubtitle());
  }
}
