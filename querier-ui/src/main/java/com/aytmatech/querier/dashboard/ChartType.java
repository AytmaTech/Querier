package com.aytmatech.querier.dashboard;

/** Enum representing the types of charts supported in the dashboard. */
public enum ChartType {

  /** Bar chart type, used for comparing values across categories. */
  BAR,

  /** Horizontal bar chart type, similar to bar chart but with horizontal bars. */
  HORIZONTAL_BAR,

  /** Line chart type, used for showing trends over time or continuous data. */
  LINE,

  /** Pie chart type, used for showing proportions of a whole. */
  PIE,

  /** Doughnut chart type, similar to pie chart but with a hole in the center. */
  DOUGHNUT,

  /** Radar chart type, used for showing multivariate data in a circular format. */
  RADAR,

  /** Polar area chart type, similar to radar but with filled areas representing values. */
  POLAR_AREA,

  /**
   * Scatter chart type, used for showing relationships between two variables with individual data
   * points.
   */
  SCATTER,

  /**
   * Bubble chart type, similar to scatter but with an additional dimension represented by the size
   * of the bubbles.
   */
  BUBBLE
}
