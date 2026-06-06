package com.aytmatech.querier.dashboard;

/**
 * Defines the layout options for a dashboard, determining how charts are arranged on the screen.
 */
public enum DashboardLayout {

  /** Single column layout where all charts are stacked vertically in one column. */
  SINGLE_COL,

  /** Two column layout where charts are arranged in two equal-width columns. */
  GRID_2_COLS,

  /** Three column layout where charts are arranged in three equal-width columns. */
  GRID_3_COLS,

  /**
   * Masonry layout where charts are arranged in a Pinterest-style layout with variable heights and
   * optimal use of space. Note: This layout may require additional CSS and JavaScript to implement
   * the masonry effect.
   */
  MASONRY
}
