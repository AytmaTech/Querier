package com.aytmatech.querier.dashboard;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a complete dashboard composed of one or more {@link DashboardWidget}s.
 *
 * <p>Usage:
 *
 * <pre>
 *   Dashboard dashboard = Dashboard.builder()
 *       .title("Sales Overview")
 *       .layout(DashboardLayout.GRID_2_COLS)
 *       .addWidget(revenueWidget)
 *       .addWidget(ordersWidget)
 *       .build();
 * </pre>
 */
public class Dashboard {

  private final String title;
  private final List<DashboardWidget> widgets;
  private final DashboardLayout layout;

  private Dashboard(Builder builder) {
    this.title = builder.title;
    this.widgets = List.copyOf(builder.widgets);
    this.layout = builder.layout != null ? builder.layout : DashboardLayout.GRID_2_COLS;
  }

  /**
   * Returns the dashboard title shown at the top of the page.
   *
   * @return the dashboard title shown at the top of the page
   */
  public String getTitle() {
    return title;
  }

  /**
   * Returns the list of widgets to display on the dashboard, in the order they should be rendered.
   *
   * @return the ordered list of widgets to display
   */
  public List<DashboardWidget> getWidgets() {
    return widgets;
  }

  /**
   * Returns the grid layout used when rendering the dashboard (defaults to {@link
   * DashboardLayout#GRID_2_COLS} if not set).
   *
   * @return the grid layout used when rendering (defaults to {@link DashboardLayout#GRID_2_COLS})
   */
  public DashboardLayout getDashboardLayout() {
    return layout;
  }

  /**
   * Creates a new builder for constructing a Dashboard with sensible defaults (title "Dashboard",
   *
   * @return a new Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link Dashboard}. */
  public static class Builder {

    private String title = "Dashboard";
    private final List<DashboardWidget> widgets = new ArrayList<>();
    private DashboardLayout layout;

    /** Creates a new Builder with default values. */
    public Builder() {}

    /**
     * Sets the dashboard title.
     *
     * @param title the page/dashboard title
     * @return this builder
     */
    public Builder title(String title) {
      this.title = title != null ? title : "Dashboard";
      return this;
    }

    /**
     * Adds a widget to the dashboard. Widgets are rendered in the order they are added.
     *
     * @param widget the widget to add
     * @return this builder
     */
    public Builder addWidget(DashboardWidget widget) {
      if (widget != null) this.widgets.add(widget);
      return this;
    }

    /**
     * Sets the grid layout for the dashboard (defaults to {@link DashboardLayout#GRID_2_COLS}).
     *
     * @param layout the desired layout
     * @return this builder
     */
    public Builder layout(DashboardLayout layout) {
      this.layout = layout;
      return this;
    }

    /**
     * Builds the {@link Dashboard}.
     *
     * @return a new Dashboard instance
     * @throws IllegalStateException if no widgets have been added
     */
    public Dashboard build() {
      if (widgets.isEmpty())
        throw new IllegalStateException("A Dashboard must contain at least one widget");
      return new Dashboard(this);
    }
  }
}
