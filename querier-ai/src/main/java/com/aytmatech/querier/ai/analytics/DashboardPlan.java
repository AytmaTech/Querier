package com.aytmatech.querier.ai.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * AI-facing intermediate representation of an entire dashboard.
 *
 * <p>Contains the dashboard title, layout hint, and one or more {@link WidgetPlan}s. Validated and
 * compiled by {@link DashboardPlanValidator} and {@link DashboardPlanCompiler} respectively.
 */
public final class DashboardPlan {

  private final String title;
  private final String layout;
  private final List<WidgetPlan> widgets;

  private DashboardPlan(Builder b) {
    this.title = Objects.requireNonNull(b.title, "title");
    this.layout = b.layout != null ? b.layout : "GRID_2_COLS";
    this.widgets = Collections.unmodifiableList(new ArrayList<>(b.widgets));
  }

  /**
   * Creates a new {@link Builder} for constructing a {@link DashboardPlan}.
   *
   * @return a new {@link Builder} instance for constructing a {@link DashboardPlan}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Dashboard title, used in the UI and for AI prompt generation.
   *
   * @return Dashboard title.
   */
  public String title() {
    return title;
  }

  /**
   * Dashboard layout hint, used in the UI and for AI prompt generation. Defaults to {@code
   * GRID_2_COLS}.
   *
   * @return Layout hint: one of {@code SINGLE_COL}, {@code GRID_2_COLS}, {@code GRID_3_COLS},
   *     {@code MASONRY}. Defaults to {@code GRID_2_COLS}.
   * @see com.aytmatech.querier.dashboard.DashboardLayout
   */
  public String layout() {
    return layout;
  }

  /**
   * Dashboard widgets, each represented by a {@link WidgetPlan}.
   *
   * @return All widget plans in this dashboard.
   */
  public List<WidgetPlan> widgets() {
    return widgets;
  }

  /**
   * Builder for constructing a {@link DashboardPlan}. Use {@link DashboardPlan#builder()} to create
   * a new instance.
   */
  public static final class Builder {
    private String title;
    private String layout;
    private final List<WidgetPlan> widgets = new ArrayList<>();

    private Builder() {}

    /**
     * Sets Dashboard title. Required.
     *
     * @param title Dashboard title. Must not be null.
     * @return this builder
     */
    public Builder title(String title) {
      this.title = title;
      return this;
    }

    /**
     * Sets Dashboard layout hint. Optional; defaults to {@code GRID_2_COLS}.
     *
     * @param layout Layout hint: one of {@code SINGLE_COL}, {@code GRID_2_COLS}, {@code
     *     GRID_3_COLS}, {@code MASONRY}. Defaults to {@code GRID_2_COLS}.
     * @return this builder
     */
    public Builder layout(String layout) {
      this.layout = layout;
      return this;
    }

    /**
     * Adds a widget to this dashboard.
     *
     * @param widget Widget plan to add to this dashboard. Must not be null.
     * @return this builder
     */
    public Builder widget(WidgetPlan widget) {
      this.widgets.add(widget);
      return this;
    }

    /**
     * Builds the {@link DashboardPlan} instance. Validates that required fields are set.
     *
     * @return an {@link DashboardPlan} instance built from the current state of this builder
     * @throws IllegalStateException if the builder is in an invalid state (e.g., no widgets added)
     */
    public DashboardPlan build() {
      if (widgets.isEmpty()) {
        throw new IllegalStateException("A DashboardPlan must have at least one widget");
      }
      return new DashboardPlan(this);
    }
  }

  /**
   * Converts this dashboard plan into a string representation suitable for use as a prompt to an AI
   * model.
   *
   * @return a string representation of this dashboard plan, suitable for use as a prompt to an AI
   *     model
   */
  public String toPromptText() {
    StringBuilder sb = new StringBuilder();
    sb.append("Dashboard Title: ").append(title).append("\n");
    sb.append("Layout: ").append(layout).append("\n");
    sb.append("Widgets:\n");
    for (WidgetPlan widget : widgets) {
      sb.append(widget.toPromptText()).append("\n");
    }
    return sb.toString();
  }
}
