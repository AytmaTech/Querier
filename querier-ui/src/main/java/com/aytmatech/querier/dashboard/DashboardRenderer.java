package com.aytmatech.querier.dashboard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Renders a {@link Dashboard} into a self-contained HTML page or an embeddable HTML fragment,
 * powered by Chart.js.
 *
 * <p>The renderer:
 *
 * <ol>
 *   <li>Executes every widget's {@link com.aytmatech.querier.Select} query via the supplied {@link
 *       QueryRunner}.
 *   <li>Maps each result-set through {@link ChartDataMapper} to produce Chart.js data.
 *   <li>Generates a responsive CSS grid of {@code <canvas>} elements.
 *   <li>Inlines a {@code <script>} block containing one {@code new Chart(…)} call per widget.
 * </ol>
 *
 * <h2>Full-page usage (Spring MVC)</h2>
 *
 * <pre>
 *   {@literal @}GetMapping(value = "/dashboard", produces = MediaType.TEXT_HTML_VALUE)
 *   {@literal @}ResponseBody
 *   public String dashboard() {
 *       QueryRunner runner = select -> {
 *           var sp = select.toSqlAndParams();
 *           return jdbc.queryForList(sp.sql(), sp.params());
 *       };
 *       return DashboardRenderer.builder()
 *           .dashboard(myDashboard)
 *           .queryRunner(runner)
 *           .build()
 *           .renderHtml();
 *   }
 * </pre>
 *
 * <h2>Embeddable fragment (Thymeleaf / JSP)</h2>
 *
 * <pre>
 *   String fragment = DashboardRenderer.builder()
 *       .dashboard(myDashboard)
 *       .queryRunner(runner)
 *       .build()
 *       .renderFragment();
 *   // Inject `fragment` into your template as unescaped HTML
 * </pre>
 *
 * <h2>Custom Chart.js URL (offline / self-hosted)</h2>
 *
 * <pre>
 *   DashboardRenderer.builder()
 *       .dashboard(myDashboard)
 *       .queryRunner(runner)
 *       .chartJsUrl("/static/js/chart.umd.min.js")   // local bundle
 *       .build()
 *       .renderHtml();
 * </pre>
 */
public class DashboardRenderer {

  private static final String DEFAULT_CHART_JS_URL = "https://cdn.jsdelivr.net/npm/chart.js";

  private final Dashboard dashboard;
  private final QueryRunner queryRunner;
  private final String chartJsUrl;
  private final boolean omitChartJsScriptElement;

  private DashboardRenderer(Builder builder) {
    this.dashboard = builder.dashboard;
    this.queryRunner = builder.queryRunner;
    this.chartJsUrl =
        builder.chartJsUrl != null && !builder.chartJsUrl.isBlank()
            ? builder.chartJsUrl
            : DEFAULT_CHART_JS_URL;
    this.omitChartJsScriptElement = builder.omitChartJsScriptElement;
  }

  /**
   * Creates a new Builder for DashboardRenderer.
   *
   * @return a new Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Renders a complete, self-contained HTML page.
   *
   * <p>The output includes {@code <!DOCTYPE html>}, a {@code <head>} with the Chart.js script tag
   * and embedded CSS, and a {@code <body>} with the widget grid and inline JavaScript.
   *
   * @return a complete HTML document as a String
   */
  public String renderHtml() {
    List<RenderedWidget> widgets = executeWidgets();
    StringBuilder html = new StringBuilder();

    html.append("<!DOCTYPE html>\n")
        .append("<html lang=\"en\">\n")
        .append("<head>\n")
        .append("  <meta charset=\"UTF-8\">\n")
        .append("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n")
        .append("  <title>")
        .append(escapeHtml(dashboard.getTitle()))
        .append("</title>\n");
    if (!omitChartJsScriptElement)
      html.append("  <script src=\"").append(escapeHtml(chartJsUrl)).append("\"></script>\n");
    html.append("  <style>\n")
        .append(buildCss())
        .append("  </style>\n")
        .append("</head>\n")
        .append("<body>\n")
        .append("  <h1 class=\"dashboard-title\">")
        .append(escapeHtml(dashboard.getTitle()))
        .append("</h1>\n")
        .append(buildGrid(widgets))
        .append(buildScriptBlock(widgets))
        .append("</body>\n")
        .append("</html>");

    return html.toString();
  }

  /**
   * Renders an embeddable HTML fragment (no {@code <html>/<head>/<body>} wrappers).
   *
   * <p>The fragment includes the Chart.js {@code <script src>} tag, the widget grid {@code <div>},
   * and the inline {@code <script>} block. Drop it anywhere inside an existing HTML page or
   * template.
   *
   * @return an HTML fragment as a String
   */
  public String renderFragment() {
    List<RenderedWidget> widgets = executeWidgets();
    StringBuilder html = new StringBuilder();

    html.append("<!-- Querier Dashboard: ")
        .append(escapeHtml(dashboard.getTitle()))
        .append(" -->\n");
    if (!omitChartJsScriptElement)
      html.append("<script src=\"").append(escapeHtml(chartJsUrl)).append("\"></script>\n");
    html.append("<h1 class=\"dashboard-title\">")
        .append(escapeHtml(dashboard.getTitle()))
        .append("</h1>\n")
        .append(buildGrid(widgets))
        .append(buildScriptBlock(widgets));

    return html.toString();
  }

  /** Executes each widget's query and maps the rows to {@link ChartData}. */
  private List<RenderedWidget> executeWidgets() {
    List<DashboardWidget> widgetDefs = dashboard.getWidgets();
    List<RenderedWidget> result = new ArrayList<>(widgetDefs.size());
    for (int i = 0; i < widgetDefs.size(); i++) {
      DashboardWidget widget = widgetDefs.get(i);
      List<Map<String, Object>> rows = queryRunner.run(widget.getQuery());
      ChartData chartData =
          ChartDataMapper.map(rows, widget.getLabelColumn(), widget.getDatasets());
      result.add(new RenderedWidget("querier-chart-" + i, widget, chartData));
    }
    return result;
  }

  /** Builds the responsive CSS grid of canvas elements. */
  private String buildGrid(List<RenderedWidget> widgets) {
    String gridClass =
        switch (dashboard.getDashboardLayout()) {
          case SINGLE_COL -> "querier-grid querier-grid--1";
          case GRID_2_COLS -> "querier-grid querier-grid--2";
          case GRID_3_COLS -> "querier-grid querier-grid--3";
          case MASONRY -> "querier-grid querier-grid--masonry";
        };

    StringBuilder sb = new StringBuilder();
    sb.append("  <div class=\"").append(gridClass).append("\">\n");
    for (RenderedWidget rw : widgets) {
      sb.append("    <div class=\"querier-widget\">\n")
          .append("      <h3 class=\"querier-widget__title\">")
          .append(escapeHtml(rw.widget.getTitle()))
          .append("</h3>\n")
          .append("      <div class=\"querier-widget__canvas-wrapper\">\n")
          .append("        <canvas id=\"")
          .append(rw.chartId)
          .append("\"></canvas>\n")
          .append("      </div>\n")
          .append("    </div>\n");
    }
    sb.append("  </div>\n");
    return sb.toString();
  }

  /** Builds the <script> block with all new Chart(…) calls. */
  private String buildScriptBlock(List<RenderedWidget> widgets) {
    StringBuilder sb = new StringBuilder();
    sb.append("  <script>\n");
    sb.append("    (function() {\n");
    for (RenderedWidget rw : widgets) {
      sb.append(buildChartInit(rw));
    }
    sb.append("    })();\n");
    sb.append("  </script>\n");
    return sb.toString();
  }

  /** Builds one {@code new Chart(…)} call for the given widget. */
  private String buildChartInit(RenderedWidget rw) {
    ChartData data = rw.chartData;
    DashboardWidget widget = rw.widget;
    ChartOptions opts = widget.getChartOptions();

    StringBuilder sb = new StringBuilder();
    sb.append("      new Chart(document.getElementById('")
        .append(rw.chartId)
        .append("'), {\n")
        .append("        type: '")
        .append(chartTypeToJs(widget.getChartType()))
        .append("',\n")
        .append("        data: {\n")
        .append("          labels: ")
        .append(toJsStringArray(data.getLabels()))
        .append(",\n")
        .append("          datasets: [\n");

    List<ChartData.DatasetData> datasets = data.getDatasets();
    for (int i = 0; i < datasets.size(); i++) {
      ChartData.DatasetData ds = datasets.get(i);
      sb.append("            {\n")
          .append("              label: ")
          .append(toJsString(ds.label()))
          .append(",\n")
          .append("              data: ")
          .append(toJsDataArray(ds.data()))
          .append(",\n");

      if (!ds.backgroundColor().isEmpty()) {
        if (ds.backgroundColor().size() == 1) {
          sb.append("              backgroundColor: ")
              .append(toJsString(ds.backgroundColor().get(0)))
              .append(",\n");
        } else {
          sb.append("              backgroundColor: ")
              .append(toJsStringArray(ds.backgroundColor()))
              .append(",\n");
        }
      }
      if (!ds.borderColor().isEmpty()) {
        if (ds.borderColor().size() == 1) {
          sb.append("              borderColor: ")
              .append(toJsString(ds.borderColor().get(0)))
              .append(",\n");
        } else {
          sb.append("              borderColor: ")
              .append(toJsStringArray(ds.borderColor()))
              .append(",\n");
        }
      }
      sb.append("              fill: ")
          .append(ds.fill())
          .append("\n")
          .append("            }")
          .append(i < datasets.size() - 1 ? "," : "")
          .append("\n");
    }

    sb.append("          ]\n")
        .append("        },\n")
        .append("        options: ")
        .append(buildChartOptions(opts, widget.getChartType()))
        .append("\n")
        .append("      });\n");

    return sb.toString();
  }

  /**
   * Serialises a {@link ChartOptions} to a Chart.js {@code options} object literal. Omits {@code
   * scales} for chart types that don't support them (PIE, DOUGHNUT, etc.).
   */
  private String buildChartOptions(ChartOptions opts, ChartType chartType) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("          responsive: ").append(opts.isResponsive()).append(",\n");
    sb.append("          maintainAspectRatio: ").append(opts.isMaintainAspectRatio()).append(",\n");
    if (opts.getAspectRatio() != null) {
      sb.append("          aspectRatio: ").append(opts.getAspectRatio()).append(",\n");
    } else {
      switch (chartType) {
        case PIE, DOUGHNUT, RADAR, POLAR_AREA -> sb.append("          aspectRatio: 1,\n");
        default -> sb.append("          aspectRatio: 2,\n");
      }
    }
    sb.append("          resizeDelay: ").append(opts.getResizeDelay()).append(",\n");

    if (chartType == ChartType.HORIZONTAL_BAR) {
      sb.append("          indexAxis: 'y',\n");
    }

    sb.append("          layout: {\n")
        .append("          autoPadding: ")
        .append(opts.isAutoPadding())
        .append(",\n")
        .append("          padding: ")
        .append(opts.getPadding())
        .append(",\n")
        .append("          },\n");

    sb.append("          plugins: {\n")
        .append("            legend: { position: '")
        .append(opts.getLegendPosition().getValue())
        .append("' }");
    if (!opts.getSubtitle().isBlank())
      sb.append(",\n")
          .append("          subtitle: { display: true, text: ")
          .append(toJsString(opts.getSubtitle()))
          .append(" }\n");
    else sb.append("\n");
    sb.append("          }");

    if (hasScales(chartType)) {
      sb.append(",\n")
          .append("          scales: {\n")
          .append("            x: {\n")
          .append("              stacked: ")
          .append(opts.isStacked())
          .append(",\n");

      if (!opts.getXAxisLabel().isBlank()) {
        sb.append("              title: { display: true, text: ")
            .append(toJsString(opts.getXAxisLabel()))
            .append(" }\n");
      } else {
        sb.append("              title: { display: false }\n");
      }

      sb.append("            },\n")
          .append("            y: {\n")
          .append("              stacked: ")
          .append(opts.isStacked())
          .append(",\n");

      if (!opts.getYAxisLabel().isBlank()) {
        sb.append("              title: { display: true, text: ")
            .append(toJsString(opts.getYAxisLabel()))
            .append(" }\n");
      } else {
        sb.append("              title: { display: false }\n");
      }

      sb.append("            }\n").append("          }");
    }

    sb.append("\n        }");
    return sb.toString();
  }

  /** Returns {@code true} for cartesian chart types that use {@code scales}. */
  private boolean hasScales(ChartType chartType) {
    return chartType != ChartType.PIE
        && chartType != ChartType.DOUGHNUT
        && chartType != ChartType.RADAR
        && chartType != ChartType.POLAR_AREA;
  }

  /**
   * Maps a {@link ChartType} to its Chart.js v3+ {@code type} string. {@link
   * ChartType#HORIZONTAL_BAR} maps to {@code "bar"} — the horizontal orientation is controlled via
   * {@code indexAxis: 'y'} in options.
   */
  private String chartTypeToJs(ChartType chartType) {
    return switch (chartType) {
      case BAR, HORIZONTAL_BAR -> "bar";
      case LINE -> "line";
      case PIE -> "pie";
      case DOUGHNUT -> "doughnut";
      case RADAR -> "radar";
      case POLAR_AREA -> "polarArea";
      case SCATTER -> "scatter";
      case BUBBLE -> "bubble";
    };
  }

  private String buildCss() {
    return """
                    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
                    body {
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
                                     Helvetica, Arial, sans-serif;
                        background: #f5f6fa;
                        color: #333;
                        padding: 28px;
                    }
                    .dashboard-title {
                        font-size: 1.75rem;
                        font-weight: 700;
                        margin-bottom: 28px;
                        color: #1a1a2e;
                    }
                    .querier-grid {
                        display: grid;
                        gap: 24px;
                    }
                    .querier-grid--1       { grid-template-columns: 1fr; }
                    .querier-grid--2       { grid-template-columns: repeat(2, 1fr); }
                    .querier-grid--3       { grid-template-columns: repeat(3, 1fr); }
                    .querier-grid--masonry { grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); }
                    .querier-widget {
                        background: #ffffff;
                        border-radius: 14px;
                        padding: 22px 24px;
                        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.07);
                    }
                    .querier-widget__title {
                        font-size: 0.95rem;
                        font-weight: 600;
                        color: #555;
                        margin-bottom: 18px;
                        text-transform: uppercase;
                        letter-spacing: 0.04em;
                    }
                    .querier-widget__canvas-wrapper {
                        position: relative;
                        width: 100%;
                    }
                    @media (max-width: 900px) {
                        .querier-grid--2, .querier-grid--3 { grid-template-columns: 1fr; }
                    }
                    @media (max-width: 600px) {
                        .querier-grid--masonry { grid-template-columns: 1fr; }
                    }
                """;
  }

  /** Escapes a value for safe use inside an HTML attribute or text node. */
  private String escapeHtml(String s) {
    if (s == null) return "";
    return s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#x27;");
  }

  /** Wraps a value in JS single quotes, escaping backslashes and single quotes. */
  private String toJsString(String s) {
    if (s == null) return "null";
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /** Serialises a {@code List<String>} to a JS array of single-quoted strings. */
  private String toJsStringArray(List<String> list) {
    if (list == null || list.isEmpty()) return "[]";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < list.size(); i++) {
      sb.append(toJsString(list.get(i)));
      if (i < list.size() - 1) sb.append(", ");
    }
    return sb.append("]").toString();
  }

  /**
   * Serialises a data array to a JS array. Numbers are emitted without quotes; everything else is
   * quoted.
   */
  private String toJsDataArray(List<Object> list) {
    if (list == null || list.isEmpty()) return "[]";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < list.size(); i++) {
      Object val = list.get(i);
      if (val == null) {
        sb.append("null");
      } else if (val instanceof Number) {
        sb.append(val);
      } else {
        sb.append(toJsString(val.toString()));
      }
      if (i < list.size() - 1) sb.append(", ");
    }
    return sb.append("]").toString();
  }

  /** Internal holder combining a widget with its executed chart data and canvas ID. */
  private record RenderedWidget(String chartId, DashboardWidget widget, ChartData chartData) {}

  /** Builder for {@link DashboardRenderer}. */
  public static class Builder {

    private Dashboard dashboard;
    private QueryRunner queryRunner;
    private String chartJsUrl;
    private boolean omitChartJsScriptElement = false;

    /**
     * Creates a new Builder instance. Required properties (dashboard, queryRunner) must be set
     * before calling {@link #build()}. Optional properties (chartJsUrl) have sensible defaults.
     */
    public Builder() {}

    /**
     * Sets the dashboard to render.
     *
     * @param dashboard the dashboard definition
     * @return this builder
     */
    public Builder dashboard(Dashboard dashboard) {
      this.dashboard = dashboard;
      return this;
    }

    /**
     * Sets the query runner used to execute each widget's {@link com.aytmatech.querier.Select}.
     *
     * @param queryRunner the query runner
     * @return this builder
     */
    public Builder queryRunner(QueryRunner queryRunner) {
      this.queryRunner = queryRunner;
      return this;
    }

    /**
     * Overrides the Chart.js source URL.
     *
     * <p>Defaults to {@value DashboardRenderer#DEFAULT_CHART_JS_URL} when not set. Pass a local
     * path (e.g., {@code "/static/js/chart.umd.min.js"}) for offline or self-hosted deployments.
     *
     * @param chartJsUrl the URL for the Chart.js script tag
     * @return this builder
     */
    public Builder chartJsUrl(String chartJsUrl) {
      this.chartJsUrl = chartJsUrl;
      return this;
    }

    /**
     * Sets whether to omit the Chart.js script tag in the generated HTML.
     *
     * @param omitChartJsScriptElement if true, the generated HTML will not include the {@code
     *     <script src="...chart.js">} element. This is useful when embedding the dashboard in a
     *     page that already includes Chart.js.
     * @return this builder
     */
    public Builder omitChartJsScriptElement(boolean omitChartJsScriptElement) {
      this.omitChartJsScriptElement = omitChartJsScriptElement;
      return this;
    }

    /**
     * Builds the {@link DashboardRenderer}.
     *
     * @return a new DashboardRenderer instance
     * @throws IllegalStateException if dashboard or queryRunner is missing
     */
    public DashboardRenderer build() {
      if (dashboard == null)
        throw new IllegalStateException("DashboardRenderer requires a Dashboard");
      if (queryRunner == null)
        throw new IllegalStateException("DashboardRenderer requires a QueryRunner");
      return new DashboardRenderer(this);
    }
  }
}
