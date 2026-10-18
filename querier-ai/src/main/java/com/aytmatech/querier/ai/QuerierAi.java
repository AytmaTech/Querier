package com.aytmatech.querier.ai;

import com.aytmatech.querier.Select;
import com.aytmatech.querier.ai.analytics.DashboardPlan;
import com.aytmatech.querier.ai.analytics.DashboardPlanCompiler;
import com.aytmatech.querier.ai.analytics.DashboardPlanValidator;
import com.aytmatech.querier.ai.analytics.DashboardResult;
import com.aytmatech.querier.ai.analytics.WidgetPlan;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.query.QueryRequest;
import com.aytmatech.querier.ai.query.QueryResult;
import com.aytmatech.querier.ai.retry.RetryPolicy;
import com.aytmatech.querier.ai.sql.SqlSchemaValidator;
import com.aytmatech.querier.ai.sql.SqlToSelectParser;
import com.aytmatech.querier.dashboard.ChartType;
import com.aytmatech.querier.dashboard.DashboardLayout;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Primary entry point for all Querier AI features.
 *
 * <h2>Architecture: SQL-First</h2>
 *
 * <p>This implementation uses a SQL-first approach it asks the AI to produce standard SQL. The AI
 * has extensive training on SQL and produces far more reliable results for complex queries (CTEs,
 * window functions, subqueries, arithmetic expressions).
 *
 * <p>The pipeline for natural-language queries is:
 *
 * <ol>
 *   <li>Ask AI to produce SQL (leverages AI's SQL training data)
 *   <li>Validate SQL against {@link AiSchemaContext} (security boundary)
 *   <li>Parse SQL → {@link Select} (deterministic, no AI)
 * </ol>
 *
 * <h2>Configuration</h2>
 *
 * <pre>{@code
 * AiClient client = AiClient.builder()
 *     .apiKey(System.getenv("QUERIER_AI_API_KEY"))
 *     .baseUrl(System.getenv("QUERIER_AI_BASE_URL"))
 *     .model(System.getenv("QUERIER_AI_MODEL"))
 *     .build();
 *
 * QuerierAi ai = QuerierAi.builder()
 *     .client(client)
 *     .retryPolicy(RetryPolicy.ofMaxAttempts(2))
 *     .build();
 * }</pre>
 */
public final class QuerierAi {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * System prompt for SQL generation.
   *
   * <p>Key design decisions:
   *
   * <ul>
   *   <li>Ask for SQL only — not a proprietary IR. The AI is trained on SQL.
   *   <li>Return a simple JSON wrapper so we can detect ambiguity programmatically.
   *   <li>Explicit rules for what the AI must NOT do (invent columns, use non-schema tables).
   *   <li>Concrete examples of the response format to reduce hallucination.
   * </ul>
   */
  private static final String QUERY_SQL_SYSTEM_PROMPT =
      """
          You are a SQL generation assistant for a Java application using a database.

          YOUR ONLY JOB: Write a single valid SQL SELECT statement that answers the user's question.

          STRICT RULES — violating any rule will cause the query to fail:
          1. Use ONLY the tables and columns listed in the "Available schema" section below.
          2. NEVER invent table names or column names not listed in the schema.
          3. Use the exact table names and column names from the schema (case-sensitive).
          4. You MAY use table aliases (e.g. "orders o") but the actual table names must match the schema.
          5. For date arithmetic, use standard SQL: current_date - interval '30 days'.
          6. For window functions, CTEs, subqueries — write normal SQL.
          7. ALWAYS return a JSON object in this exact format and nothing else:
             {"sql": "<your complete SQL here>", "ambiguous": false, "ambiguityNote": null}
          8. If the request cannot be answered with the available schema, set ambiguous=true.
          9. No markdown, no code blocks, no explanation — ONLY the JSON object.

          EXAMPLE RESPONSE:
          {"sql": "SELECT c.name, SUM(o.total) AS revenue FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY revenue DESC LIMIT 10", "ambiguous": false, "ambiguityNote": null}
          """;

  private static final String QUERY_SQL_RESPONSE_JSON_SCHEMA =
      """
          {
            "type": "object",
            "properties": {
              "sql": {
                "type": "string",
                "description": "The complete SQL query generated to satisfy the user's request."
              },
              "ambiguous": {
                "type": "boolean",
                "description": "True if the user's request contains ambiguity that could affect the generated SQL; otherwise false."
              },
              "ambiguityNote": {
                "type": ["string", "null"],
                "description": "Explain the ambiguity when ambiguous is true. Return null when ambiguous is false."
              }
            },
            "required": [
              "sql",
              "ambiguous",
              "ambiguityNote"
            ],
            "additionalProperties": false
          }
          """;

  /** System prompt for dashboard SQL generation. Each widget gets its own SQL query. */
  private static final String DASHBOARD_SQL_SYSTEM_PROMPT =
      """
          You are a dashboard SQL generation assistant for a Java application.

          YOUR JOB: Given a dashboard description, produce a JSON dashboard plan where each widget
          has a SQL SELECT statement for its data.

          STRICT RULES:
          1. Use ONLY the tables and columns listed in the "Available schema" section below.
          2. NEVER invent table names or column names.
          3. Use exact table/column names from the schema.
          4. Each widget's sql must be a complete, valid SELECT statement.
          5. Choose chartType from: chartTypesArray.
          6. Choose layout from: dashboardLayoutArray.
          7. labelColumn is the column alias used as the X-axis / category label.
          8. Return ONLY this JSON format, nothing else:
          {
            "title": "Dashboard Title",
            "layout": "GRID_2_COLS",
            "widgets": [
              {
                "title": "Widget Title",
                "chartType": "BAR",
                "labelColumn": "column_alias",
                "sql": "SELECT ...",
                "datasets": [{"column": "alias", "label": "Legend Label"}]
              }
            ]
          }
          9. No markdown, no code blocks — ONLY the JSON object.
          """
          .replace(
              "chartTypesArray",
              Arrays.stream(ChartType.values())
                  .map(ChartType::name)
                  .collect(Collectors.joining("\", \"", "[\"", "\"]")))
          .replace(
              "dashboardLayoutArray",
              Arrays.stream(DashboardLayout.values())
                  .map(DashboardLayout::name)
                  .collect(Collectors.joining("\", \"", "[\"", "\"]")));

  private static final String DASHBOARD_SQL_RESPONSE_JSON_SCHEMA =
      """
                  {
                    "type": "object",
                    "properties": {
                      "title": {
                        "type": "string",
                        "description": "The title of the dashboard."
                      },
                      "layout": {
                        "type": "string",
                        "enum": dashboardLayoutArray,
                        "description": "The layout used to arrange dashboard widgets."
                      },
                      "widgets": {
                        "type": "array",
                        "description": "The charts displayed on the dashboard.",
                        "items": {
                          "type": "object",
                          "properties": {
                            "title": {
                              "type": "string",
                              "description": "The title of the chart widget."
                            },
                            "chartType": {
                              "type": "string",
                              "enum": chartTypesArray,
                              "description": "The chart visualization type (chartjs compatible)."
                            },
                            "labelColumn": {
                              "type": "string",
                              "description": "The SQL result column alias used for category labels or the primary label dimension of the chart."
                            },
                            "sql": {
                              "type": "string",
                              "description": "The complete SQL SELECT query used to retrieve the data for this chart."
                            },
                            "datasets": {
                              "type": "array",
                              "description": "The data series to display, referencing strictly column aliases returned by the SQL query.",
                              "items": {
                                "type": "object",
                                "properties": {
                                  "column": {
                                    "type": "string",
                                    "description": "The SQL result column alias containing the dataset values."
                                  },
                                  "label": {
                                    "type": "string",
                                    "description": "The human-readable label used to identify this dataset in the chart legend."
                                  }
                                },
                                "required": [
                                  "column",
                                  "label"
                                ],
                                "additionalProperties": false
                              }
                            }
                          },
                          "required": [
                            "title",
                            "chartType",
                            "labelColumn",
                            "sql",
                            "datasets"
                          ],
                          "additionalProperties": false
                        }
                      }
                    },
                    "required": [
                      "title",
                      "layout",
                      "widgets"
                    ],
                    "additionalProperties": false
                  }
                  """
          .replace(
              "dashboardLayoutArray",
              Arrays.stream(DashboardLayout.values())
                  .map(DashboardLayout::name)
                  .collect(Collectors.joining("\", \"", "[\"", "\"]")))
          .replace(
              "chartTypesArray",
              Arrays.stream(ChartType.values())
                  .map(ChartType::name)
                  .collect(Collectors.joining("\", \"", "[\"", "\"]")));

  private final AiClient client;
  private final RetryPolicy retryPolicy;

  private QuerierAi(Builder b) {
    this.client = Objects.requireNonNull(b.client, "client must not be null");
    this.retryPolicy = b.retryPolicy != null ? b.retryPolicy : RetryPolicy.disabled();
  }

  /**
   * Creates a new {@link QuerierAi} instance with the given {@link AiClient} and default retry
   * policy.
   *
   * @return a new {@link Builder} for constructing a {@link QuerierAi} instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Translates a natural-language prompt into a validated Querier {@link Select}.
   *
   * <p>Uses SQL-first approach: asks the AI for SQL, validates it against the schema, then converts
   * it to a {@link Select} object.
   *
   * @param request the query request containing the prompt and allowed schema
   * @return result with the compiled query when valid; validation issues when invalid
   */
  public QueryResult query(QueryRequest request) {
    QueryResult.Builder result = QueryResult.builder().prompt(request.prompt());

    AiRequest firstRequest = buildSqlRequest(request);
    AiResponse response = client.send(firstRequest);

    AiSqlResponse sqlResponse = parseSqlResponse(response, result);
    if (sqlResponse == null) return result.build();

    if (sqlResponse.isAmbiguous()) {
      sqlResponse.ambiguityNote().ifPresent(result::interpretationNote);
      result.validationIssue(
          "The request was ambiguous and could not be answered. "
              + "See interpretationNote() for details.");
      return result.build();
    }

    String sql = sqlResponse.sql().orElse(null);
    result.rawSql(sql);
    if (sql == null || sql.isBlank()) {
      result.validationIssue("AI returned empty SQL");
      return result.build();
    }

    List<String> schemaIssues = SqlSchemaValidator.validate(sql, request.schemaContext());

    if (!schemaIssues.isEmpty() && retryPolicy.isEnabled()) {
      for (int attempt = 0;
          attempt < retryPolicy.maxAttempts() && !schemaIssues.isEmpty();
          attempt++) {
        AiRequest correctionRequest = buildSqlCorrectionRequest(request, sql, schemaIssues);
        AiResponse correctionResponse = client.send(correctionRequest);

        QueryResult.Builder retryResult = QueryResult.builder().prompt(request.prompt());
        AiSqlResponse correctedSqlResponse = parseSqlResponse(correctionResponse, retryResult);
        if (correctedSqlResponse == null || correctedSqlResponse.isAmbiguous()) break;

        String correctedSql = correctedSqlResponse.sql().orElse(null);
        if (correctedSql == null || correctedSql.isBlank()) break;

        sql = correctedSql;
        schemaIssues = SqlSchemaValidator.validate(sql, request.schemaContext());
      }
    }

    if (!schemaIssues.isEmpty()) {
      schemaIssues.forEach(result::validationIssue);
      return result.build();
    }

    Select select = SqlToSelectParser.parse(sql, request.schemaContext());
    result.query(select);
    return result.build();
  }

  /**
   * Translates a natural-language prompt into a validated dashboard plan with SQL for each widget.
   *
   * @param request the query request containing the prompt and allowed schema
   * @return result with the compiled dashboard plan when valid; validation issues when invalid
   */
  public DashboardResult dashboard(QueryRequest request) {
    DashboardResult.Builder result = DashboardResult.builder().prompt(request.prompt());

    AiRequest firstRequest = buildDashboardSqlRequest(request);
    AiResponse response = client.send(firstRequest);

    DashboardPlan plan = parseDashboardPlan(response, result);
    if (plan == null) return result.build();
    result.plan(plan);

    List<String> issues = DashboardPlanValidator.validate(plan, request.schemaContext());

    if (!issues.isEmpty() && retryPolicy.isEnabled()) {
      for (int attempt = 0; attempt < retryPolicy.maxAttempts() && !issues.isEmpty(); attempt++) {
        DashboardResult.Builder retryResult = DashboardResult.builder().prompt(request.prompt());
        AiRequest correctionRequest = buildDashboardSqlCorrectionRequest(request, plan, issues);
        AiResponse correctionResponse = client.send(correctionRequest);

        DashboardPlan correctedPlan = parseDashboardPlan(correctionResponse, retryResult);
        if (correctedPlan == null) return retryResult.build();

        plan = correctedPlan;
        issues = DashboardPlanValidator.validate(plan, request.schemaContext());
      }
      result = DashboardResult.builder().prompt(request.prompt()).plan(plan);
    }

    if (!issues.isEmpty()) {
      DashboardResult.Builder finalResult = result;
      issues.forEach(finalResult::validationIssue);
      return finalResult.build();
    }

    result.dashboard(DashboardPlanCompiler.compile(plan, request.schemaContext()));
    return result.build();
  }

  private DashboardPlan parseDashboardPlan(AiResponse response, DashboardResult.Builder result) {
    String json = response.structuredContent();
    if (json == null || json.isBlank()) {
      result.validationIssue("AI response contained no structured content");
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      DashboardPlan.Builder planBuilder =
          DashboardPlan.builder()
              .title(root.path("title").asText("Dashboard"))
              .layout(root.path("layout").asText("GRID_2_COLS"));

      for (JsonNode wNode : root.path("widgets")) {
        String widgetTitle = wNode.path("title").asText("Chart");
        WidgetPlan.Builder wpBuilder =
            WidgetPlan.builder()
                .title(widgetTitle)
                .chartType(wNode.path("chartType").asText("BAR"))
                .labelColumn(wNode.path("labelColumn").asText(""));

        JsonNode sqlNode = wNode.path("sql");

        if (!sqlNode.isMissingNode() && !sqlNode.isNull() && !sqlNode.asText("").isBlank()) {
          String sql = stripMarkdownCodeFences(sqlNode.asText(""));
          wpBuilder.rawSql(sql);
        } else {
          result.validationIssue("Widget '" + widgetTitle + "' has no 'sql' field");
          return null;
        }

        for (JsonNode dsNode : wNode.path("datasets")) {
          wpBuilder.dataset(
              new WidgetPlan.DatasetSpec(
                  dsNode.path("column").asText(""), dsNode.path("label").asText("")));
        }
        planBuilder.widget(wpBuilder.build());
      }

      return planBuilder.build();
    } catch (Exception e) {
      result.validationIssue("Failed to parse AI dashboard plan response: " + e.getMessage());
      return null;
    }
  }

  /**
   * Builds a SQL generation request.
   *
   * @param queryRequest the query request containing the prompt and allowed schema
   * @return an AiRequest for the AI to generate SQL
   */
  private AiRequest buildSqlRequest(QueryRequest queryRequest) {
    String content = QUERY_SQL_SYSTEM_PROMPT + queryRequest.schemaContext().toPromptText();
    return AiRequest.builder()
        .systemMessage(content)
        .userMessage(queryRequest.prompt())
        .temperature(queryRequest.temperature() != null ? queryRequest.temperature() : 0.0)
        .model(
            (queryRequest.model() != null && !queryRequest.model().isBlank())
                ? queryRequest.model()
                : null)
        .structuredOutput(true)
        .maxTokens(queryRequest.maxTokens() != null ? queryRequest.maxTokens() : null)
        .jsonSchema(QUERY_SQL_RESPONSE_JSON_SCHEMA)
        .build();
  }

  /**
   * Builds a SQL correction request.
   *
   * @param queryRequest the original query request containing the prompt and allowed schema
   * @param previousSql the invalid SQL returned by the AI
   * @param issues list of validation issues found in the previous SQL
   * @return an AiRequest for the AI to correct the SQL
   */
  private AiRequest buildSqlCorrectionRequest(
      QueryRequest queryRequest, String previousSql, List<String> issues) {
    String correctionMessage =
        queryRequest.prompt()
            + "\n\n---\nCorrection needed. Your previous SQL:\n"
            + previousSql
            + "\n\nIssues to fix:\n"
            + formatIssues(issues)
            + "\n\nReturn corrected SQL in the same JSON format.";
    return AiRequest.builder()
        .systemMessage(QUERY_SQL_SYSTEM_PROMPT + queryRequest.schemaContext().toPromptText())
        .userMessage(correctionMessage)
        .temperature(queryRequest.temperature() != null ? queryRequest.temperature() : 0.0)
        .model(
            (queryRequest.model() != null && !queryRequest.model().isBlank())
                ? queryRequest.model()
                : null)
        .structuredOutput(true)
        .maxTokens(queryRequest.maxTokens() != null ? queryRequest.maxTokens() : null)
        .jsonSchema(QUERY_SQL_RESPONSE_JSON_SCHEMA)
        .build();
  }

  /**
   * Builds a dashboard SQL generation request.
   *
   * @param queryRequest the query request containing the prompt and allowed schema
   * @return an AiRequest for the AI to generate a dashboard plan with SQL for each widget
   */
  private AiRequest buildDashboardSqlRequest(QueryRequest queryRequest) {
    String content = DASHBOARD_SQL_SYSTEM_PROMPT + queryRequest.schemaContext().toPromptText();
    return AiRequest.builder()
        .systemMessage(content)
        .userMessage(queryRequest.prompt())
        .temperature(queryRequest.temperature() != null ? queryRequest.temperature() : 0.0)
        .model(
            (queryRequest.model() != null && !queryRequest.model().isBlank())
                ? queryRequest.model()
                : null)
        .structuredOutput(true)
        .maxTokens(queryRequest.maxTokens() != null ? queryRequest.maxTokens() : null)
        .jsonSchema(DASHBOARD_SQL_RESPONSE_JSON_SCHEMA)
        .build();
  }

  /**
   * Builds dashboard SQL correction request.
   *
   * @param queryRequest the original query request containing the prompt and allowed schema
   * @param previousPlan invalid dashboard plan returned by the AI
   * @param issues list of validation issues found in the previous plan
   * @return an AiRequest for the AI to correct the dashboard plan
   */
  private AiRequest buildDashboardSqlCorrectionRequest(
      QueryRequest queryRequest, DashboardPlan previousPlan, List<String> issues) {
    String correctionMessage =
        queryRequest.prompt()
            + "\n\n---\nCorrection needed. Your previous dashboard plan:\n"
            + previousPlan.toPromptText()
            + "\n\nIssues to fix:\n"
            + formatIssues(issues)
            + "\n\nReturn corrected dashboard plan JSON in the same JSON format.";

    return AiRequest.builder()
        .systemMessage(DASHBOARD_SQL_SYSTEM_PROMPT + queryRequest.schemaContext().toPromptText())
        .userMessage(correctionMessage)
        .temperature(queryRequest.temperature() != null ? queryRequest.temperature() : 0.0)
        .model(
            (queryRequest.model() != null && !queryRequest.model().isBlank())
                ? queryRequest.model()
                : null)
        .structuredOutput(true)
        .maxTokens(queryRequest.maxTokens() != null ? queryRequest.maxTokens() : null)
        .jsonSchema(DASHBOARD_SQL_RESPONSE_JSON_SCHEMA)
        .build();
  }

  /**
   * Parses the AI response as a SQL response JSON.
   *
   * <p>Expected format: {"sql": "...", "ambiguous": false, "ambiguityNote": null}
   *
   * @param response the AI response to parse
   * @param result the QueryResult.Builder to record validation issues
   * @return an AiSqlResponse containing the SQL or ambiguity information, or null if parsing failed
   */
  private AiSqlResponse parseSqlResponse(AiResponse response, QueryResult.Builder result) {
    String json = response.structuredContent();
    if (json == null || json.isBlank()) {
      result.validationIssue("AI response contained no structured content");
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);

      if (root.path("ambiguous").asBoolean(false)) {
        String note = root.path("ambiguityNote").asText("Request was ambiguous");
        result.interpretationNote(note);
        result.validationIssue("Request was ambiguous: " + note);
        return AiSqlResponse.ambiguous(note);
      }

      String sql = root.path("sql").asText(null);
      if (sql == null || sql.isBlank()) {
        result.validationIssue("AI response had no 'sql' field");
        return null;
      }

      sql = stripMarkdownCodeFences(sql);

      return AiSqlResponse.ofSql(sql);
    } catch (Exception e) {
      result.validationIssue("Failed to parse AI SQL response: " + e.getMessage());
      return null;
    }
  }

  /**
   * Some models wrap SQL in markdown code fences despite being told not to. Strip them defensively.
   *
   * @param sql the SQL string to strip
   * @return the SQL string without markdown code fences
   */
  private static String stripMarkdownCodeFences(String sql) {
    String trimmed = sql.trim();
    if (trimmed.startsWith("```")) {
      int firstNewline = trimmed.indexOf('\n');
      if (firstNewline > 0) {
        trimmed = trimmed.substring(firstNewline + 1);
      }
      if (trimmed.endsWith("```")) {
        trimmed = trimmed.substring(0, trimmed.lastIndexOf("```")).trim();
      }
    }
    return trimmed;
  }

  /**
   * Formats a list of issues into a numbered list string.
   *
   * @param issues list of issues to format
   * @return a numbered list of issues as a string, each issue on a new line
   */
  private static String formatIssues(List<String> issues) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < issues.size(); i++) {
      sb.append(i + 1).append(". ").append(issues.get(i)).append('\n');
    }
    return sb.toString();
  }

  /** Builder for {@link QuerierAi}. */
  public static final class Builder {
    private AiClient client;
    private RetryPolicy retryPolicy;

    private Builder() {}

    /**
     * Sets the {@link AiClient} to use for sending requests to the AI service.
     *
     * @param client the {@link AiClient} to use for sending requests to the AI service
     * @return this builder for chaining
     */
    public Builder client(AiClient client) {
      this.client = client;
      return this;
    }

    /**
     * Sets the {@link RetryPolicy} to use for retrying failed requests.
     *
     * @param retryPolicy the {@link RetryPolicy} to use for retrying failed requests
     * @return this builder for chaining
     */
    public Builder retryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Builds a new {@link QuerierAi} instance with the configured {@link AiClient} and {@link
     * RetryPolicy}.
     *
     * @return a new {@link QuerierAi} instance with the configured {@link AiClient} and {@link
     *     RetryPolicy}
     */
    public QuerierAi build() {
      return new QuerierAi(this);
    }
  }
}
