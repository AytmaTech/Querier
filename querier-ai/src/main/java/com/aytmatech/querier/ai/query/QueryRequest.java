package com.aytmatech.querier.ai.query;

import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import java.util.Objects;

/**
 * Input to the natural-language query feature.
 *
 * <p>Use the static {@link #builder()} factory:
 *
 * <pre>{@code
 * QueryRequest request = QueryRequest.builder()
 *     .prompt("Show the top 10 customers by revenue in the last 30 days")
 *     .entities(Customer.class, Order.class)
 *     .build();
 * }</pre>
 */
public final class QueryRequest {

  private final String prompt;
  private final AiSchemaContext schemaContext;
  private final Double temperature;
  private final String model;
  private final Integer maxTokens;
  private final String jsonSchema;

  private QueryRequest(Builder b) {
    this.prompt = Objects.requireNonNull(b.prompt, "prompt must not be null");
    this.schemaContext = Objects.requireNonNull(b.schemaContext, "schemaContext must not be null");
    this.temperature = b.temperature;
    this.model = b.model;
    this.maxTokens = b.maxTokens;
    this.jsonSchema = b.jsonSchema;
  }

  /**
   * Returns a new {@link Builder} for constructing a {@link QueryRequest} instance.
   *
   * @return a new {@link Builder} for constructing a {@link QueryRequest} instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The natural-language description of the desired query.
   *
   * @return the natural-language description of the desired query
   */
  public String prompt() {
    return prompt;
  }

  /**
   * The schema context restricting which entities and fields the AI may reference.
   *
   * @return the schema context restricting which entities and fields the AI may reference
   */
  public AiSchemaContext schemaContext() {
    return schemaContext;
  }

  /**
   * Returns the temperature to use for the query. Higher values (e.g., 0.8) make the output more
   * random, while lower values (e.g., 0.2) make it more focused and deterministic.
   *
   * @return the temperature to use for the query; when {@code null} the client uses the temperature
   *     configured on {@link com.aytmatech.querier.ai.AiEndpoint}
   */
  public Double temperature() {
    return temperature;
  }

  /**
   * Returns the model name to use for this request. If {@code null}, the client uses the model
   * configured on {@link com.aytmatech.querier.ai.AiEndpoint}.
   *
   * @return the model name to use for this request; when {@code null} the client uses the model
   *     configured on {@link com.aytmatech.querier.ai.AiEndpoint}
   */
  public String model() {
    return model;
  }

  /**
   * Returns the maximum number of tokens to generate in the response. If {@code null}, the client
   * uses the maxTokens configured on {@link com.aytmatech.querier.ai.AiEndpoint}.
   *
   * @return the maximum number of tokens to generate in the response; when {@code null} the client
   *     uses the maxTokens configured on {@link com.aytmatech.querier.ai.AiEndpoint}
   */
  public Integer maxTokens() {
    return maxTokens;
  }

  /**
   * Returns the JSON schema to constrain the output of the AI. If {@code null}, the client uses the
   * jsonSchema configured on {@link com.aytmatech.querier.ai.AiEndpoint}.
   *
   * @return the JSON schema to constrain the output of the AI; when {@code null} the client uses
   *     the jsonSchema configured on {@link com.aytmatech.querier.ai.AiEndpoint}
   */
  public String jsonSchema() {
    return jsonSchema;
  }

  /**
   * Builder for {@link QueryRequest} instances. Use {@link QueryRequest#builder()} to obtain an
   * instance.
   */
  public static final class Builder {
    private String prompt;
    private AiSchemaContext schemaContext;
    private Double temperature;
    private String model;
    private Integer maxTokens;
    private String jsonSchema;

    private Builder() {}

    /**
     * Sets the natural-language description of the desired query.
     *
     * @param prompt the natural-language description of the desired query
     * @return this builder
     */
    public Builder prompt(String prompt) {
      this.prompt = prompt;
      return this;
    }

    /**
     * Supplies the schema context directly.
     *
     * @param context pre-built schema context
     * @return this builder
     */
    public Builder schemaContext(AiSchemaContext context) {
      this.schemaContext = context;
      return this;
    }

    /**
     * Sets the temperature for the query. Higher values (e.g., 0.8) make the output more random,
     * while lower values (e.g., 0.2) make it more focused and deterministic.
     *
     * @param temperature the temperature to use for the query; when {@code null} the client uses
     *     the temperature configured on {@link com.aytmatech.querier.ai.AiEndpoint}
     * @return this builder
     */
    public Builder temperature(Double temperature) {
      this.temperature = temperature;
      return this;
    }

    /**
     * Sets the model to use for this request. If {@code null}, the client uses the model configured
     * on {@link com.aytmatech.querier.ai.AiEndpoint}.
     *
     * @param model model name to use for this request; when {@code null} the client uses the model
     *     configured on {@link com.aytmatech.querier.ai.AiEndpoint}
     * @return this builder
     */
    public Builder model(String model) {
      this.model = model;
      return this;
    }

    /**
     * Sets the maximum number of tokens to generate in the response. If {@code null}, the client
     * uses the maxTokens configured on {@link com.aytmatech.querier.ai.AiEndpoint}.
     *
     * @param maxTokens the maximum number of tokens to generate in the response; when {@code null}
     *     the client uses the maxTokens configured on {@link com.aytmatech.querier.ai.AiEndpoint}
     * @return this builder
     */
    public Builder maxTokens(Integer maxTokens) {
      this.maxTokens = maxTokens;
      return this;
    }

    /**
     * Sets the JSON schema to constrain the output of the AI. If {@code null}, the client uses the
     * jsonSchema configured on {@link com.aytmatech.querier.ai.AiEndpoint}.
     *
     * @param jsonSchema JSON schema to constrain the output of the AI; when {@code null} the client
     *     uses the jsonSchema configured on {@link com.aytmatech.querier.ai.AiEndpoint}
     * @return this builder
     */
    public Builder jsonSchema(String jsonSchema) {
      this.jsonSchema = jsonSchema;
      return this;
    }

    /**
     * Inspects the given entity classes using {@link AiSchemaInspector} and uses the resulting
     * context.
     *
     * @param entityClasses entity classes to expose to the AI
     * @return this builder
     */
    public Builder entities(Class<?>... entityClasses) {
      this.schemaContext = AiSchemaInspector.inspect(entityClasses);
      return this;
    }

    /**
     * Builds the {@link QueryRequest} instance with the configured parameters.
     *
     * @return a new {@link QueryRequest} instance with the configured parameters
     */
    public QueryRequest build() {
      return new QueryRequest(this);
    }
  }
}
