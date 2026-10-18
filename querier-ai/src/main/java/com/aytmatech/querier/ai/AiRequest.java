package com.aytmatech.querier.ai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Normalised request sent to an {@link AiClient}.
 *
 * <p>This model is provider-neutral. The {@link
 * com.aytmatech.querier.ai.client.OpenAiCompatibleClient} translates it to the OpenAI-style JSON
 * payload before transmission.
 */
public final class AiRequest {

  /** System role constant for chat messages. */
  public static final String ROLE_SYSTEM = "system";

  /** User role constant for chat messages. */
  public static final String ROLE_USER = "user";

  /** Assistant role constant for chat messages. */
  public static final String ROLE_ASSISTANT = "assistant";

  private final String model;
  private final List<Message> messages;
  private final Double temperature;
  private final Integer maxTokens;
  private final boolean structuredOutput;
  private final String jsonSchema;

  private AiRequest(Builder b) {
    this.model = b.model;
    this.messages = Collections.unmodifiableList(new ArrayList<>(b.messages));
    this.temperature = b.temperature;
    this.maxTokens = b.maxTokens;
    this.structuredOutput = b.structuredOutput;
    this.jsonSchema = b.jsonSchema;
  }

  /**
   * Creates a new {@link Builder} for constructing an {@link AiRequest} instance.
   *
   * @return a new {@link Builder} for constructing an {@link AiRequest} instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Model name to use for the request.
   *
   * @return Overrides the model for this request; when {@code null} the client uses the model
   *     configured on {@link AiEndpoint}.
   */
  public String model() {
    return model;
  }

  /**
   * List of messages in the conversation. The first message is typically a system message, followed
   * by user and assistant messages.
   *
   * @return The list of messages in the conversation, in order. The first message is usually a
   *     system message, followed by alternating user and assistant messages.
   */
  public List<Message> messages() {
    return messages;
  }

  /**
   * Sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more
   * random, while lower values like 0.2 will make it more focused and deterministic.
   *
   * @return Sampling temperature, e.g. {@code 0.0} for deterministic output. May be {@code null}.
   */
  public Double temperature() {
    return temperature;
  }

  /**
   * Max tokens to generate. May be {@code null} to use the default configured on the endpoint.
   *
   * @return Maximum output tokens. May be {@code null} to use the endpoint default.
   */
  public Integer maxTokens() {
    return maxTokens;
  }

  /**
   * Structured output flag. If {@code true}, the client will request structured output from the
   * model, e.g. JSON-schema-constrained output.
   *
   * @return Whether to request structured JSON output from the endpoint.
   */
  public boolean structuredOutput() {
    return structuredOutput;
  }

  /**
   * JSON schema to constrain the output when {@link #structuredOutput()} is {@code true}. May be
   * {@code null} to use the endpoint default.
   *
   * @return JSON schema string constraining the response when {@link #structuredOutput()} is {@code
   *     true}. May be {@code null} when using basic JSON mode without a schema.
   */
  public String jsonSchema() {
    return jsonSchema;
  }

  /**
   * Immutable chat message.
   *
   * @param role one of {@link #ROLE_SYSTEM}, {@link #ROLE_USER}, or {@link #ROLE_ASSISTANT}
   * @param content the message text
   */
  public record Message(String role, String content) {
    /**
     * Message constructor.
     *
     * @param role one of {@link #ROLE_SYSTEM}, {@link #ROLE_USER}, or {@link #ROLE_ASSISTANT}
     * @param content the message text
     */
    public Message {
      Objects.requireNonNull(role, "role");
      Objects.requireNonNull(content, "content");
    }
  }

  /** Builder for constructing an {@link AiRequest} instance. */
  public static final class Builder {

    private String model;
    private final List<Message> messages = new ArrayList<>();
    private Double temperature;
    private Integer maxTokens;
    private boolean structuredOutput;
    private String jsonSchema;

    private Builder() {}

    /**
     * Sets the model for this request. If not set, the client will use the model configured on
     * {@link AiEndpoint}.
     *
     * @param model model name to use for this request; when {@code null} the client uses the model
     *     configured on {@link AiEndpoint}.
     * @return this builder
     */
    public Builder model(String model) {
      this.model = model;
      return this;
    }

    /**
     * Adds a message to the conversation.
     *
     * @param content message text
     * @return this builder
     */
    public Builder systemMessage(String content) {
      messages.add(new Message(ROLE_SYSTEM, content));
      return this;
    }

    /**
     * Adds a user message to the conversation.
     *
     * @param content message text
     * @return this builder
     */
    public Builder userMessage(String content) {
      messages.add(new Message(ROLE_USER, content));
      return this;
    }

    /**
     * Adds an assistant message to the conversation.
     *
     * @param content message text
     * @return this builder
     */
    public Builder assistantMessage(String content) {
      messages.add(new Message(ROLE_ASSISTANT, content));
      return this;
    }

    /**
     * Adds a message with the given role and content.
     *
     * @param role one of {@link #ROLE_SYSTEM}, {@link #ROLE_USER}, or {@link #ROLE_ASSISTANT}
     * @param content message text
     * @return this builder
     */
    public Builder message(String role, String content) {
      messages.add(new Message(role, content));
      return this;
    }

    /**
     * Sets the sampling temperature for this request.
     *
     * @param temperature sampling temperature, e.g. {@code 0.0} for deterministic output
     * @return this builder
     */
    public Builder temperature(Double temperature) {
      this.temperature = temperature;
      return this;
    }

    /**
     * Sets the maximum number of output tokens for the request.
     *
     * @param maxTokens maximum output tokens
     * @return this builder
     */
    public Builder maxTokens(Integer maxTokens) {
      this.maxTokens = maxTokens;
      return this;
    }

    /**
     * Sets whether to request structured JSON output from the endpoint.
     *
     * @param structuredOutput whether to request structured JSON output from the endpoint
     * @return this builder
     */
    public Builder structuredOutput(boolean structuredOutput) {
      this.structuredOutput = structuredOutput;
      return this;
    }

    /**
     * Sets the JSON schema string constraining the response when {@link #structuredOutput()} is
     * {@code true}.
     *
     * @param jsonSchema JSON schema string constraining the response when {@link
     *     #structuredOutput()} is {@code true}
     * @return this builder
     */
    public Builder jsonSchema(String jsonSchema) {
      this.jsonSchema = jsonSchema;
      return this;
    }

    /**
     * Builds an {@link AiRequest} instance with the configured properties.
     *
     * @return an {@link AiRequest} instance built from the current state of this builder
     * @throws IllegalStateException if no messages have been added to the request
     */
    public AiRequest build() {
      if (messages.isEmpty()) {
        throw new IllegalStateException("At least one message is required");
      }
      return new AiRequest(this);
    }
  }
}
