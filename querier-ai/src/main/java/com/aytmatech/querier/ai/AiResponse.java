package com.aytmatech.querier.ai;

import java.util.Optional;

/**
 * Normalised response returned by an {@link AiClient}.
 *
 * <p>The transport implementation maps provider-specific JSON to this type before returning it to
 * feature code. Feature code must never parse raw provider JSON.
 */
public final class AiResponse {

  private final String text;
  private final String structuredContent;
  private final String finishReason;
  private final Integer inputTokens;
  private final Integer outputTokens;

  private AiResponse(Builder b) {
    this.text = b.text;
    this.structuredContent = b.structuredContent;
    this.finishReason = b.finishReason;
    this.inputTokens = b.inputTokens;
    this.outputTokens = b.outputTokens;
  }

  /**
   * Returns a new builder for {@link AiResponse}.
   *
   * @return Returns a new {@link Builder} for constructing an {@link AiResponse} instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Plain text content of the first assistant message. May be {@code null} for structured-only
   * responses.
   *
   * @return Plain text content of the first assistant message. May be {@code null} for
   *     structured-only responses.
   */
  public String text() {
    return text;
  }

  /**
   * Raw structured content (JSON string) returned by the model when structured output was
   * requested. May be {@code null} when the endpoint returned plain text only.
   *
   * @return Raw structured content (JSON string) returned by the model when structured output was
   *     requested. May be {@code null} when the endpoint returned plain text only.
   */
  public String structuredContent() {
    return structuredContent;
  }

  /**
   * Finish reason as returned by the provider, e.g. {@code "stop"}, {@code "length"}, {@code
   * "content_filter"}.
   *
   * @return Finish reason as returned by the provider, e.g. {@code "stop"}, {@code "length"},
   *     {@code "content_filter"}.
   */
  public Optional<String> finishReason() {
    return Optional.ofNullable(finishReason);
  }

  /**
   * Total input tokens consumed, when reported by the endpoint.
   *
   * @return Total input tokens consumed, when reported by the endpoint.
   */
  public Optional<Integer> inputTokens() {
    return Optional.ofNullable(inputTokens);
  }

  /**
   * Total output tokens generated, when reported by the endpoint.
   *
   * @return Total output tokens generated, when reported by the endpoint.
   */
  public Optional<Integer> outputTokens() {
    return Optional.ofNullable(outputTokens);
  }

  /** Builder for {@link AiResponse}. */
  public static final class Builder {
    private String text;
    private String structuredContent;
    private String finishReason;
    private Integer inputTokens;
    private Integer outputTokens;

    private Builder() {}

    /**
     * Sets the plain text content of the first assistant message.
     *
     * @param text Plain text content of the first assistant message. May be {@code null} for
     *     structured-only responses.
     * @return this builder
     */
    public Builder text(String text) {
      this.text = text;
      return this;
    }

    /**
     * Sets the raw structured content (JSON string) returned by the model when structured output
     * was requested.
     *
     * @param structuredContent Raw structured content (JSON string) returned by the model when
     *     structured output was requested. May be {@code null} when the endpoint returned plain
     *     text only.
     * @return this builder
     */
    public Builder structuredContent(String structuredContent) {
      this.structuredContent = structuredContent;
      return this;
    }

    /**
     * Sets the finish reason as returned by the provider, e.g. {@code "stop"}, {@code "length"},
     * {@code "content_filter"}.
     *
     * @param finishReason Finish reason as returned by the provider, e.g. {@code "stop"}, {@code
     *     "length"}, {@code "content_filter"}.
     * @return this builder
     */
    public Builder finishReason(String finishReason) {
      this.finishReason = finishReason;
      return this;
    }

    /**
     * Sets the total input tokens consumed, when reported by the endpoint.
     *
     * @param inputTokens Total input tokens consumed, when reported by the endpoint.
     * @return this builder
     */
    public Builder inputTokens(int inputTokens) {
      this.inputTokens = inputTokens;
      return this;
    }

    /**
     * Sets the total output tokens generated, when reported by the endpoint.
     *
     * @param outputTokens Total output tokens generated, when reported by the endpoint.
     * @return this builder
     */
    public Builder outputTokens(int outputTokens) {
      this.outputTokens = outputTokens;
      return this;
    }

    /**
     * Builds the {@link AiResponse} instance.
     *
     * @return a new {@link AiResponse} instance with the values set in this builder
     */
    public AiResponse build() {
      return new AiResponse(this);
    }
  }
}
