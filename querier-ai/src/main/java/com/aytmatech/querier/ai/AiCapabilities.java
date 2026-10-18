package com.aytmatech.querier.ai;

/**
 * Declares the capabilities known or assumed for a configured endpoint/model.
 *
 * <p>Feature code must check required capabilities before use. If a required capability is absent
 * the {@link com.aytmatech.querier.ai.error.AiException} with category {@link
 * com.aytmatech.querier.ai.error.AiErrorCategory#UNSUPPORTED_CAPABILITY} is thrown.
 *
 * <pre>{@code
 * AiCapabilities caps = AiCapabilities.builder()
 *     .chatCompletions(true)
 *     .structuredOutput(true)
 *     .build();
 * }</pre>
 */
public final class AiCapabilities {

  private final boolean chatCompletions;
  private final boolean structuredOutput;
  private final boolean toolCalling;
  private final boolean streaming;

  private AiCapabilities(Builder b) {
    this.chatCompletions = b.chatCompletions;
    this.structuredOutput = b.structuredOutput;
    this.toolCalling = b.toolCalling;
    this.streaming = b.streaming;
  }

  /**
   * Returns a new builder for {@link AiCapabilities} with defaults.
   *
   * @return Returns a sensible default: chat completions + structured output enabled; tool calling
   *     and streaming disabled.
   */
  public static AiCapabilities defaults() {
    return builder().chatCompletions(true).structuredOutput(true).build();
  }

  /**
   * Returns a new builder for {@link AiCapabilities}.
   *
   * @return Returns a new {@link Builder} for constructing an {@link AiCapabilities} instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns whether the endpoint supports chat completions.
   *
   * @return Whether the endpoint supports OpenAI-style chat completions.
   */
  public boolean chatCompletions() {
    return chatCompletions;
  }

  /**
   * Returns whether the endpoint supports structured output.
   *
   * @return Whether the endpoint supports JSON-schema-constrained structured output.
   */
  public boolean structuredOutput() {
    return structuredOutput;
  }

  /**
   * Returns whether the endpoint supports tool calling.
   *
   * @return Whether the endpoint supports tool/function calling.
   */
  public boolean toolCalling() {
    return toolCalling;
  }

  /**
   * Returns whether the endpoint supports streaming responses.
   *
   * @return Whether the endpoint supports streaming responses.
   */
  public boolean streaming() {
    return streaming;
  }

  @Override
  public String toString() {
    return "AiCapabilities{chatCompletions="
        + chatCompletions
        + ", structuredOutput="
        + structuredOutput
        + ", toolCalling="
        + toolCalling
        + ", streaming="
        + streaming
        + '}';
  }

  /** Builder for {@link AiCapabilities}. */
  public static final class Builder {
    private boolean chatCompletions;
    private boolean structuredOutput;
    private boolean toolCalling;
    private boolean streaming;

    private Builder() {}

    /**
     * Sets whether the endpoint supports chat completions.
     *
     * @param chatCompletions Whether the endpoint supports OpenAI-style chat completions.
     * @return this builder
     */
    public Builder chatCompletions(boolean chatCompletions) {
      this.chatCompletions = chatCompletions;
      return this;
    }

    /**
     * Sets whether the endpoint supports structured output.
     *
     * @param structuredOutput Whether the endpoint supports JSON-schema-constrained structured
     *     output.
     * @return this builder
     */
    public Builder structuredOutput(boolean structuredOutput) {
      this.structuredOutput = structuredOutput;
      return this;
    }

    /**
     * Sets whether the endpoint supports tool calling.
     *
     * @param toolCalling Whether the endpoint supports tool/function calling.
     * @return this builder
     */
    public Builder toolCalling(boolean toolCalling) {
      this.toolCalling = toolCalling;
      return this;
    }

    /**
     * Sets whether the endpoint supports streaming responses.
     *
     * @param streaming Whether the endpoint supports streaming responses.
     * @return this builder
     */
    public Builder streaming(boolean streaming) {
      this.streaming = streaming;
      return this;
    }

    /**
     * Builds an {@link AiCapabilities} instance with the configured values.
     *
     * @return Returns a new {@link AiCapabilities} instance with the configured capabilities.
     */
    public AiCapabilities build() {
      return new AiCapabilities(this);
    }
  }
}
