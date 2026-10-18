package com.aytmatech.querier.ai;

import com.aytmatech.querier.ai.error.AiException;

/**
 * Provider-neutral interface used by all Querier AI features to communicate with an
 * OpenAI-compatible language model endpoint.
 *
 * <p>The default implementation is {@link com.aytmatech.querier.ai.client.OpenAiCompatibleClient}.
 * Alternative implementations (test stubs, custom transports) can be substituted freely.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * AiClient client = AiClient.builder()
 *     .apiKey(System.getenv("QUERIER_AI_API_KEY"))
 *     .baseUrl(System.getenv("QUERIER_AI_BASE_URL"))
 *     .model(System.getenv("QUERIER_AI_MODEL"))
 *     .build();
 * }</pre>
 */
public interface AiClient {

  /**
   * Sends a request to the configured endpoint and returns a normalised response.
   *
   * @param request the normalised request
   * @return normalised response
   * @throws AiException on any transport, authentication, rate-limit, or parsing failure
   */
  AiResponse send(AiRequest request) throws AiException;

  /**
   * Returns the declared capabilities of the endpoint/model.
   *
   * @return Returns the capability profile of this client's configured endpoint/model. Feature code
   *     must check required capabilities before use.
   */
  AiCapabilities capabilities();

  /**
   * Convenience factory: creates an {@link com.aytmatech.querier.ai.client.OpenAiCompatibleClient}
   * from the provided endpoint.
   *
   * @param endpoint connection configuration (base URL, key, model, timeouts)
   * @return a new {@link AiClient} instance
   */
  static AiClient of(AiEndpoint endpoint) {
    return of(endpoint, AiCapabilities.defaults());
  }

  /**
   * Convenience factory: creates an {@link com.aytmatech.querier.ai.client.OpenAiCompatibleClient}
   * from the provided endpoint and declared capabilities.
   *
   * @param endpoint connection configuration (base URL, key, model, timeouts)
   * @param capabilities declared capabilities of the endpoint/model
   * @return a new {@link AiClient} instance
   */
  static AiClient of(AiEndpoint endpoint, AiCapabilities capabilities) {
    return new com.aytmatech.querier.ai.client.OpenAiCompatibleClient(endpoint, capabilities);
  }

  /**
   * Convenience factory: creates an {@link com.aytmatech.querier.ai.client.OpenAiCompatibleClient}
   * from the provided endpoint and declared capabilities.
   *
   * @return Returns a {@link Builder} that produces an {@link
   *     com.aytmatech.querier.ai.client.OpenAiCompatibleClient}.
   */
  static Builder builder() {
    return new Builder();
  }

  /** Convenience builder that mirrors the most common {@link AiEndpoint} settings. */
  final class Builder {

    private final AiEndpoint.Builder endpointBuilder = AiEndpoint.builder();
    private AiCapabilities capabilities = AiCapabilities.defaults();

    private Builder() {}

    /**
     * Sets the base URL of the endpoint.
     *
     * @param apiKey the API key to use for authentication
     * @return this builder
     */
    public Builder apiKey(String apiKey) {
      endpointBuilder.apiKey(apiKey);
      return this;
    }

    /**
     * Sets the base URL of the endpoint.
     *
     * @param baseUrl the base URL of the endpoint (e.g. {@code https://api.openai.com/v1})
     * @return this builder
     */
    public Builder baseUrl(String baseUrl) {
      endpointBuilder.baseUrl(baseUrl);
      return this;
    }

    /**
     * Sets the model to use for requests.
     *
     * @param model the model identifier to use for requests (e.g. {@code gpt-4o-mini})
     * @return this builder
     */
    public Builder model(String model) {
      endpointBuilder.model(model);
      return this;
    }

    /**
     * Sets the declared capabilities of the endpoint/model.
     *
     * @param capabilities the declared capabilities of the endpoint/model
     * @return this builder
     */
    public Builder capabilities(AiCapabilities capabilities) {
      this.capabilities = capabilities;
      return this;
    }

    /**
     * Builds a new {@link AiClient} instance with the configured settings.
     *
     * @return a new {@link AiClient} instance
     */
    public AiClient build() {
      return AiClient.of(endpointBuilder.build(), capabilities);
    }
  }
}
