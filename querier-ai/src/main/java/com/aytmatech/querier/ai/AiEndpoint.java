package com.aytmatech.querier.ai;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable configuration for an OpenAI-compatible AI endpoint.
 *
 * <p>The API key is treated as a secret: it is excluded from {@link #toString()}, {@link
 * #equals(Object)}, and {@link #hashCode()} to prevent accidental exposure in logs, exception
 * messages, or debug output.
 *
 * <p>Configure programmatically:
 *
 * <pre>{@code
 * AiEndpoint endpoint = AiEndpoint.builder()
 *     .baseUrl(System.getenv("QUERIER_AI_BASE_URL"))
 *     .apiKey(System.getenv("QUERIER_AI_API_KEY"))
 *     .model(System.getenv("QUERIER_AI_MODEL"))
 *     .build();
 * }</pre>
 */
public final class AiEndpoint {

  /** Default environment variable name for the API key. */
  public static final String ENV_API_KEY = "QUERIER_AI_API_KEY";

  /** Default environment variable name for the base URL. */
  public static final String ENV_BASE_URL = "QUERIER_AI_BASE_URL";

  /** Default environment variable name for the model identifier. */
  public static final String ENV_MODEL = "QUERIER_AI_MODEL";

  private final String baseUrl;
  private final String apiKey;
  private final String model;
  private final Map<String, String> headers;
  private final Duration connectTimeout;
  private final Duration requestTimeout;

  private AiEndpoint(Builder b) {
    this.baseUrl = Objects.requireNonNull(b.baseUrl, "baseUrl must not be null");
    this.apiKey = Objects.requireNonNull(b.apiKey, "apiKey must not be null");
    this.model = Objects.requireNonNull(b.model, "model must not be null");
    this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(b.headers));
    this.connectTimeout = b.connectTimeout != null ? b.connectTimeout : Duration.ofSeconds(10);
    this.requestTimeout = b.requestTimeout != null ? b.requestTimeout : Duration.ofSeconds(60);
  }

  /**
   * Returns a new {@link Builder} for constructing an {@link AiEndpoint} populated from the
   * standard environment variables ({@value #ENV_API_KEY}, {@value #ENV_BASE_URL}, {@value
   * #ENV_MODEL}).
   *
   * @return Creates a new {@link Builder} populated from the standard environment variables
   *     ({@value #ENV_API_KEY}, {@value #ENV_BASE_URL}, {@value #ENV_MODEL}).
   */
  public static Builder fromEnvironment() {
    return builder()
        .baseUrl(System.getenv(ENV_BASE_URL))
        .apiKey(System.getenv(ENV_API_KEY))
        .model(System.getenv(ENV_MODEL));
  }

  /**
   * Creates a new {@link AiEndpoint} from the standard environment variables ({@value
   * #ENV_API_KEY}, {@value #ENV_BASE_URL}, {@value #ENV_MODEL}).
   *
   * @return Returns a new {@link Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Base URL of the AI endpoint, e.g. {@code https://api.openai.com/v1}.
   *
   * @return Base URL of the OpenAI-compatible endpoint, e.g. {@code https://api.openai.com/v1}.
   */
  public String baseUrl() {
    return baseUrl;
  }

  /**
   * API key for authenticating with the OpenAI-compatible endpoint. This is treated as a secret and
   * is excluded from {@link #toString()}, {@link #equals(Object)}, and {@link #hashCode()}.
   *
   * @return API key secret.
   *     <p><strong>Handle with care</strong> — do not log or include in exception messages.
   */
  public String apiKey() {
    return apiKey;
  }

  /**
   * Model identifier, e.g. {@code gpt-4o} or {@code openai/gpt-oss-20b}.
   *
   * @return Model identifier, e.g. {@code gpt-4o} or {@code mistral-large-latest}.
   */
  public String model() {
    return model;
  }

  /**
   * HTTP headers to include in every request to the endpoint.
   *
   * @return Additional HTTP headers sent with every request (e.g., routing or organisation
   *     headers).
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Connection timeout for the underlying HTTP client. Defaults to 10 seconds if not set.
   *
   * @return TCP connection timeout. Default: 10 seconds.
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Request timeout for each HTTP request. Default: 60 seconds.
   *
   * @return Request / read timeout. Default: 60 seconds.
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /** Returns a safe string that deliberately omits the API key. */
  @Override
  public String toString() {
    return "AiEndpoint{baseUrl='" + baseUrl + "', model='" + model + "', apiKey=[REDACTED]}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AiEndpoint other)) return false;
    return baseUrl.equals(other.baseUrl) && model.equals(other.model);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseUrl, model);
  }

  /** Fluent builder for {@link AiEndpoint}. */
  public static final class Builder {

    private String baseUrl;
    private String apiKey;
    private String model;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private Duration connectTimeout;
    private Duration requestTimeout;

    private Builder() {}

    /**
     * Sets the base URL of the OpenAI-compatible endpoint, e.g. {@code https://api.openai.com/v1}.
     *
     * @param baseUrl the base URL of the endpoint (e.g. {@code https://api.openai.com/v1})
     * @return this builder
     */
    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    /**
     * Sets the API key for authentication.
     *
     * @param apiKey the API key to use for authentication
     * @return this builder
     */
    public Builder apiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    /**
     * Sets the model identifier to use for requests (e.g. {@code gpt-4o} or {@code
     * mistral-large-latest}).
     *
     * @param model the model identifier to use (e.g. {@code gpt-4o} or {@code
     *     mistral-large-latest})
     * @return this builder
     */
    public Builder model(String model) {
      this.model = model;
      return this;
    }

    /**
     * Adds an additional HTTP header to include with every request.
     *
     * @param name the name of the HTTP header
     * @param value the value of the HTTP header
     * @return this builder
     */
    public Builder header(String name, String value) {
      this.headers.put(name, value);
      return this;
    }

    /**
     * Sets the TCP connection timeout for requests.
     *
     * @param connectTimeout the TCP connection timeout
     * @return this builder
     */
    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Sets the request / read timeout for requests.
     *
     * @param requestTimeout the request / read timeout
     * @return this builder
     */
    public Builder requestTimeout(Duration requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    /**
     * Builds a new {@link AiEndpoint} instance.
     *
     * @return Builds a new {@link AiEndpoint} instance.
     */
    public AiEndpoint build() {
      return new AiEndpoint(this);
    }
  }
}
