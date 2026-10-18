package com.aytmatech.querier.ai.error;

/**
 * Categorises {@link AiException} so calling code can distinguish transient infrastructure failures
 * from permanent configuration or validation problems.
 */
public enum AiErrorCategory {

  /** Missing or invalid endpoint, API key, or model identifier. */
  CONFIGURATION,

  /** The API key or credential was rejected by the endpoint. */
  AUTHENTICATION,

  /** The endpoint was unreachable or returned a non-recoverable HTTP error. */
  ENDPOINT_UNAVAILABLE,

  /** The request timed out. */
  TIMEOUT,

  /** The endpoint returned a rate-limit response. */
  RATE_LIMIT,

  /** The provider returned a non-2xx error with an API error body. */
  PROVIDER_ERROR,

  /** The provider returned a response that could not be parsed. */
  MALFORMED_RESPONSE,

  /** The configured endpoint/model does not support a required capability. */
  UNSUPPORTED_CAPABILITY,

  /** The model returned structured output that failed schema validation. */
  INVALID_STRUCTURED_OUTPUT,

  /** A safety or policy filter rejected the request or response. */
  SAFETY_REJECTION,
}
