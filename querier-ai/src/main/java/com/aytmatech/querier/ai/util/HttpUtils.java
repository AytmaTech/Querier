package com.aytmatech.querier.ai.util;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Map;

/** Utility class for HTTP-related operations. */
public final class HttpUtils {

  private HttpUtils() {}

  /**
   * Builds an HTTP request for the given endpoint and request body.
   *
   * @param url the URL to create a URI from
   * @param method the HTTP method (e.g., GET, POST)
   * @param body the request body as a string
   * @param requestTimeout the timeout for the request
   * @param headers a map of header names to values
   * @return an HttpRequest object configured with the given parameters
   */
  public static HttpRequest buildHttpRequest(
      String url,
      String method,
      String body,
      Duration requestTimeout,
      Map<String, String> headers) {
    URI uri = URI.create(url);
    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(uri)
            .timeout(requestTimeout)
            .method(method, HttpRequest.BodyPublishers.ofString(body));

    headers.forEach(builder::header);
    return builder.build();
  }
}
