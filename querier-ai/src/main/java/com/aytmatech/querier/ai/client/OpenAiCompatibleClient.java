package com.aytmatech.querier.ai.client;

import com.aytmatech.querier.ai.AiCapabilities;
import com.aytmatech.querier.ai.AiClient;
import com.aytmatech.querier.ai.AiEndpoint;
import com.aytmatech.querier.ai.AiRequest;
import com.aytmatech.querier.ai.AiResponse;
import com.aytmatech.querier.ai.error.AiErrorCategory;
import com.aytmatech.querier.ai.error.AiException;
import com.aytmatech.querier.ai.util.HttpUtils;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.http.*;
import java.util.*;

/**
 * Default {@link AiClient} implementation that communicates with any endpoint exposing an
 * OpenAI-compatible Chat Completions API.
 *
 * <p>Protocol details (JSON serialisation, HTTP headers, error parsing) are isolated here. Feature
 * code depends only on {@link AiClient}.
 *
 * <p>Supported endpoints include OpenAI, Azure OpenAI, Mistral AI, Groq, Ollama (local), LM Studio,
 * and any other service following the {@code POST /chat/completions} contract.
 */
public final class OpenAiCompatibleClient implements AiClient {

  private static final String CHAT_COMPLETIONS_PATH = "/chat/completions";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final AiEndpoint endpoint;
  private final AiCapabilities capabilities;
  private final HttpClient httpClient;

  /**
   * Creates a client for the given endpoint with the supplied capability profile.
   *
   * @param endpoint connection configuration (base URL, key, model, timeouts)
   * @param capabilities declared capabilities of the endpoint/model
   */
  public OpenAiCompatibleClient(AiEndpoint endpoint, AiCapabilities capabilities) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
    this.httpClient = HttpClient.newBuilder().connectTimeout(endpoint.connectTimeout()).build();
  }

  @Override
  public AiCapabilities capabilities() {
    return capabilities;
  }

  @Override
  public AiResponse send(AiRequest request) throws AiException {
    if (!capabilities.chatCompletions()) {
      throw new AiException(
          AiErrorCategory.UNSUPPORTED_CAPABILITY,
          "Chat completions are not enabled for this endpoint");
    }
    if (request.structuredOutput() && !capabilities.structuredOutput()) {
      throw new AiException(
          AiErrorCategory.UNSUPPORTED_CAPABILITY,
          "Structured output was requested but is not enabled for this endpoint");
    }

    Map<String, String> requestHeaders = new HashMap<>();
    requestHeaders.put("Content-Type", "application/json");
    requestHeaders.put("Authorization", "Bearer " + endpoint.apiKey());
    requestHeaders.putAll(endpoint.headers());
    String body = buildRequestBody(request);
    HttpRequest httpRequest =
        HttpUtils.buildHttpRequest(
            endpoint.baseUrl().replaceAll("/+$", "") + CHAT_COMPLETIONS_PATH,
            "POST",
            body,
            endpoint.requestTimeout(),
            requestHeaders);

    HttpResponse<String> response;
    try {
      response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    } catch (HttpTimeoutException e) {
      throw new AiException(AiErrorCategory.TIMEOUT, "Request timed out", e);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new AiException(
          AiErrorCategory.ENDPOINT_UNAVAILABLE,
          "Failed to reach endpoint: " + endpoint.baseUrl(),
          e);
    }

    return parseResponse(response);
  }

  /**
   * Builds the request body as a JSON string for the given {@link AiRequest}.
   *
   * @param request the request to send
   * @return the JSON body to send to the endpoint
   * @throws AiException if the request cannot be serialized to JSON
   */
  private String buildRequestBody(AiRequest request) {
    try {
      ObjectNode root = MAPPER.createObjectNode();
      root.put("model", request.model() != null ? request.model() : endpoint.model());

      ArrayNode msgs = root.putArray("messages");
      for (AiRequest.Message m : request.messages()) {
        ObjectNode msg = msgs.addObject();
        msg.put("role", m.role());
        msg.put("content", m.content());
      }

      if (request.temperature() != null) {
        root.put("temperature", request.temperature());
      }
      if (request.maxTokens() != null) {
        root.put("max_tokens", request.maxTokens());
      }

      if (request.structuredOutput()) {
        if (request.jsonSchema() != null) {
          ObjectNode responseFormat = root.putObject("response_format");
          responseFormat.put("type", "json_schema");
          ObjectNode schemaWrapper = responseFormat.putObject("json_schema");
          schemaWrapper.put("name", "querier_response");
          schemaWrapper.put("strict", true);
          schemaWrapper.set("schema", MAPPER.readTree(request.jsonSchema()));
        } else {
          ObjectNode responseFormat = root.putObject("response_format");
          responseFormat.put("type", "json_object");
        }
      }
      return MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      throw new AiException(AiErrorCategory.CONFIGURATION, "Failed to serialise request body", e);
    }
  }

  /**
   * Parses the HTTP response from the endpoint into an {@link AiResponse}.
   *
   * @param response the HTTP response from the endpoint
   * @return the parsed {@link AiResponse}
   * @throws AiException if the response cannot be parsed or indicates an error
   */
  private AiResponse parseResponse(HttpResponse<String> response) {
    int status = response.statusCode();

    if (status == 401 || status == 403) {
      throw new AiException(
          AiErrorCategory.AUTHENTICATION,
          "Authentication rejected by endpoint (HTTP " + status + ")");
    }
    if (status == 429) {
      throw new AiException(
          AiErrorCategory.RATE_LIMIT, "Rate limit reached (HTTP 429). Retry after a backoff.");
    }
    if (status < 200 || status >= 300) {
      String detail = extractErrorMessage(response.body());
      throw new AiException(
          AiErrorCategory.PROVIDER_ERROR,
          "Endpoint returned HTTP " + status + (detail != null ? ": " + detail : ""));
    }

    try {
      JsonNode root = MAPPER.readTree(response.body());
      JsonNode choices = root.path("choices");
      if (!choices.isArray() || choices.isEmpty()) {
        throw new AiException(AiErrorCategory.MALFORMED_RESPONSE, "Response contained no choices");
      }

      JsonNode first = choices.get(0);
      JsonNode messageNode = first.path("message");
      String text = messageNode.path("content").asText(null);
      String finishReason = first.path("finish_reason").asText(null);

      AiResponse.Builder rb = AiResponse.builder().text(text).finishReason(finishReason);

      if (isValidJson(text) && capabilities.structuredOutput()) {
        rb.structuredContent(text);
      }

      JsonNode usage = root.path("usage");
      if (usage.has("prompt_tokens")) {
        rb.inputTokens(usage.get("prompt_tokens").asInt());
      }
      if (usage.has("completion_tokens")) {
        rb.outputTokens(usage.get("completion_tokens").asInt());
      }

      return rb.build();
    } catch (AiException e) {
      throw e;
    } catch (Exception e) {
      throw new AiException(
          AiErrorCategory.MALFORMED_RESPONSE, "Failed to parse endpoint response", e);
    }
  }

  /**
   * Extracts an error message from the endpoint response body, if present.
   *
   * @param body the response body from the endpoint
   * @return the extracted error message, or null if none could be found
   */
  private String extractErrorMessage(String body) {
    try {
      JsonNode root = MAPPER.readTree(body);
      JsonNode error = root.path("error");
      if (error.has("message")) {
        return error.get("message").asText();
      }
    } catch (JacksonException ignored) {
      // best-effort
    }
    return null;
  }

  private boolean isValidJson(String text) {
    if (text == null) return false;
    String t = text.strip();
    return (t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"));
  }
}
