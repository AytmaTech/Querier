package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.client.OpenAiCompatibleClient;
import com.aytmatech.querier.ai.error.AiErrorCategory;
import com.aytmatech.querier.ai.error.AiException;
import org.junit.jupiter.api.Test;

class OpenAiCompatibleClientTest {

  @Test
  void capabilitiesReturnsConfiguredDefaults() {
    AiEndpoint endpoint =
        AiEndpoint.builder()
            .baseUrl("https://api.example.com/v1")
            .apiKey("test-key")
            .model("gpt-test")
            .build();
    AiCapabilities caps = AiCapabilities.defaults();
    OpenAiCompatibleClient client = new OpenAiCompatibleClient(endpoint, caps);

    assertTrue(client.capabilities().chatCompletions());
    assertTrue(client.capabilities().structuredOutput());
    assertFalse(client.capabilities().streaming());
  }

  @Test
  void sendThrowsUnsupportedCapabilityWhenStructuredOutputNotEnabled() {
    AiEndpoint endpoint =
        AiEndpoint.builder().baseUrl("https://api.example.com/v1").apiKey("key").model("m").build();
    AiCapabilities caps =
        AiCapabilities.builder().chatCompletions(true).structuredOutput(false).build();
    OpenAiCompatibleClient client = new OpenAiCompatibleClient(endpoint, caps);

    AiRequest request = AiRequest.builder().userMessage("hello").structuredOutput(true).build();

    AiException ex = assertThrows(AiException.class, () -> client.send(request));
    assertEquals(AiErrorCategory.UNSUPPORTED_CAPABILITY, ex.category());
  }

  @Test
  void endpointToStringRedactsApiKey() {
    AiEndpoint endpoint =
        AiEndpoint.builder()
            .baseUrl("https://api.example.com/v1")
            .apiKey("super-secret-key-12345")
            .model("gpt-4o")
            .build();

    String str = endpoint.toString();
    assertFalse(str.contains("super-secret-key-12345"), "API key must not appear in toString()");
    assertTrue(str.contains("[REDACTED]"));
    assertTrue(str.contains("gpt-4o"));
  }

  @Test
  void endpointEqualsIgnoresApiKey() {
    AiEndpoint a =
        AiEndpoint.builder().baseUrl("https://x.com/v1").apiKey("key-a").model("m1").build();
    AiEndpoint b =
        AiEndpoint.builder().baseUrl("https://x.com/v1").apiKey("key-b").model("m1").build();

    assertEquals(a, b, "Endpoints with same baseUrl+model but different keys should be equal");
  }
}
