package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AiEndpointTest {

  private static AiEndpoint.Builder validBuilder() {
    return AiEndpoint.builder()
        .baseUrl("https://api.example.com/v1")
        .apiKey("secret-key")
        .model("gpt-4o");
  }

  @Test
  void build_setsAllFields() {
    AiEndpoint ep = validBuilder().build();

    assertEquals("https://api.example.com/v1", ep.baseUrl());
    assertEquals("secret-key", ep.apiKey());
    assertEquals("gpt-4o", ep.model());
  }

  @Test
  void build_defaultConnectTimeout_is10Seconds() {
    AiEndpoint ep = validBuilder().build();
    assertEquals(Duration.ofSeconds(10), ep.connectTimeout());
  }

  @Test
  void build_defaultRequestTimeout_is60Seconds() {
    AiEndpoint ep = validBuilder().build();
    assertEquals(Duration.ofSeconds(60), ep.requestTimeout());
  }

  @Test
  void build_customTimeouts_areStored() {
    AiEndpoint ep =
        validBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .requestTimeout(Duration.ofSeconds(30))
            .build();

    assertEquals(Duration.ofSeconds(5), ep.connectTimeout());
    assertEquals(Duration.ofSeconds(30), ep.requestTimeout());
  }

  @Test
  void build_missingBaseUrl_throwsNullPointer() {
    assertThrows(
        NullPointerException.class, () -> AiEndpoint.builder().apiKey("k").model("m").build());
  }

  @Test
  void build_missingApiKey_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> AiEndpoint.builder().baseUrl("https://x.com").model("m").build());
  }

  @Test
  void build_missingModel_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> AiEndpoint.builder().baseUrl("https://x.com").apiKey("k").build());
  }

  @Test
  void toString_redactsApiKey() {
    AiEndpoint ep = validBuilder().build();
    String str = ep.toString();

    assertFalse(str.contains("secret-key"));
    assertTrue(str.contains("[REDACTED]"));
  }

  @Test
  void toString_containsBaseUrlAndModel() {
    AiEndpoint ep = validBuilder().build();
    String str = ep.toString();

    assertTrue(str.contains("https://api.example.com/v1"));
    assertTrue(str.contains("gpt-4o"));
  }

  @Test
  void equals_sameBaseUrlAndModel_equalRegardlessOfApiKey() {
    AiEndpoint a =
        AiEndpoint.builder().baseUrl("https://x.com").apiKey("key-a").model("m1").build();
    AiEndpoint b =
        AiEndpoint.builder().baseUrl("https://x.com").apiKey("key-b").model("m1").build();

    assertEquals(a, b);
  }

  @Test
  void equals_differentModel_notEqual() {
    AiEndpoint a = validBuilder().model("gpt-4o").build();
    AiEndpoint b = validBuilder().model("gpt-3.5").build();

    assertNotEquals(a, b);
  }

  @Test
  void equals_differentBaseUrl_notEqual() {
    AiEndpoint a = AiEndpoint.builder().baseUrl("https://a.com").apiKey("k").model("m").build();
    AiEndpoint b = AiEndpoint.builder().baseUrl("https://b.com").apiKey("k").model("m").build();

    assertNotEquals(a, b);
  }

  @Test
  void hashCode_sameBaseUrlAndModel_sameHash() {
    AiEndpoint a =
        AiEndpoint.builder().baseUrl("https://x.com").apiKey("key-a").model("m1").build();
    AiEndpoint b =
        AiEndpoint.builder().baseUrl("https://x.com").apiKey("key-b").model("m1").build();

    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void header_addsCustomHeader() {
    AiEndpoint ep = validBuilder().header("X-Org", "my-org").build();
    Map<String, String> headers = ep.headers();

    assertEquals("my-org", headers.get("X-Org"));
  }

  @Test
  void fromEnvironment_builderChainWorks() {
    // fromEnvironment reads env vars; we override all required fields so no env dependency.
    AiEndpoint ep =
        AiEndpoint.fromEnvironment()
            .baseUrl("https://override.com")
            .apiKey("override-key")
            .model("override-model")
            .build();

    assertNotNull(ep);
    assertEquals("https://override.com", ep.baseUrl());
  }
}
