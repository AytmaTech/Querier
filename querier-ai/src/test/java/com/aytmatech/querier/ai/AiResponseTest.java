package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiResponseTest {

  @Test
  void build_text_isStored() {
    AiResponse resp = AiResponse.builder().text("hello").build();
    assertEquals("hello", resp.text());
  }

  @Test
  void build_structuredContent_isStored() {
    AiResponse resp = AiResponse.builder().structuredContent("{\"key\":\"val\"}").build();
    assertEquals("{\"key\":\"val\"}", resp.structuredContent());
  }

  @Test
  void build_finishReason_wrappedInOptional() {
    AiResponse resp = AiResponse.builder().finishReason("stop").build();
    assertTrue(resp.finishReason().isPresent());
    assertEquals("stop", resp.finishReason().get());
  }

  @Test
  void build_inputTokens_wrappedInOptional() {
    AiResponse resp = AiResponse.builder().inputTokens(100).build();
    assertTrue(resp.inputTokens().isPresent());
    assertEquals(100, resp.inputTokens().get());
  }

  @Test
  void build_outputTokens_wrappedInOptional() {
    AiResponse resp = AiResponse.builder().outputTokens(50).build();
    assertTrue(resp.outputTokens().isPresent());
    assertEquals(50, resp.outputTokens().get());
  }

  @Test
  void build_missingOptionalFields_areEmpty() {
    AiResponse resp = AiResponse.builder().text("hi").build();

    assertTrue(resp.finishReason().isEmpty());
    assertTrue(resp.inputTokens().isEmpty());
    assertTrue(resp.outputTokens().isEmpty());
  }
}
