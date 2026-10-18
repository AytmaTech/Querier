package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiCapabilitiesTest {

  @Test
  void defaults_chatCompletions_true() {
    assertTrue(AiCapabilities.defaults().chatCompletions());
  }

  @Test
  void defaults_structuredOutput_true() {
    assertTrue(AiCapabilities.defaults().structuredOutput());
  }

  @Test
  void defaults_toolCalling_false() {
    assertFalse(AiCapabilities.defaults().toolCalling());
  }

  @Test
  void defaults_streaming_false() {
    assertFalse(AiCapabilities.defaults().streaming());
  }

  @Test
  void builder_allFieldsStoredCorrectly() {
    AiCapabilities caps =
        AiCapabilities.builder()
            .chatCompletions(true)
            .structuredOutput(false)
            .toolCalling(true)
            .streaming(true)
            .build();

    assertTrue(caps.chatCompletions());
    assertFalse(caps.structuredOutput());
    assertTrue(caps.toolCalling());
    assertTrue(caps.streaming());
  }

  @Test
  void toString_containsAllFields() {
    String str = AiCapabilities.defaults().toString();

    assertTrue(str.contains("chatCompletions"));
    assertTrue(str.contains("structuredOutput"));
    assertTrue(str.contains("toolCalling"));
    assertTrue(str.contains("streaming"));
  }
}
