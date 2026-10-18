package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiRequestTest {

  @Test
  void build_systemMessage_addedWithCorrectRole() {
    AiRequest req = AiRequest.builder().systemMessage("You are helpful.").build();

    assertEquals(1, req.messages().size());
    assertEquals(AiRequest.ROLE_SYSTEM, req.messages().get(0).role());
    assertEquals("You are helpful.", req.messages().get(0).content());
  }

  @Test
  void build_userMessage_addedWithCorrectRole() {
    AiRequest req = AiRequest.builder().userMessage("Hello").build();

    assertEquals(AiRequest.ROLE_USER, req.messages().get(0).role());
  }

  @Test
  void build_assistantMessage_addedWithCorrectRole() {
    AiRequest req = AiRequest.builder().assistantMessage("Hi there").build();

    assertEquals(AiRequest.ROLE_ASSISTANT, req.messages().get(0).role());
  }

  @Test
  void build_multipleMessages_preserveOrder() {
    AiRequest req =
        AiRequest.builder()
            .systemMessage("sys")
            .userMessage("user")
            .assistantMessage("asst")
            .build();

    assertEquals(3, req.messages().size());
    assertEquals(AiRequest.ROLE_SYSTEM, req.messages().get(0).role());
    assertEquals(AiRequest.ROLE_USER, req.messages().get(1).role());
    assertEquals(AiRequest.ROLE_ASSISTANT, req.messages().get(2).role());
  }

  @Test
  void build_emptyMessages_throwsIllegalState() {
    assertThrows(IllegalStateException.class, () -> AiRequest.builder().build());
  }

  @Test
  void build_temperature_isStored() {
    AiRequest req = AiRequest.builder().userMessage("x").temperature(0.5).build();
    assertEquals(0.5, req.temperature());
  }

  @Test
  void build_maxTokens_isStored() {
    AiRequest req = AiRequest.builder().userMessage("x").maxTokens(1024).build();
    assertEquals(1024, req.maxTokens());
  }

  @Test
  void build_structuredOutput_defaultFalse() {
    AiRequest req = AiRequest.builder().userMessage("x").build();
    assertFalse(req.structuredOutput());
  }

  @Test
  void build_structuredOutput_canBeSetTrue() {
    AiRequest req = AiRequest.builder().userMessage("x").structuredOutput(true).build();
    assertTrue(req.structuredOutput());
  }

  @Test
  void build_jsonSchema_isStored() {
    AiRequest req =
        AiRequest.builder().userMessage("x").jsonSchema("{\"type\":\"object\"}").build();
    assertEquals("{\"type\":\"object\"}", req.jsonSchema());
  }

  @Test
  void build_modelOverride_isStored() {
    AiRequest req = AiRequest.builder().userMessage("x").model("gpt-3.5").build();
    assertEquals("gpt-3.5", req.model());
  }

  @Test
  void message_record_nullRole_throwsNullPointer() {
    assertThrows(NullPointerException.class, () -> new AiRequest.Message(null, "content"));
  }

  @Test
  void message_record_nullContent_throwsNullPointer() {
    assertThrows(NullPointerException.class, () -> new AiRequest.Message("user", null));
  }
}
