package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiSqlResponseTest {

  @Test
  void ofSql_sqlPresent_notAmbiguous() {
    AiSqlResponse resp = AiSqlResponse.ofSql("SELECT 1");

    assertTrue(resp.sql().isPresent());
    assertEquals("SELECT 1", resp.sql().get());
    assertFalse(resp.isAmbiguous());
  }

  @Test
  void ofSql_ambiguityNote_isEmpty() {
    AiSqlResponse resp = AiSqlResponse.ofSql("SELECT 1");
    assertTrue(resp.ambiguityNote().isEmpty());
  }

  @Test
  void ambiguous_sqlEmpty_isAmbiguous() {
    AiSqlResponse resp = AiSqlResponse.ambiguous("too vague");
    assertTrue(resp.isAmbiguous());
    assertTrue(resp.sql().isEmpty());
  }

  @Test
  void ambiguous_notePresent() {
    AiSqlResponse resp = AiSqlResponse.ambiguous("could mean two things");
    assertTrue(resp.ambiguityNote().isPresent());
    assertEquals("could mean two things", resp.ambiguityNote().get());
  }
}
