package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.error.AiErrorCategory;
import com.aytmatech.querier.ai.error.AiException;
import org.junit.jupiter.api.Test;

class AiExceptionTest {

  @Test
  void constructor_storesCategory() {
    AiException ex = new AiException(AiErrorCategory.TIMEOUT, "timed out");
    assertEquals(AiErrorCategory.TIMEOUT, ex.category());
  }

  @Test
  void constructor_storesMessage() {
    AiException ex = new AiException(AiErrorCategory.CONFIGURATION, "bad config");
    assertEquals("bad config", ex.getMessage());
  }

  @Test
  void constructor_withCause_storesCause() {
    RuntimeException cause = new RuntimeException("root cause");
    AiException ex = new AiException(AiErrorCategory.ENDPOINT_UNAVAILABLE, "unreachable", cause);

    assertSame(cause, ex.getCause());
  }

  @Test
  void constructor_nullCategory_throwsNullPointer() {
    assertThrows(NullPointerException.class, () -> new AiException(null, "msg"));
  }

  @Test
  void toString_containsCategoryAndMessage() {
    AiException ex = new AiException(AiErrorCategory.RATE_LIMIT, "too many requests");
    String str = ex.toString();

    assertTrue(str.contains("RATE_LIMIT"));
    assertTrue(str.contains("too many requests"));
  }
}
