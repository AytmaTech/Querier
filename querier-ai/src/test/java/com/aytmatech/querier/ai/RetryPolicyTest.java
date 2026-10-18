package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  @Test
  void disabled_isNotEnabled() {
    RetryPolicy policy = RetryPolicy.disabled();
    assertFalse(policy.isEnabled());
    assertEquals(0, policy.maxAttempts());
  }

  @Test
  void ofMaxAttempts_isEnabledWithCorrectCount() {
    RetryPolicy policy = RetryPolicy.ofMaxAttempts(3);
    assertTrue(policy.isEnabled());
    assertEquals(3, policy.maxAttempts());
  }

  @Test
  void ofMaxAttempts_rejectsZero() {
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.ofMaxAttempts(0));
  }

  @Test
  void ofMaxAttempts_rejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.ofMaxAttempts(-1));
  }

  @Test
  void builder_defaults() {
    RetryPolicy policy = RetryPolicy.builder().build();
    assertTrue(policy.isEnabled());
    assertEquals(1, policy.maxAttempts());
  }

  @Test
  void builder_customMaxAttempts() {
    RetryPolicy policy = RetryPolicy.builder().maxAttempts(5).build();
    assertEquals(5, policy.maxAttempts());
  }

  @Test
  void builder_disabled() {
    RetryPolicy policy = RetryPolicy.builder().enabled(false).build();
    assertFalse(policy.isEnabled());
  }

  @Test
  void equality() {
    assertEquals(RetryPolicy.ofMaxAttempts(2), RetryPolicy.ofMaxAttempts(2));
    assertNotEquals(RetryPolicy.ofMaxAttempts(2), RetryPolicy.ofMaxAttempts(3));
    assertEquals(RetryPolicy.disabled(), RetryPolicy.disabled());
  }

  @Test
  void toString_disabled() {
    assertTrue(RetryPolicy.disabled().toString().contains("enabled=false"));
  }

  @Test
  void toString_enabled() {
    String s = RetryPolicy.ofMaxAttempts(2).toString();
    assertTrue(s.contains("enabled=true"));
    assertTrue(s.contains("maxAttempts=2"));
  }
}
