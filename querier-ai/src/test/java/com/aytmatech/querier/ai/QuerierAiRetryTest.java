package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.error.AiErrorCategory;
import com.aytmatech.querier.ai.error.AiException;
import com.aytmatech.querier.ai.query.QueryRequest;
import com.aytmatech.querier.ai.query.QueryResult;
import com.aytmatech.querier.ai.retry.RetryPolicy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link QuerierAi} respects the {@link RetryPolicy} contract: - without a policy no
 * retry occurs - with a policy the client is called again with validation-issue context - a
 * corrected plan on the second attempt produces a valid result
 *
 * <p>Uses a simple stub {@link AiClient} to avoid network calls.
 */
class QuerierAiRetryTest {

  // ---- minimal entity for schema context ----------------------------------

  static class Customer {
    private String name;
    private String email;

    public String getName() {
      return name;
    }

    public String getEmail() {
      return email;
    }
  }

  // ---- stub AiClient ------------------------------------------------------

  /**
   * Stub that returns pre-configured responses in order. Tracks how many times {@link #send} was
   * called and what messages were in each request.
   */
  static class StubAiClient implements AiClient {

    private final List<String> responses;
    private final List<AiRequest> receivedRequests = new ArrayList<>();
    private int callIndex = 0;

    StubAiClient(String... responses) {
      this.responses = List.of(responses);
    }

    @Override
    public AiCapabilities capabilities() {
      return AiCapabilities.defaults();
    }

    @Override
    public AiResponse send(AiRequest request) throws AiException {
      receivedRequests.add(request);
      if (callIndex >= responses.size()) {
        throw new AiException(AiErrorCategory.PROVIDER_ERROR, "No more stub responses");
      }
      String json = responses.get(callIndex++);
      return AiResponse.builder().text(json).structuredContent(json).build();
    }

    int callCount() {
      return callIndex;
    }

    AiRequest requestAt(int index) {
      return receivedRequests.get(index);
    }
  }

  // ---- helpers ------------------------------------------------------------

  /** A QueryPlan JSON that references an entity unknown to our schema (will fail validation). */
  private static final String INVALID_PLAN_JSON =
      """
         {
         "sql": "SELECT c.name, SUM(o.total) AS revenue FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY revenue DESC LIMIT 10",
         "ambiguous": false,
         "ambiguityNote": null
         }
            """;

  /** A QueryPlan JSON that is valid against the Customer schema. */
  private static final String VALID_PLAN_JSON =
      """
         {
         "sql": "SELECT c.name FROM customer c GROUP BY c.name ORDER BY revenue DESC LIMIT 10",
         "ambiguous": false,
         "ambiguityNote": null
         }
            """;

  private QueryRequest customerRequest() {
    return QueryRequest.builder()
        .prompt("List all customer names")
        .entities(Customer.class)
        .build();
  }

  // ---- tests --------------------------------------------------------------

  @Test
  void noRetryPolicy_singleCallOnInvalidPlan() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON);
    QuerierAi ai = QuerierAi.builder().client(stub).build(); // no retry policy

    QueryResult result = ai.query(customerRequest());

    assertEquals(1, stub.callCount(), "Should call the model exactly once");
    assertFalse(result.isValid());
    assertFalse(result.validationIssues().isEmpty());
  }

  @Test
  void retryDisabled_singleCallOnInvalidPlan() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON);
    QuerierAi ai = QuerierAi.builder().client(stub).retryPolicy(RetryPolicy.disabled()).build();

    QueryResult result = ai.query(customerRequest());

    assertEquals(1, stub.callCount());
    assertFalse(result.isValid());
  }

  @Test
  void retryEnabled_secondCallSucceeds_resultIsValid() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON, VALID_PLAN_JSON);
    QuerierAi ai =
        QuerierAi.builder().client(stub).retryPolicy(RetryPolicy.ofMaxAttempts(1)).build();

    QueryResult result = ai.query(customerRequest());

    assertEquals(2, stub.callCount(), "Should make an initial call then one retry");
    assertTrue(result.isValid(), "After retry with valid plan result should be valid");
  }

  @Test
  void retryEnabled_correctionRequestContainsValidationIssues() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON, VALID_PLAN_JSON);
    QuerierAi ai =
        QuerierAi.builder().client(stub).retryPolicy(RetryPolicy.ofMaxAttempts(1)).build();

    ai.query(customerRequest());

    // The second (correction) request must mention the validation issue
    AiRequest correctionRequest = stub.requestAt(1);
    boolean issuesMentioned =
        correctionRequest.messages().stream()
            .anyMatch(
                m ->
                    m.content().contains("UnknownEntity")
                        || m.content().contains("unknown")
                        || m.content().contains("Validation issues"));
    assertTrue(issuesMentioned, "Correction request should contain the original validation issues");
  }

  @Test
  void retryEnabled_bothAttemptsFail_returnsInvalidResult() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON, INVALID_PLAN_JSON);
    QuerierAi ai =
        QuerierAi.builder().client(stub).retryPolicy(RetryPolicy.ofMaxAttempts(1)).build();

    QueryResult result = ai.query(customerRequest());

    assertEquals(2, stub.callCount());
    assertFalse(result.isValid());
    assertFalse(result.validationIssues().isEmpty());
  }

  @Test
  void retryEnabled_maxAttempts2_retriesTwiceIfNeeded() {
    StubAiClient stub = new StubAiClient(INVALID_PLAN_JSON, INVALID_PLAN_JSON, VALID_PLAN_JSON);
    QuerierAi ai =
        QuerierAi.builder().client(stub).retryPolicy(RetryPolicy.ofMaxAttempts(2)).build();

    QueryResult result = ai.query(customerRequest());

    assertEquals(3, stub.callCount(), "1 initial + 2 retries");
    assertTrue(result.isValid());
  }
}
