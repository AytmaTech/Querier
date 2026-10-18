package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.error.AiErrorCategory;
import com.aytmatech.querier.ai.error.AiException;
import com.aytmatech.querier.ai.query.QueryRequest;
import com.aytmatech.querier.ai.query.QueryResult;
import com.aytmatech.querier.annotation.Table;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link QuerierAi#query(QueryRequest)} complementing {@link QuerierAiRetryTest}. */
class QuerierAiQueryTest {

  // ---- fixture entity -----------------------------------------------------

  @Table("customers")
  static class Customer {
    public String getName() {
      return null;
    }
  }

  // ---- stub AiClient ------------------------------------------------------

  static class StubAiClient implements AiClient {

    private final List<String> responses;
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
      if (callIndex >= responses.size()) {
        throw new AiException(AiErrorCategory.PROVIDER_ERROR, "No more stub responses");
      }
      String json = responses.get(callIndex++);
      return AiResponse.builder().text(json).structuredContent(json).build();
    }

    int callCount() {
      return callIndex;
    }
  }

  // ---- fixture JSON responses ---------------------------------------------

  private static final String VALID_SQL_JSON =
      """
            {"sql": "SELECT c.name FROM customers c", "ambiguous": false, "ambiguityNote": null}
            """;

  private static final String AMBIGUOUS_JSON =
      """
            {"sql": null, "ambiguous": true, "ambiguityNote": "Request was too vague"}
            """;

  private static final String MALFORMED_JSON = "not-json{{";

  private static final String EMPTY_SQL_JSON =
      """
            {"sql": "", "ambiguous": false, "ambiguityNote": null}
            """;

  private static final String UNKNOWN_TABLE_SQL_JSON =
      """
            {"sql": "SELECT o.id FROM orders o", "ambiguous": false, "ambiguityNote": null}
            """;

  // ---- helpers ------------------------------------------------------------

  private QueryRequest customerRequest() {
    return QueryRequest.builder().prompt("List all customers").entities(Customer.class).build();
  }

  private QuerierAi aiWith(String... responses) {
    return QuerierAi.builder().client(new StubAiClient(responses)).build();
  }

  // ---- tests --------------------------------------------------------------

  @Test
  void query_validSql_returnsValidResult() {
    QueryResult result = aiWith(VALID_SQL_JSON).query(customerRequest());
    assertTrue(
        result.isValid(), "Expected valid result but got issues: " + result.validationIssues());
  }

  @Test
  void query_validSql_rawSqlIsSet() {
    QueryResult result = aiWith(VALID_SQL_JSON).query(customerRequest());
    assertTrue(result.rawSql().isPresent());
    assertTrue(result.rawSql().get().contains("customers"));
  }

  @Test
  void query_ambiguousResponse_returnsInvalidResultWithInterpretationNote() {
    QueryResult result = aiWith(AMBIGUOUS_JSON).query(customerRequest());

    assertFalse(result.isValid());
    assertTrue(result.interpretationNote().isPresent());
    assertTrue(result.interpretationNote().get().contains("too vague"));
  }

  @Test
  void query_malformedJson_returnsInvalidResult() {
    QueryResult result = aiWith(MALFORMED_JSON).query(customerRequest());
    assertFalse(result.isValid());
    assertFalse(result.validationIssues().isEmpty());
  }

  @Test
  void query_emptySql_returnsInvalidResult() {
    QueryResult result = aiWith(EMPTY_SQL_JSON).query(customerRequest());
    assertFalse(result.isValid());
  }

  @Test
  void query_unknownTableInSql_returnsInvalidResult() {
    QueryResult result = aiWith(UNKNOWN_TABLE_SQL_JSON).query(customerRequest());
    assertFalse(result.isValid());
    assertTrue(result.validationIssues().stream().anyMatch(i -> i.contains("orders")));
  }

  @Test
  void query_noRetryOnValidResult_singleCall() {
    StubAiClient stub = new StubAiClient(VALID_SQL_JSON);
    QuerierAi ai =
        QuerierAi.builder()
            .client(stub)
            .retryPolicy(com.aytmatech.querier.ai.retry.RetryPolicy.ofMaxAttempts(3))
            .build();

    ai.query(customerRequest());

    assertEquals(1, stub.callCount(), "Valid result should not trigger any retries");
  }
}
