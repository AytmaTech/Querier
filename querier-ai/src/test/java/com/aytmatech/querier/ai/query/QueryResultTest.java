package com.aytmatech.querier.ai.query;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.Select;
import org.junit.jupiter.api.Test;

class QueryResultTest {

  private static final Select STUB_SELECT = Select.rawQuery("SELECT 1");

  @Test
  void isValid_withQueryAndNoIssues_returnsTrue() {
    QueryResult result = QueryResult.builder().prompt("test").query(STUB_SELECT).build();

    assertTrue(result.isValid());
  }

  @Test
  void isValid_withIssue_returnsFalse() {
    QueryResult result =
        QueryResult.builder()
            .prompt("test")
            .query(STUB_SELECT)
            .validationIssue("something wrong")
            .build();

    assertFalse(result.isValid());
  }

  @Test
  void isValid_nullQuery_returnsFalse() {
    QueryResult result = QueryResult.builder().prompt("test").build();
    assertFalse(result.isValid());
  }

  @Test
  void query_whenValid_wrappedInOptional() {
    QueryResult result = QueryResult.builder().prompt("test").query(STUB_SELECT).build();
    assertTrue(result.query().isPresent());
    assertSame(STUB_SELECT, result.query().get());
  }

  @Test
  void query_whenInvalid_emptyOptional() {
    QueryResult result = QueryResult.builder().prompt("test").build();
    assertTrue(result.query().isEmpty());
  }

  @Test
  void validationIssues_areImmutable() {
    QueryResult result = QueryResult.builder().prompt("test").validationIssue("issue").build();

    assertThrows(
        UnsupportedOperationException.class, () -> result.validationIssues().add("another"));
  }

  @Test
  void interpretationNote_whenSet_wrappedInOptional() {
    QueryResult result =
        QueryResult.builder().prompt("test").interpretationNote("interpreted as X").build();

    assertTrue(result.interpretationNote().isPresent());
    assertEquals("interpreted as X", result.interpretationNote().get());
  }

  @Test
  void interpretationNote_whenNotSet_emptyOptional() {
    QueryResult result = QueryResult.builder().prompt("test").build();
    assertTrue(result.interpretationNote().isEmpty());
  }

  @Test
  void rawSql_whenSet_wrappedInOptional() {
    QueryResult result = QueryResult.builder().prompt("test").rawSql("SELECT 1").build();

    assertTrue(result.rawSql().isPresent());
    assertEquals("SELECT 1", result.rawSql().get());
  }

  @Test
  void rawSql_whenNotSet_emptyOptional() {
    QueryResult result = QueryResult.builder().prompt("test").build();
    assertTrue(result.rawSql().isEmpty());
  }

  @Test
  void build_missingPrompt_throwsNullPointer() {
    assertThrows(NullPointerException.class, () -> QueryResult.builder().build());
  }
}
