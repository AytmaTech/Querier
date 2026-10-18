package com.aytmatech.querier.ai.sql;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import com.aytmatech.querier.annotation.Table;
import java.util.List;
import org.junit.jupiter.api.Test;

class SqlSchemaValidatorTest {

  @Table("customers")
  private static class Customer {
    public String getName() {
      return null;
    }

    public String getEmail() {
      return null;
    }
  }

  private static final AiSchemaContext CTX = AiSchemaInspector.inspect(Customer.class);

  @Test
  void validate_validSelectStar_returnsNoIssues() {
    List<String> issues = SqlSchemaValidator.validate("SELECT * FROM customers", CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_validSelectWithColumn_returnsNoIssues() {
    List<String> issues = SqlSchemaValidator.validate("SELECT customers.name FROM customers", CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_validSelectWithAlias_returnsNoIssues() {
    List<String> issues = SqlSchemaValidator.validate("SELECT c.name FROM customers c", CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_selectWithJoin_validColumns_returnsNoIssues() {
    List<String> issues =
        SqlSchemaValidator.validate("SELECT c.name, c.email FROM customers c", CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_dmlStatement_returnsIssue() {
    List<String> issues = SqlSchemaValidator.validate("UPDATE customers SET name = 'x'", CTX);
    assertFalse(issues.isEmpty());
  }

  @Test
  void validate_unknownTable_returnsIssue() {
    List<String> issues = SqlSchemaValidator.validate("SELECT o.id FROM orders o", CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("orders")));
  }

  @Test
  void validate_unknownColumn_returnsIssue() {
    List<String> issues = SqlSchemaValidator.validate("SELECT c.nonexistent FROM customers c", CTX);
    assertFalse(issues.isEmpty());
    assertTrue(issues.stream().anyMatch(i -> i.contains("nonexistent")));
  }

  @Test
  void validate_blankSql_returnsIssue() {
    List<String> issues = SqlSchemaValidator.validate("   ", CTX);
    assertFalse(issues.isEmpty());
  }

  @Test
  void validate_nullSql_returnsIssue() {
    List<String> issues = SqlSchemaValidator.validate(null, CTX);
    assertFalse(issues.isEmpty());
  }

  @Test
  void validate_withClause_valid_returnsNoIssues() {
    String cte = "WITH cte AS (SELECT name FROM customers) SELECT cte.name FROM cte";
    List<String> issues = SqlSchemaValidator.validate(cte, CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_starColumn_ignored_returnsNoIssues() {
    List<String> issues = SqlSchemaValidator.validate("SELECT c.* FROM customers c", CTX);
    assertTrue(issues.isEmpty());
  }

  @Test
  void validate_sqlKeywordPrefix_notTreatedAsAlias() {
    List<String> issues =
        SqlSchemaValidator.validate("SELECT c.name FROM customers c WHERE c.name IS NOT NULL", CTX);
    assertTrue(issues.isEmpty());
  }
}
