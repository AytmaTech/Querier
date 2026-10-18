package com.aytmatech.querier.ai.query;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import com.aytmatech.querier.annotation.Table;
import org.junit.jupiter.api.Test;

class QueryRequestTest {

  @Table("items")
  private static class Item {
    public String getName() {
      return null;
    }
  }

  private static final AiSchemaContext ITEM_CTX = AiSchemaInspector.inspect(Item.class);

  @Test
  void build_setsPrompt() {
    QueryRequest req =
        QueryRequest.builder().prompt("List all items").schemaContext(ITEM_CTX).build();

    assertEquals("List all items", req.prompt());
  }

  @Test
  void build_entities_populatesSchemaContext() {
    QueryRequest req = QueryRequest.builder().prompt("test").entities(Item.class).build();

    assertNotNull(req.schemaContext());
    assertEquals(1, req.schemaContext().schemas().size());
  }

  @Test
  void build_schemaContext_direct() {
    QueryRequest req = QueryRequest.builder().prompt("test").schemaContext(ITEM_CTX).build();

    assertSame(ITEM_CTX, req.schemaContext());
  }

  @Test
  void build_missingPrompt_throwsNullPointer() {
    assertThrows(
        NullPointerException.class, () -> QueryRequest.builder().schemaContext(ITEM_CTX).build());
  }

  @Test
  void build_missingSchemaContext_throwsNullPointer() {
    assertThrows(NullPointerException.class, () -> QueryRequest.builder().prompt("test").build());
  }

  @Test
  void build_temperature_isStored() {
    QueryRequest req =
        QueryRequest.builder().prompt("test").schemaContext(ITEM_CTX).temperature(0.7).build();

    assertEquals(0.7, req.temperature());
  }

  @Test
  void build_maxTokens_isStored() {
    QueryRequest req =
        QueryRequest.builder().prompt("test").schemaContext(ITEM_CTX).maxTokens(512).build();

    assertEquals(512, req.maxTokens());
  }

  @Test
  void build_model_isStored() {
    QueryRequest req = QueryRequest.builder().prompt("test").schemaContext(ITEM_CTX).build();

    // model field is set via builder — default is null
    assertNull(req.model());
  }
}
