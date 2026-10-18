package com.aytmatech.querier.ai.model;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.annotation.Table;
import org.junit.jupiter.api.Test;

class AiSchemaContextTest {

  @Table("products")
  private static class Product {
    public String getName() {
      return null;
    }

    public Long getId() {
      return null;
    }
  }

  @Table("categories")
  private static class Category {
    public String getLabel() {
      return null;
    }
  }

  private static AiSchemaContext twoEntityContext() {
    return AiSchemaInspector.inspect(Product.class, Category.class);
  }

  @Test
  void of_storesAllSchemas() {
    AiSchemaContext ctx = twoEntityContext();
    assertEquals(2, ctx.schemas().size());
  }

  @Test
  void findByClassName_caseInsensitive_returnsSchema() {
    AiSchemaContext ctx = twoEntityContext();
    assertTrue(ctx.findByClassName("product").isPresent());
    assertTrue(ctx.findByClassName("PRODUCT").isPresent());
    assertTrue(ctx.findByClassName("Product").isPresent());
  }

  @Test
  void findByClassName_unknown_returnsEmpty() {
    AiSchemaContext ctx = twoEntityContext();
    assertTrue(ctx.findByClassName("NonExistent").isEmpty());
  }

  @Test
  void findByTable_caseInsensitive_returnsSchema() {
    AiSchemaContext ctx = twoEntityContext();
    assertTrue(ctx.findByTable("products").isPresent());
    assertTrue(ctx.findByTable("PRODUCTS").isPresent());
  }

  @Test
  void findByTable_unknown_returnsEmpty() {
    AiSchemaContext ctx = twoEntityContext();
    assertTrue(ctx.findByTable("nonexistent").isEmpty());
  }

  @Test
  void toPromptText_containsEntityName() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(Product.class);
    assertTrue(ctx.toPromptText().contains("Product"));
  }

  @Test
  void toPromptText_containsTableName() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(Product.class);
    assertTrue(ctx.toPromptText().contains("products"));
  }

  @Test
  void toPromptText_multipleEntities_allIncluded() {
    AiSchemaContext ctx = twoEntityContext();
    String text = ctx.toPromptText();

    assertTrue(text.contains("Product"));
    assertTrue(text.contains("Category"));
  }

  @Test
  void schemas_areImmutable() {
    AiSchemaContext ctx = twoEntityContext();
    assertThrows(
        UnsupportedOperationException.class,
        () -> ctx.schemas().add(AiSchemaInspector.inspect(Product.class).schemas().get(0)));
  }
}
