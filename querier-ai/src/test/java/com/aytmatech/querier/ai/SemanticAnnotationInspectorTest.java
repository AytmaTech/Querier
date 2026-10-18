package com.aytmatech.querier.ai;

import static org.junit.jupiter.api.Assertions.*;

import com.aytmatech.querier.ai.annotation.Semantic;
import com.aytmatech.querier.ai.model.AiEntityField;
import com.aytmatech.querier.ai.model.AiEntitySchema;
import com.aytmatech.querier.ai.model.AiSchemaContext;
import com.aytmatech.querier.ai.model.AiSchemaInspector;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link AiSchemaInspector} reads {@link Semantic} annotations from getter methods
 * and from backing fields.
 */
class SemanticAnnotationInspectorTest {

  // ---- fixture entities ---------------------------------------------------

  static class OrderWithGetterAnnotation {
    private BigDecimal total;
    private String status;

    @Semantic(
        description = "Total monetary value of the order",
        synonyms = {"revenue", "sales value", "order value"})
    public BigDecimal getTotal() {
      return total;
    }

    public String getStatus() {
      return status;
    }
  }

  static class OrderWithFieldAnnotation {
    @Semantic(
        description = "Current lifecycle status of the order",
        synonyms = {"state", "order state"})
    private String status;

    private BigDecimal total;

    public BigDecimal getTotal() {
      return total;
    }

    public String getStatus() {
      return status;
    }
  }

  static class OrderWithNoAnnotation {
    private BigDecimal total;

    public BigDecimal getTotal() {
      return total;
    }
  }

  // ---- tests --------------------------------------------------------------

  @Test
  void getterAnnotation_populatesDescriptionAndSynonyms() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(OrderWithGetterAnnotation.class);
    AiEntitySchema schema = ctx.schemas().get(0);

    Optional<AiEntityField> field = schema.fieldByColumn("total");
    assertTrue(field.isPresent(), "Field 'total' should be present");

    AiEntityField f = field.get();
    assertEquals("Total monetary value of the order", f.description());
    assertTrue(f.synonyms().contains("revenue"));
    assertTrue(f.synonyms().contains("sales value"));
    assertTrue(f.synonyms().contains("order value"));
    assertEquals(3, f.synonyms().size());
  }

  @Test
  void fieldAnnotation_populatesDescriptionAndSynonyms() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(OrderWithFieldAnnotation.class);
    AiEntitySchema schema = ctx.schemas().get(0);

    Optional<AiEntityField> field = schema.fieldByColumn("status");
    assertTrue(field.isPresent(), "Field 'status' should be present");

    AiEntityField f = field.get();
    assertEquals("Current lifecycle status of the order", f.description());
    assertTrue(f.synonyms().contains("state"));
    assertTrue(f.synonyms().contains("order state"));
    assertEquals(2, f.synonyms().size());
  }

  @Test
  void unannotatedField_hasNullDescriptionAndEmptySynonyms() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(OrderWithNoAnnotation.class);
    AiEntitySchema schema = ctx.schemas().get(0);

    AiEntityField f = schema.fieldByColumn("total").orElseThrow();
    assertNull(f.description());
    assertTrue(f.synonyms().isEmpty());
  }

  @Test
  void unannotatedFieldInAnnotatedEntity_hasNullDescriptionAndEmptySynonyms() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(OrderWithGetterAnnotation.class);
    AiEntitySchema schema = ctx.schemas().get(0);

    AiEntityField f = schema.fieldByColumn("status").orElseThrow();
    assertNull(f.description());
    assertTrue(f.synonyms().isEmpty());
  }

  @Test
  void promptText_includesSemanticData() {
    AiSchemaContext ctx = AiSchemaInspector.inspect(OrderWithGetterAnnotation.class);
    String promptText = ctx.toPromptText();

    assertTrue(promptText.contains("Total monetary value of the order"));
    assertTrue(promptText.contains("revenue"));
    assertTrue(promptText.contains("sales value"));
  }

  @Test
  void multipleEntities_allInspectedCorrectly() {
    AiSchemaContext ctx =
        AiSchemaInspector.inspect(OrderWithGetterAnnotation.class, OrderWithFieldAnnotation.class);
    assertEquals(2, ctx.schemas().size());
  }
}
