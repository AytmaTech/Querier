package com.aytmatech.querier.ai.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiEntityFieldTest {

  private static AiEntityField.Builder validBuilder() {
    return AiEntityField.builder()
        .getterName("getTotal")
        .columnName("total")
        .javaType("BigDecimal");
  }

  @Test
  void build_setsGetterName() {
    assertEquals("getTotal", validBuilder().build().getterName());
  }

  @Test
  void build_setsColumnName() {
    assertEquals("total", validBuilder().build().columnName());
  }

  @Test
  void build_setsJavaType() {
    assertEquals("BigDecimal", validBuilder().build().javaType());
  }

  @Test
  void build_description_isStored() {
    AiEntityField field = validBuilder().description("The order total").build();
    assertEquals("The order total", field.description());
  }

  @Test
  void build_synonyms_areStored() {
    AiEntityField field = validBuilder().synonym("revenue").synonym("sales").build();
    assertTrue(field.synonyms().contains("revenue"));
    assertTrue(field.synonyms().contains("sales"));
  }

  @Test
  void build_missingGetterName_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> AiEntityField.builder().columnName("col").javaType("String").build());
  }

  @Test
  void build_missingColumnName_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> AiEntityField.builder().getterName("get").javaType("String").build());
  }

  @Test
  void build_missingJavaType_throwsNullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> AiEntityField.builder().getterName("get").columnName("col").build());
  }

  @Test
  void toPromptText_containsTableAndColumn() {
    AiEntityField field = validBuilder().build();
    String text = field.toPromptText("orders");

    assertTrue(text.contains("orders"));
    assertTrue(text.contains("total"));
  }

  @Test
  void toPromptText_withDescription_containsDescription() {
    AiEntityField field = validBuilder().description("Grand total of the order").build();
    assertTrue(field.toPromptText("orders").contains("Grand total of the order"));
  }

  @Test
  void toPromptText_withSynonyms_containsSynonyms() {
    AiEntityField field = validBuilder().synonym("revenue").build();
    assertTrue(field.toPromptText("orders").contains("revenue"));
  }

  @Test
  void synonyms_areImmutable() {
    AiEntityField field = validBuilder().synonym("x").build();
    assertThrows(UnsupportedOperationException.class, () -> field.synonyms().add("y"));
  }
}
