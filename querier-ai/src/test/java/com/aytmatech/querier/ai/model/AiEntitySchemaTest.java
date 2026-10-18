package com.aytmatech.querier.ai.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AiEntitySchemaTest {

  private static AiEntityField sampleField() {
    return AiEntityField.builder()
        .getterName("getName")
        .columnName("name")
        .javaType("String")
        .build();
  }

  private static AiEntitySchema.Builder validBuilder() {
    return AiEntitySchema.builder().entityClass(String.class).tableName("strings");
  }

  @Test
  void build_setsEntityClass() {
    AiEntitySchema schema = validBuilder().build();
    assertEquals(String.class, schema.entityClass());
  }

  @Test
  void build_setsTableName() {
    AiEntitySchema schema = validBuilder().build();
    assertEquals("strings", schema.tableName());
  }

  @Test
  void build_description_isStored() {
    AiEntitySchema schema = validBuilder().description("A test entity").build();
    assertEquals("A test entity", schema.description());
  }

  @Test
  void build_synonyms_areStored() {
    AiEntitySchema schema = validBuilder().synonym("alias1").synonym("alias2").build();
    assertTrue(schema.synonyms().contains("alias1"));
    assertTrue(schema.synonyms().contains("alias2"));
  }

  @Test
  void fieldByColumn_caseInsensitive_returnsField() {
    AiEntitySchema schema = validBuilder().field(sampleField()).build();

    assertTrue(schema.fieldByColumn("name").isPresent());
    assertTrue(schema.fieldByColumn("NAME").isPresent());
    assertTrue(schema.fieldByColumn("Name").isPresent());
  }

  @Test
  void fieldByColumn_unknown_returnsEmpty() {
    AiEntitySchema schema = validBuilder().field(sampleField()).build();
    assertTrue(schema.fieldByColumn("nonexistent").isEmpty());
  }

  @Test
  void fieldByGetter_caseInsensitive_returnsField() {
    AiEntitySchema schema = validBuilder().field(sampleField()).build();

    assertTrue(schema.fieldByGetter("getName").isPresent());
    assertTrue(schema.fieldByGetter("GETNAME").isPresent());
  }

  @Test
  void toPromptText_containsEntitySimpleName() {
    AiEntitySchema schema = validBuilder().build();
    assertTrue(schema.toPromptText().contains("String"));
  }

  @Test
  void toPromptText_containsTableName() {
    AiEntitySchema schema = validBuilder().build();
    assertTrue(schema.toPromptText().contains("strings"));
  }

  @Test
  void toPromptText_containsColumnName() {
    AiEntitySchema schema = validBuilder().field(sampleField()).build();
    assertTrue(schema.toPromptText().contains("name"));
  }

  @Test
  void toPromptText_withDescription_containsDescription() {
    AiEntitySchema schema = validBuilder().description("My entity description").build();
    assertTrue(schema.toPromptText().contains("My entity description"));
  }

  @Test
  void toPromptText_withSynonyms_containsSynonyms() {
    AiEntitySchema schema = validBuilder().synonym("myalias").build();
    assertTrue(schema.toPromptText().contains("myalias"));
  }

  @Test
  void fields_areImmutable() {
    AiEntitySchema schema = validBuilder().field(sampleField()).build();
    assertThrows(UnsupportedOperationException.class, () -> schema.fields().add(sampleField()));
  }
}
