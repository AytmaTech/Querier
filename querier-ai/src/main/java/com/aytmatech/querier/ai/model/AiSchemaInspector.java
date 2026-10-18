package com.aytmatech.querier.ai.model;

import com.aytmatech.querier.ai.annotation.Semantic;
import com.aytmatech.querier.util.NameResolver;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Inspects entity classes to produce {@link AiSchemaContext} using the same annotation resolution
 * rules as the core {@link NameResolver}: Querier {@code @Table}/{@code @Column} first, JPA
 * fallback, then snake_case defaults.
 *
 * <p>Additionally reads {@link Semantic} annotations (on getter methods or their backing fields) to
 * populate {@link AiEntityField#description()} and {@link AiEntityField#synonyms()}, enriching the
 * AI prompt context with business-domain vocabulary.
 *
 * <p>No second schema system is introduced — all name resolution delegates to {@link NameResolver}.
 */
public final class AiSchemaInspector {

  private AiSchemaInspector() {}

  /**
   * Inspects the given entity classes and returns an {@link AiSchemaContext}.
   *
   * @param entityClasses one or more entity classes to expose to the AI
   * @return context containing resolved schemas for all supplied classes
   */
  public static AiSchemaContext inspect(Class<?>... entityClasses) {
    List<AiEntitySchema> schemas = new ArrayList<>();
    for (Class<?> cls : entityClasses) {
      schemas.add(inspectClass(cls));
    }
    return AiSchemaContext.of(schemas);
  }

  /**
   * Inspects a single entity class and returns an {@link AiEntitySchema} containing the resolved
   * table name, field names, types, and any semantic metadata.
   *
   * @param cls the entity class to inspect
   * @return the resolved schema for the given class, including table name, field names, types, and
   *     any semantic metadata
   */
  private static AiEntitySchema inspectClass(Class<?> cls) {
    String tableName = NameResolver.getTableName(cls);
    AiEntitySchema.Builder schemaBuilder =
        AiEntitySchema.builder().entityClass(cls).tableName(tableName);

    Semantic classSemantic = cls.getAnnotation(Semantic.class);
    if (classSemantic != null) {
      if (!classSemantic.description().isEmpty())
        schemaBuilder.description(classSemantic.description());
      for (String synonym : classSemantic.synonyms()) {
        if (synonym != null && !synonym.isBlank()) {
          schemaBuilder.synonym(synonym.trim());
        }
      }
    }

    for (Method method : NameResolver.collectGetters(cls)) {
      String columnName = NameResolver.getColumnName(cls, method.getName());
      String javaType = method.getReturnType().getSimpleName();

      AiEntityField.Builder fieldBuilder =
          AiEntityField.builder()
              .getterName(method.getName())
              .columnName(columnName)
              .javaType(javaType);

      Semantic semantic = method.getAnnotation(Semantic.class);
      if (semantic == null) {
        semantic = findFieldSemantic(cls, method.getName());
      }
      if (semantic != null) {
        if (!semantic.description().isEmpty()) {
          fieldBuilder.description(semantic.description());
        }
        for (String synonym : semantic.synonyms()) {
          if (synonym != null && !synonym.isBlank()) {
            fieldBuilder.synonym(synonym.trim());
          }
        }
      }

      schemaBuilder.field(fieldBuilder.build());
    }

    return schemaBuilder.build();
  }

  /**
   * Looks for a {@link Semantic} annotation on the field whose name is derived from the getter.
   * Handles both {@code getXxx} → {@code xxx} and {@code isXxx} → {@code xxx} conventions. Walks
   * the class hierarchy.
   *
   * @param cls the class to inspect
   * @param getterName the name of the getter method
   * @return the {@link Semantic} annotation if found, or {@code null} if not found
   */
  private static Semantic findFieldSemantic(Class<?> cls, String getterName) {
    String fieldName = NameResolver.deriveFieldName(getterName);
    if (fieldName == null) return null;

    Class<?> current = cls;
    while (current != null && current != Object.class) {
      try {
        Field field = current.getDeclaredField(fieldName);
        Semantic s = field.getAnnotation(Semantic.class);
        if (s != null) return s;
      } catch (NoSuchFieldException ignored) {
        // continue walking
      }
      current = current.getSuperclass();
    }
    return null;
  }
}
