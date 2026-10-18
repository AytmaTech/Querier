package com.aytmatech.querier.ai.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The complete set of entity schemas exposed to the AI for a single request.
 *
 * <p>Applications explicitly supply only the entity classes they want to expose — the AI cannot
 * reference tables or columns outside this context. This is the primary security boundary for
 * schema-scope restriction.
 *
 * <p>Build using {@link AiSchemaInspector}:
 *
 * <pre>{@code
 * AiSchemaContext ctx = AiSchemaInspector.inspect(Customer.class, Order.class);
 * }</pre>
 */
public final class AiSchemaContext {

  private final List<AiEntitySchema> schemas;

  private AiSchemaContext(List<AiEntitySchema> schemas) {
    this.schemas = Collections.unmodifiableList(new ArrayList<>(schemas));
  }

  /**
   * Creates a context containing the given schemas.
   *
   * @param schemas the entity schemas to include in this context
   * @return a new {@code AiSchemaContext} instance
   */
  public static AiSchemaContext of(List<AiEntitySchema> schemas) {
    return new AiSchemaContext(Objects.requireNonNull(schemas, "schemas"));
  }

  /**
   * All entity schemas in this context.
   *
   * @return an unmodifiable list of {@link AiEntitySchema} objects
   */
  public List<AiEntitySchema> schemas() {
    return schemas;
  }

  /**
   * Finds a schema by entity class name (simple name, case-insensitive).
   *
   * @param simpleName simple class name, e.g. {@code "Customer"}
   * @return the matching schema, or empty
   */
  public Optional<AiEntitySchema> findByClassName(String simpleName) {
    return schemas.stream()
        .filter(s -> s.entityClass().getSimpleName().equalsIgnoreCase(simpleName))
        .findFirst();
  }

  /**
   * Finds a schema by table name (case-insensitive).
   *
   * @param tableName SQL table name
   * @return the matching schema, or empty
   */
  public Optional<AiEntitySchema> findByTable(String tableName) {
    return schemas.stream().filter(s -> s.tableName().equalsIgnoreCase(tableName)).findFirst();
  }

  /**
   * Produces a multi-entity prompt block suitable for injection into an AI system prompt as
   * available schema context.
   *
   * @return a string describing all available entities and their fields, formatted for AI
   *     consumption
   */
  public String toPromptText() {
    StringBuilder sb = new StringBuilder("\n\nAvailable entities:\n\n");
    for (AiEntitySchema schema : schemas) {
      sb.append(schema.toPromptText()).append('\n');
    }
    return sb.toString();
  }
}
