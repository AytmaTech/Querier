package com.aytmatech.querier.ai.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * AI-facing schema for a single entity class.
 *
 * <p>Created by {@link AiSchemaInspector} from the entity's annotations and getter signatures.
 * Contains resolved table name, all inspectable fields, and optional business metadata.
 */
public final class AiEntitySchema {

  private final Class<?> entityClass;
  private final String tableName;
  private final List<AiEntityField> fields;
  private final String description;
  private final List<String> synonyms;

  private AiEntitySchema(Builder b) {
    this.entityClass = Objects.requireNonNull(b.entityClass, "entityClass");
    this.tableName = Objects.requireNonNull(b.tableName, "tableName");
    this.fields = Collections.unmodifiableList(new ArrayList<>(b.fields));
    this.description = b.description;
    this.synonyms = Collections.unmodifiableList(new ArrayList<>(b.synonyms));
  }

  /**
   * Creates a new builder for creating an {@link AiEntitySchema} instance.
   *
   * @return A new builder for creating an {@link AiEntitySchema} instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Entity class this schema was derived from.
   *
   * @return The entity class this schema was derived from.
   */
  public Class<?> entityClass() {
    return entityClass;
  }

  /**
   * Table name resolved from the entity's {@code @Table} annotation, JPA fallback, or snake_case
   * conversion.
   *
   * @return Resolved SQL table name (honours {@code @Table}, JPA fallback, snake_case).
   */
  public String tableName() {
    return tableName;
  }

  /**
   * Fields of this entity that can be inspected by the AI system.
   *
   * @return All inspectable fields of this entity.
   */
  public List<AiEntityField> fields() {
    return fields;
  }

  /**
   * Optional human-readable description of the entity's business purpose.
   *
   * @return Optional human-readable description of the entity's business purpose.
   */
  public String description() {
    return description;
  }

  /**
   * Optional list of synonyms for the entity, used to improve AI understanding of the entity's
   * business purpose.
   *
   * @return Optional list of synonyms for the entity, used to improve AI understanding of the
   *     entity's business purpose.
   */
  public List<String> synonyms() {
    return synonyms;
  }

  /**
   * Looks up a field by column name (case-insensitive).
   *
   * @param columnName the SQL column name
   * @return the matching field, or empty
   */
  public Optional<AiEntityField> fieldByColumn(String columnName) {
    return fields.stream().filter(f -> f.columnName().equalsIgnoreCase(columnName)).findFirst();
  }

  /**
   * Looks up a field by getter name (case-insensitive).
   *
   * @param getterName the Java getter method name, e.g. {@code getTotal}
   * @return the matching field, or empty
   */
  public Optional<AiEntityField> fieldByGetter(String getterName) {
    return fields.stream().filter(f -> f.getterName().equalsIgnoreCase(getterName)).findFirst();
  }

  /**
   * Generates a human-readable prompt block for this entity, suitable for injection into an AI
   * system prompt.
   *
   * @return Returns a human-readable prompt block for this entity, suitable for injection into an
   *     AI system prompt.
   */
  public String toPromptText() {
    StringBuilder sb = new StringBuilder();
    sb.append("Entity: ")
        .append(entityClass.getSimpleName())
        .append(" (table: ")
        .append(tableName)
        .append(')');
    if (description != null) {
      sb.append(" — ").append(description);
    }
    if (!synonyms.isEmpty()) {
      sb.append(" [synonyms: ").append(String.join(", ", synonyms)).append(']');
    }
    sb.append('\n');
    for (AiEntityField f : fields) {
      sb.append(f.toPromptText(tableName)).append('\n');
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "AiEntitySchema{entity=" + entityClass.getSimpleName() + ", table=" + tableName + '}';
  }

  /** Builder for creating an {@link AiEntitySchema} instance. */
  public static final class Builder {
    private Class<?> entityClass;
    private String tableName;
    private final List<AiEntityField> fields = new ArrayList<>();
    private String description;
    private final List<String> synonyms = new ArrayList<>();

    private Builder() {}

    /**
     * Sets the entity class this schema was derived from.
     *
     * @param entityClass the entity class this schema was derived from
     * @return this builder
     */
    public Builder entityClass(Class<?> entityClass) {
      this.entityClass = entityClass;
      return this;
    }

    /**
     * Sets the resolved SQL table name (honours {@code @Table}, JPA fallback, snake_case).
     *
     * @param tableName the resolved SQL table name (honours {@code @Table}, JPA fallback,
     *     snake_case)
     * @return this builder
     */
    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    /**
     * Sets the list of inspectable fields of this entity.
     *
     * @param field an inspectable field of this entity
     * @return this builder
     */
    public Builder field(AiEntityField field) {
      this.fields.add(field);
      return this;
    }

    /**
     * Sets an optional human-readable description of the entity's business purpose.
     *
     * @param description an optional human-readable description of the entity's business purpose
     * @return this builder
     */
    public Builder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * Adds an optional synonym for the entity, used to improve AI understanding of the entity's
     * business purpose.
     *
     * @param synonym an optional synonym for the entity, used to improve AI understanding of the
     *     entity's business purpose
     * @return this builder
     */
    public Builder synonym(String synonym) {
      this.synonyms.add(synonym);
      return this;
    }

    /**
     * Builds a new {@link AiEntitySchema} instance from the current builder state.
     *
     * @return a new {@link AiEntitySchema} instance
     */
    public AiEntitySchema build() {
      return new AiEntitySchema(this);
    }
  }
}
