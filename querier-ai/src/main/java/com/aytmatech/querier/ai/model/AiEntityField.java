package com.aytmatech.querier.ai.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Describes a single field (getter/column) of a Querier entity for AI context.
 *
 * <p>Instances are created by {@link AiSchemaInspector} from the entity's annotations and getter
 * signatures. Optional {@link #synonyms()} and {@link #description()} allow business semantics to
 * be added without modifying the entity class.
 */
public final class AiEntityField {

  private final String getterName;
  private final String columnName;
  private final String javaType;
  private final List<String> synonyms;
  private final String description;

  private AiEntityField(Builder b) {
    this.getterName = Objects.requireNonNull(b.getterName, "getterName");
    this.columnName = Objects.requireNonNull(b.columnName, "columnName");
    this.javaType = Objects.requireNonNull(b.javaType, "javaType");
    this.synonyms = Collections.unmodifiableList(new ArrayList<>(b.synonyms));
    this.description = b.description;
  }

  /**
   * Creates a new builder for {@link AiEntityField}.
   *
   * @return A new builder for {@link AiEntityField}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Getter method name, e.g. {@code getTotal}.
   *
   * @return Getter method name, e.g. {@code getTotal}.
   */
  public String getterName() {
    return getterName;
  }

  /**
   * Resolved SQL column name, e.g. {@code total}.
   *
   * @return Resolved SQL column name, e.g. {@code total}.
   */
  public String columnName() {
    return columnName;
  }

  /**
   * Simple Java type name, e.g. {@code BigDecimal}, {@code String}, {@code LocalDate}.
   *
   * @return Simple Java type name, e.g. {@code BigDecimal}, {@code String}, {@code LocalDate}.
   */
  public String javaType() {
    return javaType;
  }

  /**
   * Optional business synonyms (e.g. "revenue", "sales value") for NLP matching.
   *
   * @return Optional business synonyms (e.g. "revenue", "sales value") for NLP matching.
   */
  public List<String> synonyms() {
    return synonyms;
  }

  /**
   * Optional human-readable description of the field's business meaning.
   *
   * @return Optional human-readable description of the field's business meaning.
   */
  public String description() {
    return description;
  }

  /**
   * Builds a compact text representation suitable for injection into an AI prompt.
   *
   * @param tableName Optional table name to prefix the column name with, e.g. {@code
   *     "orders.total"}.
   * @return Builds a compact text representation suitable for injection into an AI prompt.
   */
  public String toPromptText(String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("  - ")
        .append(tableName)
        .append('.')
        .append(columnName)
        .append(" (getter: ")
        .append(getterName)
        .append(", type: ")
        .append(javaType)
        .append(')');
    if (description != null) {
      sb.append(" — ").append(description);
    }
    if (!synonyms.isEmpty()) {
      sb.append(" [synonyms: ").append(String.join(", ", synonyms)).append(']');
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "AiEntityField{getter="
        + getterName
        + ", column="
        + columnName
        + ", type="
        + javaType
        + '}';
  }

  /** Builder for {@link AiEntityField}. */
  public static final class Builder {
    private String getterName;
    private String columnName;
    private String javaType;
    private final List<String> synonyms = new ArrayList<>();
    private String description;

    private Builder() {}

    /**
     * Sets the getter method name for this field.
     *
     * @param getterName Getter method name, e.g. {@code getTotal}.
     * @return This builder.
     */
    public Builder getterName(String getterName) {
      this.getterName = getterName;
      return this;
    }

    /**
     * Sets the resolved SQL column name for this field.
     *
     * @param columnName Resolved SQL column name, e.g. {@code total}.
     * @return This builder.
     */
    public Builder columnName(String columnName) {
      this.columnName = columnName;
      return this;
    }

    /**
     * Sets the simple Java type name for this field.
     *
     * @param javaType Simple Java type name, e.g. {@code BigDecimal}, {@code String}, {@code
     *     LocalDate}.
     * @return This builder.
     */
    public Builder javaType(String javaType) {
      this.javaType = javaType;
      return this;
    }

    /**
     * Adds a business synonym for this field.
     *
     * @param synonym Optional business synonym (e.g. "revenue", "sales value") for NLP matching.
     * @return This builder.
     */
    public Builder synonym(String synonym) {
      this.synonyms.add(synonym);
      return this;
    }

    /**
     * Sets the human-readable description of the field's business meaning.
     *
     * @param description Optional human-readable description of the field's business meaning.
     * @return This builder.
     */
    public Builder description(String description) {
      this.description = description;
      return this;
    }

    /**
     * Builds the {@link AiEntityField} instance.
     *
     * @return Builds the {@link AiEntityField} instance.
     */
    public AiEntityField build() {
      return new AiEntityField(this);
    }
  }
}
