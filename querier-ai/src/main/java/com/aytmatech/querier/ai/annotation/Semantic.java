package com.aytmatech.querier.ai.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enriches an entity class, getter method or field with AI-facing semantic metadata.
 *
 * <p>The Querier AI module reads this annotation during schema inspection and injects the {@link
 * #description()} and {@link #synonyms()} into the prompt context sent to the language model,
 * helping it map natural-language terms to the correct column.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @Semantic(
 *     description = "Total monetary value of the order",
 *     synonyms = {"revenue", "sales value", "order value"}
 * )
 * public BigDecimal getTotal() { return total; }
 * }</pre>
 *
 * <p>The annotation may be placed on either the getter method or the backing field; the inspector
 * checks the getter first, then falls back to the field with the derived name.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
public @interface Semantic {

  /**
   * Human-readable description of this field's business meaning. Injected into AI prompts as a
   * plain-English explanation.
   *
   * @return description string, empty means none
   */
  String description() default "";

  /**
   * Business synonyms the AI should treat as equivalent to this column name. Examples: {@code
   * {"revenue", "sales value", "order value"}}.
   *
   * @return array of synonym strings, empty means none
   */
  String[] synonyms() default {};
}
