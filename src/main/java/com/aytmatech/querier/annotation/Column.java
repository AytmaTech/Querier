package com.aytmatech.querier.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify the column name for a getter method or field. If not present, the getter
 * name will be converted to snake_case.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Column {
  /**
   * The name of the database column.
   *
   * @return The column name to use in SQL queries.
   */
  String value();
}
