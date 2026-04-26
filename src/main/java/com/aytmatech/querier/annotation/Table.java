package com.aytmatech.querier.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify the table name for an entity class. If not present, the class name will be
 * converted to snake_case.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {
  /**
   * The name of the database table.
   *
   * @return The table name to use in SQL queries.
   */
  String value();

  /**
   * The schema of the database table. Default is empty (no schema qualifier).
   *
   * @return The schema name to use in SQL queries.
   */
  String schema() default "";

  /**
   * The catalog of the database table. Default is empty (no catalog qualifier).
   *
   * @return The catalog name to use in SQL queries.
   */
  String catalog() default "";
}
