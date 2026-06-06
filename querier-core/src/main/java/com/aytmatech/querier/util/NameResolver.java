package com.aytmatech.querier.util;

import com.aytmatech.querier.ColumnRef;
import com.aytmatech.querier.QuoteStrategy;
import com.aytmatech.querier.annotation.Column;
import com.aytmatech.querier.annotation.Table;
import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Utility class for resolving table and column names from classes and method references. Supports
 * both custom annotations (@Table, @Column) and JPA annotations (jakarta.persistence.Table,
 * jakarta.persistence.Column).
 */
@SuppressWarnings("unchecked")
public final class NameResolver {

  private NameResolver() {}

  /**
   * Gets the table and column names for a method reference.
   *
   * @param columnRef the method reference to resolve
   * @param <T> the entity type
   * @param <R> the return type
   * @return a TableAndColumnName containing the resolved table and column names
   */
  public static <T, R> TableAndColumnName resolve(ColumnRef<T, R> columnRef) {
    ClassAndMethod cam = getImplClassAndImplMethodName(columnRef);
    return new TableAndColumnName(
        NameResolver.getTableName(cam.implClass()),
        NameResolver.getColumnName(cam.implClass(), cam.implMethodName()));
  }

  /**
   * Gets the table and column names for a method reference with the specified quote strategy.
   *
   * @param columnRef the method reference to resolve
   * @param quoteStrategy the quote strategy to apply to the resolved names
   * @param <T> the entity type
   * @param <R> the return type
   * @return a TableAndColumnName containing the resolved and quoted table and column names
   */
  public static <T, R> TableAndColumnName resolve(
      ColumnRef<T, R> columnRef, QuoteStrategy quoteStrategy) {
    ClassAndMethod cam = getImplClassAndImplMethodName(columnRef);
    return new TableAndColumnName(
        NameResolver.getTableName(cam.implClass(), quoteStrategy),
        NameResolver.getColumnName(cam.implClass(), cam.implMethodName(), quoteStrategy));
  }

  /**
   * Gets the table name for a given class. First checks for custom @Table annotation, then
   * JPA @Table annotation, otherwise converts class name to snake_case.
   *
   * @param clazz the class to get the table name for
   * @return the table name
   */
  public static String getTableName(Class<?> clazz) {
    return getTableName(clazz, QuoteStrategy.NONE);
  }

  /**
   * Gets the table name for a given class with the specified quote strategy. First checks for
   * custom @Table annotation, then JPA @Table annotation, otherwise converts class name to
   * snake_case.
   *
   * @param clazz the class to get the table name for
   * @param quoteStrategy the quote strategy to apply
   * @return the quoted table name
   */
  public static String getTableName(Class<?> clazz, QuoteStrategy quoteStrategy) {
    Table tableAnnotation = clazz.getAnnotation(Table.class);
    if (tableAnnotation != null) {
      return buildQualifiedTableName(
          tableAnnotation.catalog(),
          tableAnnotation.schema(),
          tableAnnotation.value(),
          quoteStrategy);
    }

    String jpaTableName = getJpaTableName(clazz, quoteStrategy);
    if (jpaTableName != null) {
      return jpaTableName;
    }

    return quoteStrategy.quote(toSnakeCase(clazz.getSimpleName()));
  }

  /**
   * Gets the column name from a class and method reference. First checks for custom @Column
   * annotation on the method, then JPA @Column annotation, otherwise converts getter name to
   * snake_case.
   *
   * @param clazz the class to get the column name for
   * @param methodName the column reference method name
   * @return the column name
   */
  public static String getColumnName(Class<?> clazz, String methodName) {
    return getColumnName(clazz, methodName, QuoteStrategy.NONE);
  }

  /**
   * Gets the column name from a method reference with the specified quote strategy. First checks
   * for custom @Column annotation on the method, then JPA @Column annotation, otherwise converts
   * getter name to snake_case.
   *
   * @param clazz the class to get the column name for
   * @param methodName the column reference method name
   * @param quoteStrategy the quote strategy to apply
   * @return the quoted column name
   */
  private static String getColumnName(
      Class<?> clazz, String methodName, QuoteStrategy quoteStrategy) {
    try {
      try {
        Method method = clazz.getDeclaredMethod(methodName);

        Column columnAnnotation = method.getAnnotation(Column.class);
        if (columnAnnotation != null) {
          return quoteStrategy.quote(columnAnnotation.value());
        }

        String jpaColumnName = getJpaColumnName(method);
        if (jpaColumnName != null) {
          return quoteStrategy.quote(jpaColumnName);
        }
      } catch (NoSuchMethodException e) {
        // Method not found, continue with name conversion
      }

      String columnName = methodName;
      if (methodName.startsWith("get") && methodName.length() > 3) {
        columnName = methodName.substring(3);
      } else if (methodName.startsWith("is") && methodName.length() > 2) {
        columnName = methodName.substring(2);
      }

      return quoteStrategy.quote(toSnakeCase(columnName));
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve column name from method reference", e);
    }
  }

  /**
   * Gets the class that a method reference belongs to.
   *
   * @param columnRef the method reference to resolve
   * @param <T> the entity type
   * @param <R> the return type
   * @return a ClassAndMethod containing the implementation class and method name of the method
   *     reference
   */
  private static <T, R> ClassAndMethod getImplClassAndImplMethodName(ColumnRef<T, R> columnRef) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandles.Lookup privateLookup =
          MethodHandles.privateLookupIn(columnRef.getClass(), lookup);

      MethodHandle writeReplace =
          privateLookup.findVirtual(
              columnRef.getClass(), "writeReplace", MethodType.methodType(Object.class));

      Object replacement = writeReplace.invoke(columnRef);
      SerializedLambda serializedLambda = (SerializedLambda) replacement;

      Class<?> implClass = Class.forName(serializedLambda.getImplClass().replace('/', '.'));
      String implMethodName = serializedLambda.getImplMethodName();
      return new ClassAndMethod(implClass, implMethodName);
    } catch (Throwable t) {
      throw new RuntimeException("Failed to resolve class from method reference", t);
    }
  }

  /**
   * Converts a camelCase or PascalCase string to snake_case.
   *
   * @param str the string to convert
   * @return the converted snake_case string
   */
  public static String toSnakeCase(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }

    StringBuilder result = new StringBuilder();
    result.append(Character.toLowerCase(str.charAt(0)));

    for (int i = 1; i < str.length(); i++) {
      char ch = str.charAt(i);
      if (Character.isUpperCase(ch)) {
        result.append('_');
        result.append(Character.toLowerCase(ch));
      } else {
        result.append(ch);
      }
    }

    return result.toString();
  }

  /**
   * Builds a fully qualified table name from catalog, schema, and table name with quoting. Each
   * segment is quoted individually according to the quote strategy.
   *
   * @param catalog the catalog name (may be null or empty)
   * @param schema the schema name (may be null or empty)
   * @param name the table name
   * @param quoteStrategy the quote strategy to apply
   * @return the qualified and quoted table name
   */
  private static String buildQualifiedTableName(
      String catalog, String schema, String name, QuoteStrategy quoteStrategy) {
    StringBuilder qualified = new StringBuilder();

    if (catalog != null && !catalog.isEmpty()) {
      qualified.append(quoteStrategy.quote(catalog)).append(".");
    }

    if (schema != null && !schema.isEmpty()) {
      qualified.append(quoteStrategy.quote(schema)).append(".");
    }

    qualified.append(quoteStrategy.quote(name));

    return qualified.toString();
  }

  /**
   * Gets table name from JPA @Table annotation if present with quoting. Returns null if JPA is not
   * available or annotation is not present.
   *
   * @param clazz the class to get the table name for
   * @param quoteStrategy the quote strategy to apply
   * @return the quoted table name or null
   */
  private static String getJpaTableName(Class<?> clazz, QuoteStrategy quoteStrategy) {
    try {
      Class<?> jpaTableClass = Class.forName("jakarta.persistence.Table");
      Object annotation = clazz.getAnnotation((Class<? extends Annotation>) jpaTableClass);
      if (annotation != null) {
        Method nameMethod = jpaTableClass.getMethod("name");
        String name = (String) nameMethod.invoke(annotation);
        if (name != null && !name.isEmpty()) {
          Method schemaMethod = jpaTableClass.getMethod("schema");
          String schema = (String) schemaMethod.invoke(annotation);
          Method catalogMethod = jpaTableClass.getMethod("catalog");
          String catalog = (String) catalogMethod.invoke(annotation);
          return buildQualifiedTableName(catalog, schema, name, quoteStrategy);
        }
      }
    } catch (ClassNotFoundException e) {
      try {
        Class<?> javaxTableClass = Class.forName("javax.persistence.Table");
        Object annotation = clazz.getAnnotation((Class<? extends Annotation>) javaxTableClass);
        if (annotation != null) {
          Method nameMethod = javaxTableClass.getMethod("name");
          String name = (String) nameMethod.invoke(annotation);
          if (name != null && !name.isEmpty()) {
            Method schemaMethod = javaxTableClass.getMethod("schema");
            String schema = (String) schemaMethod.invoke(annotation);
            Method catalogMethod = javaxTableClass.getMethod("catalog");
            String catalog = (String) catalogMethod.invoke(annotation);
            return buildQualifiedTableName(catalog, schema, name, quoteStrategy);
          }
        }
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException ex) {
        // javax.persistence also not available or annotation processing failed, ignore
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      // Annotation processing failed, ignore
    }
    return null;
  }

  /**
   * Gets column name from JPA @Column annotation if present. Returns null if JPA is not available
   * or annotation is not present.
   *
   * @param method the method to get the column name for
   * @return the column name or null
   */
  private static String getJpaColumnName(Method method) {
    try {
      Class<?> jpaColumnClass = Class.forName("jakarta.persistence.Column");
      Object annotation = method.getAnnotation((Class<? extends Annotation>) jpaColumnClass);
      if (annotation != null) {
        Method nameMethod = jpaColumnClass.getMethod("name");
        String name = (String) nameMethod.invoke(annotation);
        if (name != null && !name.isEmpty()) {
          return name;
        }
      }
    } catch (ClassNotFoundException e) {
      try {
        Class<?> javaxColumnClass = Class.forName("javax.persistence.Column");
        Object annotation = method.getAnnotation((Class<? extends Annotation>) javaxColumnClass);
        if (annotation != null) {
          Method nameMethod = javaxColumnClass.getMethod("name");
          String name = (String) nameMethod.invoke(annotation);
          if (name != null && !name.isEmpty()) {
            return name;
          }
        }
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException ex) {
        // javax.persistence also not available, ignore
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      // Annotation processing failed, ignore
    }
    return null;
  }

  /**
   * Simple record to hold resolved table and column names together.
   *
   * @param tableName the resolved table name
   * @param columnName the resolved column name
   */
  public record TableAndColumnName(String tableName, String columnName) {}

  /**
   * Simple record to hold the implementation class and method name extracted from a method
   * reference. This is used internally to resolve the table and column names based on the class and
   * method of the method reference.
   *
   * @param implClass the implementation class that contains the method reference
   * @param implMethodName the name of the method in the implementation class that corresponds to
   *     the method reference
   */
  private record ClassAndMethod(Class<?> implClass, String implMethodName) {}
}
