package com.aytmatech.querier;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable function interface used to capture method references for type-safe column
 * references.
 *
 * @param <T> The entity type
 * @param <R> The return type
 */
@FunctionalInterface
public interface ColumnRef<T, R> extends Function<T, R>, Serializable {}
