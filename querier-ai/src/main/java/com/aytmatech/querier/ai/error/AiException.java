package com.aytmatech.querier.ai.error;

import java.util.Objects;

/**
 * Unchecked exception thrown by all Querier AI components.
 *
 * <p>Always includes an {@link AiErrorCategory} so callers can distinguish transient failures from
 * permanent configuration problems.
 *
 * <p><strong>Security note:</strong> messages must never include API keys, raw prompt content, or
 * database credentials.
 */
public final class AiException extends RuntimeException {

  /** The category of the error, never null. */
  private final AiErrorCategory category;

  /**
   * Constructs an exception with the given category and message.
   *
   * @param category the category of the error, never null
   * @param message the error message, never null
   */
  public AiException(AiErrorCategory category, String message) {
    super(message);
    this.category = Objects.requireNonNull(category, "category");
  }

  /**
   * Constructs an exception with the given category, message, and cause.
   *
   * @param category the category of the error
   * @param message the error message
   * @param cause the underlying cause of the error
   */
  public AiException(AiErrorCategory category, String message, Throwable cause) {
    super(message, cause);
    this.category = Objects.requireNonNull(category, "category");
  }

  /**
   * Returns the category of this failure.
   *
   * @return The category of this failure.
   */
  public AiErrorCategory category() {
    return category;
  }

  @Override
  public String toString() {
    return "AiException{category=" + category + ", message=" + getMessage() + '}';
  }
}
