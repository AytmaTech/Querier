package com.aytmatech.querier.ai.retry;

import java.util.Objects;

/**
 * Controls whether and how many times the Querier AI engine will re-ask the language model when a
 * generated plan fails validation.
 *
 * <p>When retrying, the full list of structured validation issues from the previous attempt is
 * appended to the next request so the model can self-correct without starting from scratch.
 *
 * <p>Use {@link #disabled()} to opt out entirely, or {@link #ofMaxAttempts(int)} for the most
 * common configuration:
 *
 * <pre>{@code
 * QuerierAi ai = QuerierAi.builder()
 *     .client(client)
 *     .retryPolicy(RetryPolicy.ofMaxAttempts(2))   // up to 2 correction attempts
 *     .build();
 * }</pre>
 *
 * <p>A single attempt means one initial call followed by up to {@code maxAttempts} correction
 * calls, so the model is contacted at most {@code maxAttempts + 1} times total.
 */
public final class RetryPolicy {

  /** Singleton disabled instance. */
  private static final RetryPolicy DISABLED = new RetryPolicy(false, 0);

  private final boolean enabled;
  private final int maxAttempts;

  private RetryPolicy(boolean enabled, int maxAttempts) {
    this.enabled = enabled;
    this.maxAttempts = maxAttempts;
  }

  /**
   * Returns a policy where no retries are ever performed. This is the default behaviour when no
   * policy is configured on {@link com.aytmatech.querier.ai.QuerierAi}.
   *
   * @return a {@link RetryPolicy} instance that disables retry logic
   */
  public static RetryPolicy disabled() {
    return DISABLED;
  }

  /**
   * Returns a policy that allows up to {@code maxAttempts} correction attempts after the initial
   * call.
   *
   * @param maxAttempts number of additional attempts (must be &ge; 1)
   * @return a new {@link RetryPolicy} instance with the specified maximum attempts
   * @throws IllegalArgumentException if {@code maxAttempts < 1}
   */
  public static RetryPolicy ofMaxAttempts(int maxAttempts) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1, got: " + maxAttempts);
    }
    return new RetryPolicy(true, maxAttempts);
  }

  /**
   * Returns a fluent builder for fine-grained configuration.
   *
   * @return a new {@link Builder} instance for constructing a {@link RetryPolicy} with custom
   *     settings.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Whether retry logic is active.
   *
   * @return true if retry logic is enabled, false otherwise.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Maximum number of correction attempts after the first failure. Meaningful only when {@link
   * #isEnabled()} is {@code true}.
   *
   * @return the maximum number of correction attempts allowed (must be &ge; 1 if enabled)
   */
  public int maxAttempts() {
    return maxAttempts;
  }

  @Override
  public String toString() {
    return enabled
        ? "RetryPolicy{enabled=true, maxAttempts=" + maxAttempts + '}'
        : "RetryPolicy{enabled=false}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RetryPolicy other)) return false;
    return enabled == other.enabled && maxAttempts == other.maxAttempts;
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, maxAttempts);
  }

  /** Fluent builder for {@link RetryPolicy}. */
  public static final class Builder {

    private boolean enabled = true;
    private int maxAttempts = 1;

    private Builder() {}

    /**
     * Enables or disables retry logic. Default: {@code true}.
     *
     * @param enabled whether retry logic should be enabled
     * @return this builder
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Sets the maximum number of correction attempts. Default: {@code 1}.
     *
     * @param maxAttempts must be &ge; 1
     * @return this builder
     * @throws IllegalArgumentException if {@code maxAttempts < 1}
     */
    public Builder maxAttempts(int maxAttempts) {
      if (maxAttempts < 1) {
        throw new IllegalArgumentException("maxAttempts must be >= 1, got: " + maxAttempts);
      }
      this.maxAttempts = maxAttempts;
      return this;
    }

    /**
     * Builds a {@link RetryPolicy} instance from the current state of this builder.
     *
     * @return a {@link RetryPolicy} instance built from the current state of this builder
     * @throws IllegalStateException if {@code maxAttempts < 1}
     */
    public RetryPolicy build() {
      return new RetryPolicy(enabled, maxAttempts);
    }
  }
}
