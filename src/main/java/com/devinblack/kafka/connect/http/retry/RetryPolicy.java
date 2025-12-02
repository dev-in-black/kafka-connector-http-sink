package com.devinblack.kafka.connect.http.retry;

/**
 * Interface for retry policies.
 *
 * A retry policy determines:
 * 1. Whether a particular failure should be retried
 * 2. How long to wait before the next retry attempt
 * 3. When to stop retrying (max attempts reached)
 *
 * Implementations can provide different strategies like:
 * - Exponential backoff
 * - Fixed delay
 * - Linear backoff
 */
public interface RetryPolicy {

    /**
     * Check if the operation should be retried based on the HTTP status code.
     *
     * @param statusCode The HTTP status code from the failed request
     * @return true if the operation should be retried, false otherwise
     */
    boolean shouldRetry(int statusCode);

    /**
     * Check if the operation should be retried based on the exception.
     *
     * @param exception The exception thrown during the operation
     * @return true if the operation should be retried, false otherwise
     */
    boolean shouldRetry(Exception exception);

    /**
     * Check if more retry attempts are available.
     *
     * @param attemptNumber The current attempt number (0-based, so 0 is first attempt)
     * @return true if more attempts are available, false otherwise
     */
    boolean hasMoreAttempts(int attemptNumber);

    /**
     * Get the delay in milliseconds before the next retry attempt.
     *
     * @param attemptNumber The attempt number (0-based)
     * @return The delay in milliseconds
     */
    long getDelayMs(int attemptNumber);

    /**
     * Reset the retry policy state (if stateful).
     * This should be called before starting a new operation.
     */
    void reset();
}
