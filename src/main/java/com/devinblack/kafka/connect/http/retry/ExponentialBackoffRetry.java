package com.devinblack.kafka.connect.http.retry;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.List;

/**
 * Exponential backoff retry policy.
 *
 * Implements retry with exponential backoff:
 * - First retry: initialDelayMs
 * - Second retry: initialDelayMs * multiplier
 * - Third retry: initialDelayMs * multiplier^2
 * - And so on, up to maxDelayMs
 *
 * Example with default settings (initial=1000ms, multiplier=2.0, max=60000ms):
 * - Attempt 1: 1000ms
 * - Attempt 2: 2000ms
 * - Attempt 3: 4000ms
 * - Attempt 4: 8000ms
 * - Attempt 5: 16000ms
 * - Attempt 6+: 60000ms (capped at max)
 */
public class ExponentialBackoffRetry implements RetryPolicy {
    private static final Logger log = LoggerFactory.getLogger(ExponentialBackoffRetry.class);

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final List<Integer> retryableStatusCodes;

    public ExponentialBackoffRetry(HttpSinkConnectorConfig config) {
        this.maxAttempts = config.getRetryMaxAttempts();
        this.initialDelayMs = config.getRetryBackoffInitialMs();
        this.maxDelayMs = config.getRetryBackoffMaxMs();
        this.multiplier = config.getRetryBackoffMultiplier();
        this.retryableStatusCodes = config.getRetryOnStatusCodes();

        log.info("ExponentialBackoffRetry initialized: maxAttempts={}, initialDelayMs={}, " +
                "maxDelayMs={}, multiplier={}, retryableStatusCodes={}",
                maxAttempts, initialDelayMs, maxDelayMs, multiplier, retryableStatusCodes);
    }

    @Override
    public boolean shouldRetry(int statusCode) {
        boolean shouldRetry = retryableStatusCodes.contains(statusCode);
        log.debug("shouldRetry(statusCode={}): {}", statusCode, shouldRetry);
        return shouldRetry;
    }

    @Override
    public boolean shouldRetry(Exception exception) {
        // Retry on network-related exceptions
        boolean shouldRetry = isRetryableException(exception);
        log.debug("shouldRetry(exception={}): {}", exception.getClass().getSimpleName(), shouldRetry);
        return shouldRetry;
    }

    /**
     * Determine if an exception is retryable.
     */
    private boolean isRetryableException(Exception exception) {
        // Network/IO exceptions are generally retryable
        if (exception instanceof IOException) {
            return true;
        }

        // Timeout exceptions are retryable
        if (exception instanceof SocketTimeoutException) {
            return true;
        }

        // Check if the exception is caused by a retryable exception
        Throwable cause = exception.getCause();
        if (cause instanceof IOException || cause instanceof SocketTimeoutException) {
            return true;
        }

        // Default: don't retry
        return false;
    }

    @Override
    public boolean hasMoreAttempts(int attemptNumber) {
        // attemptNumber is 0-based, so attempt 0 is the first attempt
        // We allow maxAttempts total attempts (including the initial one)
        boolean hasMore = attemptNumber < maxAttempts - 1;
        log.debug("hasMoreAttempts(attemptNumber={}): {} (maxAttempts={})",
                attemptNumber, hasMore, maxAttempts);
        return hasMore;
    }

    @Override
    public long getDelayMs(int attemptNumber) {
        // Calculate exponential backoff delay
        // attemptNumber is 0-based, so first retry (after initial attempt) is attemptNumber=1
        long delay = (long) (initialDelayMs * Math.pow(multiplier, attemptNumber));

        // Cap at maximum delay
        delay = Math.min(delay, maxDelayMs);

        log.debug("getDelayMs(attemptNumber={}): {}ms", attemptNumber, delay);
        return delay;
    }

    @Override
    public void reset() {
        // This implementation is stateless, so nothing to reset
        log.trace("reset() called (no-op for stateless policy)");
    }

    // Getters for testing
    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public List<Integer> getRetryableStatusCodes() {
        return retryableStatusCodes;
    }
}
