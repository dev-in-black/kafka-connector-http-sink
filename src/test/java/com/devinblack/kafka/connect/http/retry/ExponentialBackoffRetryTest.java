package com.devinblack.kafka.connect.http.retry;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ExponentialBackoffRetry.
 */
class ExponentialBackoffRetryTest {

    private HttpSinkConnectorConfig config;
    private ExponentialBackoffRetry retryPolicy;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.RETRY_MAX_ATTEMPTS, "5");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_INITIAL_MS, "1000");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "60000");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MULTIPLIER, "2.0");
        props.put(HttpSinkConnectorConfig.RETRY_ON_STATUS_CODES, "429,500,502,503,504");

        config = new HttpSinkConnectorConfig(props);
        retryPolicy = new ExponentialBackoffRetry(config);
    }

    @Test
    void testInitialization() {
        assertEquals(5, retryPolicy.getMaxAttempts());
        assertEquals(1000, retryPolicy.getInitialDelayMs());
        assertEquals(60000, retryPolicy.getMaxDelayMs());
        assertEquals(2.0, retryPolicy.getMultiplier());

        List<Integer> expectedCodes = List.of(429, 500, 502, 503, 504);
        assertEquals(expectedCodes, retryPolicy.getRetryableStatusCodes());
    }

    @Test
    void testShouldRetryOnRetryableStatusCode() {
        assertTrue(retryPolicy.shouldRetry(429), "Should retry on 429");
        assertTrue(retryPolicy.shouldRetry(500), "Should retry on 500");
        assertTrue(retryPolicy.shouldRetry(502), "Should retry on 502");
        assertTrue(retryPolicy.shouldRetry(503), "Should retry on 503");
        assertTrue(retryPolicy.shouldRetry(504), "Should retry on 504");
    }

    @Test
    void testShouldNotRetryOnNonRetryableStatusCode() {
        assertFalse(retryPolicy.shouldRetry(200), "Should not retry on 200");
        assertFalse(retryPolicy.shouldRetry(400), "Should not retry on 400");
        assertFalse(retryPolicy.shouldRetry(401), "Should not retry on 401");
        assertFalse(retryPolicy.shouldRetry(403), "Should not retry on 403");
        assertFalse(retryPolicy.shouldRetry(404), "Should not retry on 404");
    }

    @Test
    void testShouldRetryOnIOException() {
        IOException ioException = new IOException("Connection refused");
        assertTrue(retryPolicy.shouldRetry(ioException), "Should retry on IOException");
    }

    @Test
    void testShouldRetryOnSocketTimeoutException() {
        SocketTimeoutException timeoutException = new SocketTimeoutException("Read timed out");
        assertTrue(retryPolicy.shouldRetry(timeoutException), "Should retry on SocketTimeoutException");
    }

    @Test
    void testShouldNotRetryOnOtherExceptions() {
        IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
        assertFalse(retryPolicy.shouldRetry(illegalArgException), "Should not retry on IllegalArgumentException");

        NullPointerException nullPointerException = new NullPointerException("Null pointer");
        assertFalse(retryPolicy.shouldRetry(nullPointerException), "Should not retry on NullPointerException");
    }

    @Test
    void testShouldRetryOnExceptionWithRetryableCause() {
        IOException cause = new IOException("Connection reset");
        RuntimeException exception = new RuntimeException("Wrapped exception", cause);
        assertTrue(retryPolicy.shouldRetry(exception), "Should retry when cause is IOException");
    }

    @Test
    void testHasMoreAttempts() {
        assertTrue(retryPolicy.hasMoreAttempts(0), "Should have more attempts at attempt 0");
        assertTrue(retryPolicy.hasMoreAttempts(1), "Should have more attempts at attempt 1");
        assertTrue(retryPolicy.hasMoreAttempts(2), "Should have more attempts at attempt 2");
        assertTrue(retryPolicy.hasMoreAttempts(3), "Should have more attempts at attempt 3");
        assertFalse(retryPolicy.hasMoreAttempts(4), "Should not have more attempts at attempt 4");
        assertFalse(retryPolicy.hasMoreAttempts(5), "Should not have more attempts at attempt 5");
    }

    @Test
    void testGetDelayExponentialBackoff() {
        // With initialDelay=1000ms and multiplier=2.0:
        // Attempt 0: 1000ms
        // Attempt 1: 2000ms
        // Attempt 2: 4000ms
        // Attempt 3: 8000ms
        // Attempt 4: 16000ms

        assertEquals(1000, retryPolicy.getDelayMs(0), "Delay for attempt 0 should be 1000ms");
        assertEquals(2000, retryPolicy.getDelayMs(1), "Delay for attempt 1 should be 2000ms");
        assertEquals(4000, retryPolicy.getDelayMs(2), "Delay for attempt 2 should be 4000ms");
        assertEquals(8000, retryPolicy.getDelayMs(3), "Delay for attempt 3 should be 8000ms");
        assertEquals(16000, retryPolicy.getDelayMs(4), "Delay for attempt 4 should be 16000ms");
    }

    @Test
    void testGetDelayMaxCap() {
        // With maxDelay=60000ms, high attempt numbers should be capped
        long delay = retryPolicy.getDelayMs(10); // Would be 1024000ms without cap
        assertEquals(60000, delay, "Delay should be capped at maxDelayMs");
    }

    @Test
    void testGetDelayWithCustomMultiplier() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_INITIAL_MS, "500");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MULTIPLIER, "3.0");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "100000");

        ExponentialBackoffRetry customPolicy = new ExponentialBackoffRetry(new HttpSinkConnectorConfig(props));

        // With initialDelay=500ms and multiplier=3.0:
        // Attempt 0: 500ms
        // Attempt 1: 1500ms
        // Attempt 2: 4500ms
        // Attempt 3: 13500ms

        assertEquals(500, customPolicy.getDelayMs(0), "Delay for attempt 0 should be 500ms");
        assertEquals(1500, customPolicy.getDelayMs(1), "Delay for attempt 1 should be 1500ms");
        assertEquals(4500, customPolicy.getDelayMs(2), "Delay for attempt 2 should be 4500ms");
        assertEquals(13500, customPolicy.getDelayMs(3), "Delay for attempt 3 should be 13500ms");
    }

    @Test
    void testResetIsNoOp() {
        // reset() is a no-op for stateless policy, but should not throw
        assertDoesNotThrow(() -> retryPolicy.reset(), "reset() should not throw");
    }

    @Test
    void testCustomRetryableCodes() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_ON_STATUS_CODES, "408,429,503");

        ExponentialBackoffRetry customPolicy = new ExponentialBackoffRetry(new HttpSinkConnectorConfig(props));

        assertTrue(customPolicy.shouldRetry(408), "Should retry on 408");
        assertTrue(customPolicy.shouldRetry(429), "Should retry on 429");
        assertTrue(customPolicy.shouldRetry(503), "Should retry on 503");

        assertFalse(customPolicy.shouldRetry(500), "Should not retry on 500");
        assertFalse(customPolicy.shouldRetry(502), "Should not retry on 502");
    }

    @Test
    void testMaxAttemptsOne() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_MAX_ATTEMPTS, "1");

        ExponentialBackoffRetry singleAttemptPolicy = new ExponentialBackoffRetry(new HttpSinkConnectorConfig(props));

        assertEquals(1, singleAttemptPolicy.getMaxAttempts());
        assertFalse(singleAttemptPolicy.hasMoreAttempts(0), "Should not have more attempts after first attempt");
    }
}
