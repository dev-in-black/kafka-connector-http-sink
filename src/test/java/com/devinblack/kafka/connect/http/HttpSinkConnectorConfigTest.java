package com.devinblack.kafka.connect.http;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HttpSinkConnectorConfig validation and configuration.
 */
class HttpSinkConnectorConfigTest {

    @Test
    void testMinimalValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080/api");

        assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props),
                "Minimal configuration should be valid");
    }

    @Test
    void testHttpApiUrlRequired() {
        Map<String, String> props = new HashMap<>();

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "HTTP API URL is required");
    }

    @Test
    void testDefaultValues() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);

        assertEquals("POST", config.getHttpMethod());
        assertEquals("none", config.getAuthType());
        assertEquals(5000, config.getHttpConnectionTimeoutMs());
        assertEquals(30000, config.getHttpRequestTimeoutMs());
        assertTrue(config.isRetryEnabled());
        assertEquals(5, config.getRetryMaxAttempts());
    }

    @Test
    void testHttpMethodConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HTTP_METHOD, "PUT");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertEquals("PUT", config.getHttpMethod());
    }

    // Authentication Tests

    @Test
    void testBasicAuthValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "basic");
        props.put(HttpSinkConnectorConfig.AUTH_BASIC_USERNAME, "user");
        props.put(HttpSinkConnectorConfig.AUTH_BASIC_PASSWORD, "pass");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertEquals("basic", config.getAuthType());
        assertEquals("user", config.getAuthBasicUsername());
        assertEquals("pass", config.getAuthBasicPassword());
    }

    @Test
    void testBasicAuthMissingUsername() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "basic");
        props.put(HttpSinkConnectorConfig.AUTH_BASIC_PASSWORD, "pass");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "Basic auth requires username");
    }

    @Test
    void testBasicAuthMissingPassword() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "basic");
        props.put(HttpSinkConnectorConfig.AUTH_BASIC_USERNAME, "user");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "Basic auth requires password");
    }

    @Test
    void testBearerAuthValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "bearer");
        props.put(HttpSinkConnectorConfig.AUTH_BEARER_TOKEN, "my-token");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertEquals("bearer", config.getAuthType());
        assertEquals("my-token", config.getAuthBearerToken());
    }

    @Test
    void testBearerAuthMissingToken() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "bearer");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "Bearer auth requires token");
    }

    @Test
    void testApiKeyAuthValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "apikey");
        props.put(HttpSinkConnectorConfig.AUTH_APIKEY_NAME, "X-API-Key");
        props.put(HttpSinkConnectorConfig.AUTH_APIKEY_VALUE, "secret-key");
        props.put(HttpSinkConnectorConfig.AUTH_APIKEY_LOCATION, "header");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertEquals("apikey", config.getAuthType());
        assertEquals("X-API-Key", config.getAuthApiKeyName());
        assertEquals("secret-key", config.getAuthApiKeyValue());
        assertEquals("header", config.getAuthApiKeyLocation());
    }

    @Test
    void testApiKeyAuthMissingName() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "apikey");
        props.put(HttpSinkConnectorConfig.AUTH_APIKEY_VALUE, "secret-key");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "API key auth requires name");
    }

    @Test
    void testOAuth2ValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "oauth2");
        props.put(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_URL, "http://localhost:9090/token");
        props.put(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_ID, "client-id");
        props.put(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_SECRET, "client-secret");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertEquals("oauth2", config.getAuthType());
        assertEquals("http://localhost:9090/token", config.getAuthOAuth2TokenUrl());
        assertEquals("client-id", config.getAuthOAuth2ClientId());
        assertEquals("client-secret", config.getAuthOAuth2ClientSecret());
    }

    @Test
    void testOAuth2MissingClientId() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "oauth2");
        props.put(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_URL, "http://localhost:9090/token");
        props.put(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_SECRET, "client-secret");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "OAuth2 requires client ID");
    }

    // Response Topic Tests

    @Test
    void testResponseTopicValidConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertTrue(config.isResponseTopicEnabled());
        assertEquals("responses", config.getResponseTopicName());
    }

    @Test
    void testResponseTopicEnabledWithoutName() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_ENABLED, "true");

        assertThrows(ConfigException.class, () -> new HttpSinkConnectorConfig(props),
                "Response topic enabled requires topic name");
    }

    @Test
    void testResponseTopicDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_ENABLED, "false");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertFalse(config.isResponseTopicEnabled());
    }

    // Header Forwarding Tests

    @Test
    void testHeaderForwardConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_PREFIX, "kafka.");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_INCLUDE, "content-type,user-id");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_EXCLUDE, "internal-*");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));
        assertTrue(config.isHeaderForwardEnabled());
        assertEquals("kafka.", config.getHeadersForwardPrefix());
    }

    // Retry Configuration Tests

    @Test
    void testRetryConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.RETRY_MAX_ATTEMPTS, "3");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_INITIAL_MS, "500");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MAX_MS, "30000");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_MULTIPLIER, "1.5");
        props.put(HttpSinkConnectorConfig.RETRY_ON_STATUS_CODES, "408,429,503");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertTrue(config.isRetryEnabled());
        assertEquals(3, config.getRetryMaxAttempts());
        assertEquals(500, config.getRetryBackoffInitialMs());
        assertEquals(30000, config.getRetryBackoffMaxMs());
        assertEquals(1.5, config.getRetryBackoffMultiplier());

        List<Integer> expectedCodes = List.of(408, 429, 503);
        assertEquals(expectedCodes, config.getRetryOnStatusCodes());
    }

    @Test
    void testRetryDisabled() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RETRY_ENABLED, "false");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertFalse(config.isRetryEnabled());
    }

    // Error Handling Tests

    @Test
    void testErrorHandlingConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.BEHAVIOR_ON_ERROR, "fail");
        props.put(HttpSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES, "ignore");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertEquals("fail", config.getBehaviorOnError());
        assertEquals("ignore", config.getBehaviorOnNullValues());
    }

    // Static Headers Tests

    @Test
    void testStaticHeadersConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HEADERS_STATIC, "Content-Type:application/json,X-Custom:value");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        Map<String, String> headers = config.getHeadersStatic();

        assertEquals(2, headers.size());
        assertEquals("application/json", headers.get("Content-Type"));
        assertEquals("value", headers.get("X-Custom"));
    }

    @Test
    void testStaticHeadersEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        Map<String, String> headers = config.getHeadersStatic();

        assertNotNull(headers);
        assertTrue(headers.isEmpty());
    }

    // Timeout Configuration Tests

    @Test
    void testTimeoutConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HTTP_CONNECTION_TIMEOUT_MS, "5000");
        props.put(HttpSinkConnectorConfig.HTTP_REQUEST_TIMEOUT_MS, "10000");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertEquals(5000, config.getHttpConnectionTimeoutMs());
        assertEquals(10000, config.getHttpRequestTimeoutMs());
    }

    // Connection Pool Tests

    @Test
    void testConnectionPoolConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HTTP_MAX_CONNECTIONS_PER_ROUTE, "20");
        props.put(HttpSinkConnectorConfig.HTTP_MAX_CONNECTIONS_TOTAL, "100");

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertEquals(20, config.getHttpMaxConnectionsPerRoute());
        assertEquals(100, config.getHttpMaxConnectionsTotal());
    }

    // Complex Configuration Tests

    @Test
    void testCompleteConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "https://api.example.com/webhook");
        props.put(HttpSinkConnectorConfig.HTTP_METHOD, "PUT");
        props.put(HttpSinkConnectorConfig.AUTH_TYPE, "bearer");
        props.put(HttpSinkConnectorConfig.AUTH_BEARER_TOKEN, "secret-token");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_PREFIX, "x-kafka-");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "http-responses");
        props.put(HttpSinkConnectorConfig.RETRY_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.RETRY_MAX_ATTEMPTS, "5");
        props.put(HttpSinkConnectorConfig.BEHAVIOR_ON_ERROR, "log");

        HttpSinkConnectorConfig config = assertDoesNotThrow(() -> new HttpSinkConnectorConfig(props));

        assertEquals("https://api.example.com/webhook", config.getHttpApiUrl());
        assertEquals("PUT", config.getHttpMethod());
        assertEquals("bearer", config.getAuthType());
        assertTrue(config.isHeaderForwardEnabled());
        assertTrue(config.isResponseTopicEnabled());
        assertTrue(config.isRetryEnabled());
        assertEquals("log", config.getBehaviorOnError());
    }
}
