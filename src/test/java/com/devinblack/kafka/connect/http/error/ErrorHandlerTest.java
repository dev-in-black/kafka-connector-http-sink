package com.devinblack.kafka.connect.http.error;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import com.devinblack.kafka.connect.http.client.HttpResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ErrorHandler.
 */
class ErrorHandlerTest {

    private HttpSinkConnectorConfig config;
    private ErrorHandler handler;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.ERROR_TOPIC_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.ERROR_TOPIC_NAME, "error-topic");

        config = new HttpSinkConnectorConfig(props);
        handler = new ErrorHandler(config);
        objectMapper = new ObjectMapper();
    }

    @Test
    void testCreateErrorRecordWithHttpError() throws Exception {
        // Create HTTP response with error
        Map<String, String> responseHeaders = new HashMap<>();
        responseHeaders.put("Content-Type", "application/json");
        HttpResponse httpResponse = new HttpResponse(500, responseHeaders, "{\"error\":\"Internal Server Error\"}", 150L);

        // Create original record
        SinkRecord originalRecord = new SinkRecord(
                "input-topic",
                0,
                null,
                "key123",
                null,
                "{\"data\":\"test\"}",
                100L,
                1234567890000L,
                null
        );

        // Create error record
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "HTTP_ERROR",
                "HTTP request returned error status: 500",
                httpResponse,
                0
        );

        // Verify
        assertNotNull(errorRecord);
        assertEquals("error-topic", errorRecord.getTopic());
        assertEquals("key123", errorRecord.getKey());
        assertNotNull(errorRecord.getValue());

        // Parse and verify JSON payload
        String jsonValue = new String(errorRecord.getValue());
        JsonNode jsonNode = objectMapper.readTree(jsonValue);

        assertEquals("HTTP_ERROR", jsonNode.get("errorType").asText());
        assertEquals("HTTP request returned error status: 500", jsonNode.get("errorMessage").asText());
        assertEquals(500, jsonNode.get("httpStatusCode").asInt());
        assertEquals("{\"error\":\"Internal Server Error\"}", jsonNode.get("httpResponseBody").asText());
        assertEquals("input-topic", jsonNode.get("originalTopic").asText());
        assertEquals(0, jsonNode.get("originalPartition").asInt());
        assertEquals(100L, jsonNode.get("originalOffset").asLong());

        // Verify headers
        assertNotNull(errorRecord.getHeaders());
        Header errorTypeHeader = findHeader(errorRecord, ErrorHandler.HEADER_ERROR_TYPE);
        assertNotNull(errorTypeHeader);
        assertEquals("HTTP_ERROR", new String((byte[]) errorTypeHeader.value()));
    }

    @Test
    void testCreateErrorRecordWithRetryExhaustion() throws Exception {
        // Create HTTP response
        HttpResponse httpResponse = new HttpResponse(503, new HashMap<>(), "Service Unavailable", 2000L);

        // Create original record
        SinkRecord originalRecord = new SinkRecord(
                "events",
                2,
                null,
                null,
                null,
                "{\"event\":\"click\"}",
                500L,
                null,
                null
        );

        // Create error record with retry count
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "RETRY_EXHAUSTED",
                "HTTP request failed after 5 attempts",
                httpResponse,
                5
        );

        // Verify
        assertNotNull(errorRecord);
        assertEquals("error-topic", errorRecord.getTopic());
        assertNull(errorRecord.getKey()); // Original had no key

        // Parse JSON payload
        String jsonValue = new String(errorRecord.getValue());
        JsonNode jsonNode = objectMapper.readTree(jsonValue);

        assertEquals("RETRY_EXHAUSTED", jsonNode.get("errorType").asText());
        assertEquals(5, jsonNode.get("retryCount").asInt());
        assertEquals(503, jsonNode.get("httpStatusCode").asInt());
        assertEquals("events", jsonNode.get("originalTopic").asText());
        assertEquals(2, jsonNode.get("originalPartition").asInt());
        assertEquals(500L, jsonNode.get("originalOffset").asLong());

        // Verify retry count header
        Header retryCountHeader = findHeader(errorRecord, ErrorHandler.HEADER_RETRY_COUNT);
        assertNotNull(retryCountHeader);
    }

    @Test
    void testCreateErrorRecordWithConversionError() throws Exception {
        // Create original record
        SinkRecord originalRecord = new SinkRecord(
                "data-topic",
                1,
                null,
                "key-abc",
                null,
                "invalid-data",
                200L,
                null,
                null
        );

        // Create error record without HTTP response
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "CONVERSION_ERROR",
                "Record conversion failed: Invalid format",
                null,
                0
        );

        // Verify
        assertNotNull(errorRecord);
        assertEquals("error-topic", errorRecord.getTopic());

        // Parse JSON payload
        String jsonValue = new String(errorRecord.getValue());
        JsonNode jsonNode = objectMapper.readTree(jsonValue);

        assertEquals("CONVERSION_ERROR", jsonNode.get("errorType").asText());
        assertEquals("Record conversion failed: Invalid format", jsonNode.get("errorMessage").asText());
        assertFalse(jsonNode.has("httpStatusCode")); // No HTTP response
        assertFalse(jsonNode.has("httpResponseBody")); // No HTTP response
        assertEquals("data-topic", jsonNode.get("originalTopic").asText());

        // Verify no HTTP status code header
        Header statusCodeHeader = findHeader(errorRecord, ErrorHandler.HEADER_HTTP_STATUS_CODE);
        assertNull(statusCodeHeader);
    }

    @Test
    void testCreateErrorRecordWithNullValue() throws Exception {
        // Create original record with null value
        SinkRecord originalRecord = new SinkRecord(
                "null-topic",
                0,
                null,
                "key-null",
                null,
                null,
                300L,
                null,
                null
        );

        // Create error record
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "NULL_VALUE",
                "Null value encountered in record",
                null,
                0
        );

        // Verify
        assertNotNull(errorRecord);

        // Parse JSON payload
        String jsonValue = new String(errorRecord.getValue());
        JsonNode jsonNode = objectMapper.readTree(jsonValue);

        assertEquals("NULL_VALUE", jsonNode.get("errorType").asText());
        assertEquals("Null value encountered in record", jsonNode.get("errorMessage").asText());
    }

    @Test
    void testResolveErrorTopicStatic() {
        // Create original record
        SinkRecord originalRecord = new SinkRecord(
                "source-topic",
                0,
                null,
                null,
                null,
                "data",
                0L,
                null,
                null
        );

        // Resolve topic
        String resolvedTopic = handler.resolveErrorTopic(originalRecord);

        // Verify - should be static name
        assertEquals("error-topic", resolvedTopic);
    }

    @Test
    void testResolveErrorTopicDynamic() {
        // Create config with ${topic} variable
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.ERROR_TOPIC_ENABLED, "true");
        props.put(HttpSinkConnectorConfig.ERROR_TOPIC_NAME, "${topic}-errors");

        HttpSinkConnectorConfig dynamicConfig = new HttpSinkConnectorConfig(props);
        ErrorHandler dynamicHandler = new ErrorHandler(dynamicConfig);

        // Create original record
        SinkRecord originalRecord = new SinkRecord(
                "orders",
                0,
                null,
                null,
                null,
                "data",
                0L,
                null,
                null
        );

        // Resolve topic
        String resolvedTopic = dynamicHandler.resolveErrorTopic(originalRecord);

        // Verify - should have topic name substituted
        assertEquals("orders-errors", resolvedTopic);
    }

    @Test
    void testErrorRecordIncludesOriginalHeaders() throws Exception {
        // Create original record with headers
        ConnectHeaders originalHeaders = new ConnectHeaders();
        originalHeaders.addString("traceId", "trace-123");
        originalHeaders.addString("userId", "user-456");

        SinkRecord originalRecord = new SinkRecord(
                "events",
                0,
                null,
                "key1",
                null,
                "data",
                100L,
                null,
                originalHeaders
        );

        // Create error record
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "HTTP_ERROR",
                "Test error",
                null,
                0
        );

        // Verify original headers are forwarded
        Header traceIdHeader = findHeader(errorRecord, "traceId");
        assertNotNull(traceIdHeader, "Original traceId header should be forwarded");
        assertEquals("trace-123", new String((byte[]) traceIdHeader.value()));

        Header userIdHeader = findHeader(errorRecord, "userId");
        assertNotNull(userIdHeader, "Original userId header should be forwarded");
        assertEquals("user-456", new String((byte[]) userIdHeader.value()));
    }

    @Test
    void testErrorRecordIncludesHttpResponseHeaders() throws Exception {
        // Create HTTP response with headers
        Map<String, String> responseHeaders = new HashMap<>();
        responseHeaders.put("Content-Type", "application/json");
        responseHeaders.put("X-Request-Id", "req-789");
        HttpResponse httpResponse = new HttpResponse(500, responseHeaders, "{}", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "topic1",
                0,
                null,
                null,
                null,
                "data",
                0L,
                null,
                null
        );

        // Create error record
        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "HTTP_ERROR",
                "Test error",
                httpResponse,
                0
        );

        // Verify HTTP response headers are prefixed and included
        Header contentTypeHeader = findHeader(errorRecord, "http.response.Content-Type");
        assertNotNull(contentTypeHeader, "HTTP response header should be prefixed with 'http.response.'");
        assertEquals("application/json", new String((byte[]) contentTypeHeader.value()));

        Header requestIdHeader = findHeader(errorRecord, "http.response.X-Request-Id");
        assertNotNull(requestIdHeader);
        assertEquals("req-789", new String((byte[]) requestIdHeader.value()));
    }

    @Test
    void testErrorRecordTimestampPresent() throws Exception {
        SinkRecord originalRecord = new SinkRecord(
                "topic1",
                0,
                null,
                null,
                null,
                "data",
                0L,
                null,
                null
        );

        long beforeTime = System.currentTimeMillis();

        ErrorRecord errorRecord = handler.createErrorRecord(
                originalRecord,
                "HTTP_ERROR",
                "Test error",
                null,
                0
        );

        long afterTime = System.currentTimeMillis();

        // Parse JSON to verify timestamp
        String jsonValue = new String(errorRecord.getValue());
        JsonNode jsonNode = objectMapper.readTree(jsonValue);

        long errorTimestamp = jsonNode.get("errorTimestamp").asLong();
        assertTrue(errorTimestamp >= beforeTime && errorTimestamp <= afterTime,
                "Error timestamp should be within test execution time");
    }

    // Helper method to find header by key
    private Header findHeader(ErrorRecord errorRecord, String key) {
        if (errorRecord.getHeaders() == null) {
            return null;
        }
        for (Header header : errorRecord.getHeaders()) {
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }
}
