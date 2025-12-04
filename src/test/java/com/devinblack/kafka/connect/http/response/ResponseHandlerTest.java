package com.devinblack.kafka.connect.http.response;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import com.devinblack.kafka.connect.http.client.HttpResponse;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ResponseHandler.
 */
class ResponseHandlerTest {

    private HttpSinkConnectorConfig config;
    private ResponseHandler handler;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_KEY, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_REQUEST_METADATA, "true");

        config = new HttpSinkConnectorConfig(props);
        handler = new ResponseHandler(config);
    }

    @Test
    void testCreateResponseRecordBasic() {
        // Create HTTP response
        Map<String, String> responseHeaders = new HashMap<>();
        responseHeaders.put("Content-Type", "application/json");
        HttpResponse httpResponse = new HttpResponse(200, responseHeaders, "{\"status\":\"ok\"}", 150L);

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

        // Create response record
        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Verify
        assertNotNull(responseRecord);
        assertEquals("responses", responseRecord.getTopic());
        assertEquals("key123", responseRecord.getKey());
        assertNotNull(responseRecord.getValue());
        assertEquals("{\"status\":\"ok\"}", new String(responseRecord.getValue()));

        // Verify metadata
        ResponseMetadata metadata = responseRecord.getMetadata();
        assertNotNull(metadata);
        assertEquals(200, metadata.getStatusCode());
        assertEquals(150L, metadata.getResponseTimeMs());
        assertEquals("input-topic", metadata.getOriginalTopic());
        assertEquals(0, metadata.getOriginalPartition());
        assertEquals(100L, metadata.getOriginalOffset());
    }

    @Test
    void testCreateResponseRecordWithMetadataHeaders() {
        Map<String, String> responseHeaders = new HashMap<>();
        HttpResponse httpResponse = new HttpResponse(201, responseHeaders, "created", 200L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic",
                5,
                null,
                "key456",
                null,
                "value",
                500L,
                1234567890000L,
                null
        );

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Verify metadata headers are present
        assertNotNull(responseRecord.getHeaders());

        boolean foundStatusCode = false;
        boolean foundResponseTime = false;
        boolean foundOriginalTopic = false;
        boolean foundOriginalPartition = false;
        boolean foundOriginalOffset = false;

        for (Header header : responseRecord.getHeaders()) {
            if (ResponseMetadata.HEADER_STATUS_CODE.equals(header.key())) {
                assertEquals(201, header.value());
                foundStatusCode = true;
            } else if (ResponseMetadata.HEADER_RESPONSE_TIME_MS.equals(header.key())) {
                assertEquals(200L, header.value());
                foundResponseTime = true;
            } else if (ResponseMetadata.HEADER_ORIGINAL_TOPIC.equals(header.key())) {
                assertEquals("input-topic", header.value());
                foundOriginalTopic = true;
            } else if (ResponseMetadata.HEADER_ORIGINAL_PARTITION.equals(header.key())) {
                assertEquals(5, header.value());
                foundOriginalPartition = true;
            } else if (ResponseMetadata.HEADER_ORIGINAL_OFFSET.equals(header.key())) {
                assertEquals(500L, header.value());
                foundOriginalOffset = true;
            }
        }

        assertTrue(foundStatusCode, "Status code header not found");
        assertTrue(foundResponseTime, "Response time header not found");
        assertTrue(foundOriginalTopic, "Original topic header not found");
        assertTrue(foundOriginalPartition, "Original partition header not found");
        assertTrue(foundOriginalOffset, "Original offset header not found");
    }

    @Test
    void testCreateResponseRecordWithHttpHeaders() {
        Map<String, String> responseHeaders = new HashMap<>();
        responseHeaders.put("Content-Type", "application/json");
        responseHeaders.put("X-Request-ID", "abc123");
        HttpResponse httpResponse = new HttpResponse(200, responseHeaders, "body", 100L);

        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Verify HTTP response headers are included with "http.response." prefix
        boolean foundContentType = false;
        boolean foundRequestId = false;

        for (Header header : responseRecord.getHeaders()) {
            if ("http.response.Content-Type".equals(header.key())) {
                assertEquals("application/json", header.value());
                foundContentType = true;
            } else if ("http.response.X-Request-ID".equals(header.key())) {
                assertEquals("abc123", header.value());
                foundRequestId = true;
            }
        }

        assertTrue(foundContentType, "Content-Type header not found");
        assertTrue(foundRequestId, "X-Request-ID header not found");
    }

    @Test
    void testCreateResponseRecordWithoutOriginalKey() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_KEY, "false");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNull(responseRecord.getKey(), "Key should be null when includeOriginalKey is false");
    }

    @Test
    void testCreateResponseRecordWithNullBody() {
        HttpResponse httpResponse = new HttpResponse(204, null, null, 50L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNull(responseRecord.getValue(), "Value should be null when response body is null");
    }

    @Test
    void testResolveResponseTopicSimple() {
        SinkRecord record = createSimpleRecord();
        String resolved = handler.resolveResponseTopic(record);
        assertEquals("responses", resolved);
    }

    @Test
    void testResolveResponseTopicWithVariable() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "${topic}-responses");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        SinkRecord record = new SinkRecord(
                "orders",
                0,
                null,
                "key",
                null,
                "value",
                0L
        );

        String resolved = handler.resolveResponseTopic(record);
        assertEquals("orders-responses", resolved);
    }

    @Test
    void testResolveResponseTopicMultipleVariables() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "${topic}-${topic}-responses");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        SinkRecord record = new SinkRecord(
                "test",
                0,
                null,
                "key",
                null,
                "value",
                0L
        );

        String resolved = handler.resolveResponseTopic(record);
        assertEquals("test-test-responses", resolved);
    }

    @Test
    void testCreateResponseRecordWithErrorStatus() {
        HttpResponse httpResponse = new HttpResponse(500, null, "Internal Server Error", 250L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "errors"
        );

        assertNotNull(responseRecord);
        assertEquals(500, responseRecord.getMetadata().getStatusCode());
        assertEquals("errors", responseRecord.getTopic());
    }

    // =============================
    // JSON FORMAT VALIDATION TESTS
    // =============================

    @Test
    void testStringFormatWithJsonBody() {
        // Default format is "string" - should work with JSON content
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "string");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{\"status\":\"ok\"}", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("{\"status\":\"ok\"}", new String(responseRecord.getValue()));
    }

    @Test
    void testStringFormatWithHtmlBody() {
        // String format should work with any content type
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "string");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "<html><body>Hello</body></html>", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("<html><body>Hello</body></html>", new String(responseRecord.getValue()));
    }

    @Test
    void testStringFormatWithPlainText() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "string");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "Plain text response", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("Plain text response", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithValidJsonObject() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{\"status\":\"ok\",\"count\":42}", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("{\"status\":\"ok\",\"count\":42}", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithValidJsonArray() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "[1,2,3,4,5]", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("[1,2,3,4,5]", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithNestedJson() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        String nestedJson = "{\"user\":{\"id\":123,\"name\":\"test\",\"tags\":[\"a\",\"b\"]}}";
        HttpResponse httpResponse = new HttpResponse(200, null, nestedJson, 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals(nestedJson, new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithEmptyObject() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{}", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("{}", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithEmptyArray() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "[]", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("[]", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithInvalidJsonPlainText() {
        // JSON format should still create record but log error for invalid JSON
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "Not JSON", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Should still create record with fallback to string format
        assertNotNull(responseRecord.getValue());
        assertEquals("Not JSON", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithInvalidJsonHtml() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "<html><body>Error</body></html>", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Should still create record with fallback to string format
        assertNotNull(responseRecord.getValue());
        assertEquals("<html><body>Error</body></html>", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithMalformedJson() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{\"status\":\"ok\"", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Should still create record with fallback to string format
        assertNotNull(responseRecord.getValue());
        assertEquals("{\"status\":\"ok\"", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithInvalidJsonUnquotedKeys() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{status: ok}", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Should still create record with fallback to string format
        assertNotNull(responseRecord.getValue());
        assertEquals("{status: ok}", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatWithNullBody() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(204, null, null, 50L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNull(responseRecord.getValue(), "Value should be null when response body is null");
    }

    @Test
    void testDefaultFormatIsString() {
        // When no format is specified, should default to "string"
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        // Not setting RESPONSE_VALUE_FORMAT

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        assertEquals("string", config.getResponseValueFormat());
    }

    @Test
    void testJsonFormatCaseInsensitive() {
        // Test that format is case insensitive
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "JSON");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{\"status\":\"ok\"}", 100L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        assertNotNull(responseRecord.getValue());
        assertEquals("{\"status\":\"ok\"}", new String(responseRecord.getValue()));
    }

    @Test
    void testJsonFormatHeadersUnchanged() {
        // Verify that JSON format doesn't change headers structure
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_VALUE_FORMAT, "json");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_REQUEST_METADATA, "true");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "{\"status\":\"ok\"}", 150L);
        SinkRecord originalRecord = createSimpleRecord();

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse,
                originalRecord,
                "responses"
        );

        // Verify metadata headers are still present
        boolean foundStatusCode = false;
        boolean foundResponseTime = false;

        for (Header header : responseRecord.getHeaders()) {
            if (ResponseMetadata.HEADER_STATUS_CODE.equals(header.key())) {
                assertEquals(200, header.value());
                foundStatusCode = true;
            } else if (ResponseMetadata.HEADER_RESPONSE_TIME_MS.equals(header.key())) {
                assertEquals(150L, header.value());
                foundResponseTime = true;
            }
        }

        assertTrue(foundStatusCode, "Status code header should be present in JSON format");
        assertTrue(foundResponseTime, "Response time header should be present in JSON format");
    }

    // =============================
    // ORIGINAL HEADER FILTERING TESTS
    // =============================

    @Test
    void testOriginalHeadersDefaultIncludeAll() {
        // When include list is empty, all original headers should be forwarded
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        // Create record with multiple headers
        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");
        originalRecord.headers().addString("requestId", "req789");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify all headers are present
        assertTrue(hasHeader(responseRecord, "traceId"));
        assertTrue(hasHeader(responseRecord, "userId"));
        assertTrue(hasHeader(responseRecord, "requestId"));
    }

    @Test
    void testOriginalHeadersIncludeListSingle() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "traceId");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");
        originalRecord.headers().addString("requestId", "req789");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify only traceId is present
        assertTrue(hasHeader(responseRecord, "traceId"));
        assertFalse(hasHeader(responseRecord, "userId"));
        assertFalse(hasHeader(responseRecord, "requestId"));
    }

    @Test
    void testOriginalHeadersIncludeListMultiple() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "traceId,requestId");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");
        originalRecord.headers().addString("requestId", "req789");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify only traceId and requestId are present
        assertTrue(hasHeader(responseRecord, "traceId"));
        assertFalse(hasHeader(responseRecord, "userId"));
        assertTrue(hasHeader(responseRecord, "requestId"));
    }

    @Test
    void testOriginalHeadersIncludeListWithWhitespace() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, " traceId , requestId ");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify whitespace is trimmed
        assertTrue(hasHeader(responseRecord, "traceId"));
        assertFalse(hasHeader(responseRecord, "userId"));
    }

    @Test
    void testOriginalHeadersIncludeListNonExistent() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "nonExistent");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify no original headers are present (non-existent header in list)
        assertFalse(hasHeader(responseRecord, "traceId"));
        assertFalse(hasHeader(responseRecord, "userId"));
    }

    @Test
    void testOriginalHeadersDisabledIgnoresIncludeList() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "false");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "traceId");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 100L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify no original headers when disabled
        assertFalse(hasHeader(responseRecord, "traceId"));
    }

    @Test
    void testOriginalHeadersIncludeListDoesNotAffectMetadata() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.RESPONSE_TOPIC_NAME, "responses");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_ORIGINAL_HEADERS, "true");
        props.put(HttpSinkConnectorConfig.RESPONSE_ORIGINAL_HEADERS_INCLUDE, "traceId");
        props.put(HttpSinkConnectorConfig.RESPONSE_INCLUDE_REQUEST_METADATA, "true");

        handler = new ResponseHandler(new HttpSinkConnectorConfig(props));

        HttpResponse httpResponse = new HttpResponse(200, null, "body", 150L);

        SinkRecord originalRecord = new SinkRecord(
                "input-topic", 0, null, "key", null, "value", 100L, 1234567890000L, null
        );
        originalRecord.headers().addString("traceId", "trace123");
        originalRecord.headers().addString("userId", "user456");

        ResponseRecord responseRecord = handler.createResponseRecord(
                httpResponse, originalRecord, "responses"
        );

        // Verify metadata headers are present regardless of include list
        boolean foundStatusCode = false;
        boolean foundResponseTime = false;

        for (Header header : responseRecord.getHeaders()) {
            if (ResponseMetadata.HEADER_STATUS_CODE.equals(header.key())) {
                foundStatusCode = true;
            } else if (ResponseMetadata.HEADER_RESPONSE_TIME_MS.equals(header.key())) {
                foundResponseTime = true;
            }
        }

        assertTrue(foundStatusCode);
        assertTrue(foundResponseTime);

        // Verify only traceId from original headers
        assertTrue(hasHeader(responseRecord, "traceId"));
        assertFalse(hasHeader(responseRecord, "userId"));
    }

    // Helper methods
    private boolean hasHeader(ResponseRecord record, String headerName) {
        for (Header header : record.getHeaders()) {
            if (header.key().equals(headerName)) {
                return true;
            }
        }
        return false;
    }

    // Helper method
    private SinkRecord createSimpleRecord() {
        return new SinkRecord(
                "input-topic",
                0,
                null,
                "test-key",
                null,
                "test-value",
                100L,
                1234567890000L,
                null
        );
    }
}
