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
