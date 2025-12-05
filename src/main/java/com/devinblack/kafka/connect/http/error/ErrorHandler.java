package com.devinblack.kafka.connect.http.error;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import com.devinblack.kafka.connect.http.client.HttpResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Handles mapping processing errors to Kafka error records.
 *
 * Creates Kafka records with:
 * - Error details as JSON message value
 * - Error metadata as message headers
 * - HTTP response details (if applicable)
 * - Original record context (topic, partition, offset)
 */
public class ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    // Header names for error metadata
    public static final String HEADER_ERROR_TYPE = "error.type";
    public static final String HEADER_ERROR_MESSAGE = "error.message";
    public static final String HEADER_ERROR_TIMESTAMP = "error.timestamp";
    public static final String HEADER_HTTP_STATUS_CODE = "error.http.status.code";
    public static final String HEADER_RETRY_COUNT = "error.retry.count";
    public static final String HEADER_ORIGINAL_TOPIC = "kafka.original.topic";
    public static final String HEADER_ORIGINAL_PARTITION = "kafka.original.partition";
    public static final String HEADER_ORIGINAL_OFFSET = "kafka.original.offset";

    private final HttpSinkConnectorConfig config;
    private final ObjectMapper objectMapper;

    public ErrorHandler(HttpSinkConnectorConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();

        log.info("ErrorHandler initialized for error topic publishing");
    }

    /**
     * Create an error record from a failed processing attempt.
     *
     * @param originalRecord The original Kafka sink record that failed
     * @param errorType The type of error (HTTP_ERROR, RETRY_EXHAUSTED, CONVERSION_ERROR, NULL_VALUE)
     * @param errorMessage The error message
     * @param httpResponse The HTTP response (if applicable, can be null)
     * @param retryCount The number of retry attempts
     * @return An ErrorRecord ready to be sent to Kafka
     */
    public ErrorRecord createErrorRecord(
            SinkRecord originalRecord,
            String errorType,
            String errorMessage,
            HttpResponse httpResponse,
            int retryCount) {

        // Resolve the error topic name
        String errorTopic = resolveErrorTopic(originalRecord);

        // Build headers with error metadata
        Headers headers = buildErrorHeaders(
                errorType,
                errorMessage,
                httpResponse,
                retryCount,
                originalRecord
        );

        // Determine key (use original record key)
        Object key = originalRecord.key();

        // Build JSON error payload
        byte[] value = buildErrorJson(
                errorType,
                errorMessage,
                httpResponse,
                retryCount,
                originalRecord
        );

        log.debug("Created error record for topic={}, errorType={}, originalTopic={}, partition={}, offset={}",
                errorTopic, errorType, originalRecord.topic(),
                originalRecord.kafkaPartition(), originalRecord.kafkaOffset());

        return new ErrorRecord(errorTopic, key, value, headers);
    }

    /**
     * Resolve the error topic name, supporting ${topic} variable substitution.
     *
     * @param originalRecord The original Kafka sink record
     * @return The resolved error topic name
     */
    public String resolveErrorTopic(SinkRecord originalRecord) {
        String topicTemplate = config.getErrorTopicName();

        if (topicTemplate == null || topicTemplate.isEmpty()) {
            throw new IllegalStateException("Error topic name is not configured");
        }

        // Support ${topic} variable substitution
        String resolvedTopic = topicTemplate.replace("${topic}", originalRecord.topic());

        log.trace("Resolved error topic: {} -> {}", topicTemplate, resolvedTopic);

        return resolvedTopic;
    }

    /**
     * Build headers for the error record.
     *
     * Includes:
     * 1. Original record headers (forwarded from source)
     * 2. HTTP response headers (if applicable)
     * 3. Error metadata headers (error type, message, timestamp, etc.)
     */
    private Headers buildErrorHeaders(
            String errorType,
            String errorMessage,
            HttpResponse httpResponse,
            int retryCount,
            SinkRecord originalRecord) {

        Headers headers = new ConnectHeaders();

        // 1. Add original record headers
        if (originalRecord.headers() != null) {
            originalRecord.headers().forEach(header -> {
                headers.add(header);
                log.trace("Forwarding original header: {}", header.key());
            });
        }

        // 2. Add HTTP response headers (if applicable)
        if (httpResponse != null && httpResponse.getHeaders() != null) {
            for (Map.Entry<String, String> entry : httpResponse.getHeaders().entrySet()) {
                // Prefix with "http.response." to distinguish from original headers
                String headerName = "http.response." + entry.getKey();
                headers.addString(headerName, entry.getValue());
                log.trace("Adding HTTP response header: {} = {}", headerName, entry.getValue());
            }
        }

        // 3. Add error metadata headers
        headers.addString(HEADER_ERROR_TYPE, errorType);
        headers.addString(HEADER_ERROR_MESSAGE, errorMessage);
        headers.addLong(HEADER_ERROR_TIMESTAMP, System.currentTimeMillis());

        if (httpResponse != null) {
            headers.addInt(HEADER_HTTP_STATUS_CODE, httpResponse.getStatusCode());
        }

        if (retryCount > 0) {
            headers.addInt(HEADER_RETRY_COUNT, retryCount);
        }

        // Add original record context
        headers.addString(HEADER_ORIGINAL_TOPIC, originalRecord.topic());
        headers.addInt(HEADER_ORIGINAL_PARTITION, originalRecord.kafkaPartition());
        headers.addLong(HEADER_ORIGINAL_OFFSET, originalRecord.kafkaOffset());

        log.trace("Added error headers: errorType={}, statusCode={}, retryCount={}, originalTopic={}",
                errorType, httpResponse != null ? httpResponse.getStatusCode() : "N/A", retryCount,
                originalRecord.topic());

        return headers;
    }

    /**
     * Build JSON error payload.
     *
     * Format:
     * {
     *   "errorType": "HTTP_ERROR",
     *   "errorMessage": "HTTP request returned status 500",
     *   "errorTimestamp": 1701728400000,
     *   "retryCount": 5,
     *   "httpStatusCode": 500,
     *   "httpResponseBody": "{\"error\": \"Internal Server Error\"}",
     *   "originalTopic": "source-topic",
     *   "originalPartition": 2,
     *   "originalOffset": 12345
     * }
     */
    private byte[] buildErrorJson(
            String errorType,
            String errorMessage,
            HttpResponse httpResponse,
            int retryCount,
            SinkRecord originalRecord) {

        try {
            ObjectNode errorJson = objectMapper.createObjectNode();

            // Add error information
            errorJson.put("errorType", errorType);
            errorJson.put("errorMessage", errorMessage);
            errorJson.put("errorTimestamp", System.currentTimeMillis());

            if (retryCount > 0) {
                errorJson.put("retryCount", retryCount);
            }

            // Add HTTP response details if available
            if (httpResponse != null) {
                errorJson.put("httpStatusCode", httpResponse.getStatusCode());
                if (httpResponse.getBody() != null) {
                    errorJson.put("httpResponseBody", httpResponse.getBody());
                }
            }

            // Add original record context
            errorJson.put("originalTopic", originalRecord.topic());
            errorJson.put("originalPartition", originalRecord.kafkaPartition());
            errorJson.put("originalOffset", originalRecord.kafkaOffset());

            String jsonString = objectMapper.writeValueAsString(errorJson);
            return jsonString.getBytes(StandardCharsets.UTF_8);

        } catch (Exception e) {
            log.error("Failed to build error JSON: {}", e.getMessage(), e);
            // Fallback to simple error message
            String fallback = String.format("{\"errorType\":\"%s\",\"errorMessage\":\"%s\"}",
                    errorType, errorMessage.replace("\"", "\\\""));
            return fallback.getBytes(StandardCharsets.UTF_8);
        }
    }
}
