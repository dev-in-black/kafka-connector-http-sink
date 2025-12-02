package com.devinblack.kafka.connect.http.response;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import com.devinblack.kafka.connect.http.client.HttpResponse;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Handles mapping HTTP responses to Kafka producer records.
 *
 * Creates Kafka records with:
 * - Response body as message value
 * - Response headers as message headers
 * - Metadata headers (status code, response time, original topic/partition/offset)
 * - Optionally preserves original record key and headers
 */
public class ResponseHandler {
    private static final Logger log = LoggerFactory.getLogger(ResponseHandler.class);

    private final HttpSinkConnectorConfig config;
    private final boolean includeOriginalKey;
    private final boolean includeOriginalHeaders;
    private final boolean includeRequestMetadata;

    public ResponseHandler(HttpSinkConnectorConfig config) {
        this.config = config;
        this.includeOriginalKey = config.isResponseIncludeOriginalKey();
        this.includeOriginalHeaders = config.isResponseIncludeOriginalHeaders();
        this.includeRequestMetadata = config.isResponseIncludeRequestMetadata();

        log.info("ResponseHandler initialized: includeOriginalKey={}, includeOriginalHeaders={}, includeRequestMetadata={}",
                includeOriginalKey, includeOriginalHeaders, includeRequestMetadata);
    }

    /**
     * Create a ProducerRecord from an HTTP response and the original Kafka record.
     *
     * @param response The HTTP response
     * @param originalRecord The original Kafka sink record
     * @param responseTopic The target response topic
     * @return A ResponseRecord ready to be sent to Kafka
     */
    public ResponseRecord createResponseRecord(
            HttpResponse response,
            SinkRecord originalRecord,
            String responseTopic) {

        // Build headers
        Headers headers = buildHeaders(response, originalRecord);

        // Determine key
        Object key = includeOriginalKey ? originalRecord.key() : null;

        // Use response body as value (as byte array for flexibility)
        byte[] value = response.getBody() != null
                ? response.getBody().getBytes(StandardCharsets.UTF_8)
                : null;

        // Create metadata
        ResponseMetadata metadata = new ResponseMetadata(
                response.getStatusCode(),
                response.getResponseTimeMs(),
                originalRecord.topic(),
                originalRecord.kafkaPartition(),
                originalRecord.kafkaOffset(),
                originalRecord.timestamp() != null ? originalRecord.timestamp() : System.currentTimeMillis()
        );

        log.debug("Created response record for topic={}, statusCode={}, responseTime={}ms",
                responseTopic, response.getStatusCode(), response.getResponseTimeMs());

        return new ResponseRecord(responseTopic, key, value, headers, metadata);
    }

    /**
     * Build headers for the response record.
     *
     * Includes:
     * 1. HTTP response headers (if configured)
     * 2. Metadata headers (status code, response time, etc.)
     * 3. Original record headers (if configured)
     */
    private Headers buildHeaders(HttpResponse response, SinkRecord originalRecord) {
        Headers headers = new ConnectHeaders();

        // 1. Add original headers if configured
        if (includeOriginalHeaders && originalRecord.headers() != null) {
            originalRecord.headers().forEach(header -> {
                headers.add(header);
                log.trace("Forwarding original header: {}", header.key());
            });
        }

        // 2. Add HTTP response headers
        if (response.getHeaders() != null) {
            for (Map.Entry<String, String> entry : response.getHeaders().entrySet()) {
                // Prefix with "http." to distinguish from original headers
                String headerName = "http.response." + entry.getKey();
                headers.addString(headerName, entry.getValue());
                log.trace("Adding HTTP response header: {} = {}", headerName, entry.getValue());
            }
        }

        // 3. Add metadata headers if configured
        if (includeRequestMetadata) {
            headers.addInt(ResponseMetadata.HEADER_STATUS_CODE, response.getStatusCode());
            headers.addLong(ResponseMetadata.HEADER_RESPONSE_TIME_MS, response.getResponseTimeMs());
            headers.addString(ResponseMetadata.HEADER_ORIGINAL_TOPIC, originalRecord.topic());
            headers.addInt(ResponseMetadata.HEADER_ORIGINAL_PARTITION, originalRecord.kafkaPartition());
            headers.addLong(ResponseMetadata.HEADER_ORIGINAL_OFFSET, originalRecord.kafkaOffset());

            if (originalRecord.timestamp() != null) {
                headers.addLong(ResponseMetadata.HEADER_TIMESTAMP, originalRecord.timestamp());
            }

            log.trace("Added metadata headers: statusCode={}, responseTime={}ms, originalTopic={}",
                    response.getStatusCode(), response.getResponseTimeMs(), originalRecord.topic());
        }

        return headers;
    }

    /**
     * Resolve the response topic name, supporting ${topic} variable substitution.
     *
     * @param originalRecord The original Kafka sink record
     * @return The resolved response topic name
     */
    public String resolveResponseTopic(SinkRecord originalRecord) {
        String topicTemplate = config.getResponseTopicName();

        if (topicTemplate == null || topicTemplate.isEmpty()) {
            throw new IllegalStateException("Response topic name is not configured");
        }

        // Support ${topic} variable substitution
        String resolvedTopic = topicTemplate.replace("${topic}", originalRecord.topic());

        log.trace("Resolved response topic: {} -> {}", topicTemplate, resolvedTopic);

        return resolvedTopic;
    }
}
