package com.devinblack.kafka.connect.http.response;

/**
 * Metadata about an HTTP response to be included in response topic headers.
 *
 * These metadata headers help track the relationship between the original request
 * and the HTTP response.
 */
public class ResponseMetadata {

    // Header names for metadata
    public static final String HEADER_STATUS_CODE = "http.status.code";
    public static final String HEADER_RESPONSE_TIME_MS = "http.response.time.ms";
    public static final String HEADER_ORIGINAL_TOPIC = "kafka.original.topic";
    public static final String HEADER_ORIGINAL_PARTITION = "kafka.original.partition";
    public static final String HEADER_ORIGINAL_OFFSET = "kafka.original.offset";
    public static final String HEADER_TIMESTAMP = "kafka.timestamp";

    private final int statusCode;
    private final long responseTimeMs;
    private final String originalTopic;
    private final int originalPartition;
    private final long originalOffset;
    private final long timestamp;

    public ResponseMetadata(
            int statusCode,
            long responseTimeMs,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            long timestamp) {
        this.statusCode = statusCode;
        this.responseTimeMs = responseTimeMs;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.timestamp = timestamp;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getResponseTimeMs() {
        return responseTimeMs;
    }

    public String getOriginalTopic() {
        return originalTopic;
    }

    public int getOriginalPartition() {
        return originalPartition;
    }

    public long getOriginalOffset() {
        return originalOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ResponseMetadata{" +
                "statusCode=" + statusCode +
                ", responseTimeMs=" + responseTimeMs +
                ", originalTopic='" + originalTopic + '\'' +
                ", originalPartition=" + originalPartition +
                ", originalOffset=" + originalOffset +
                ", timestamp=" + timestamp +
                '}';
    }
}
