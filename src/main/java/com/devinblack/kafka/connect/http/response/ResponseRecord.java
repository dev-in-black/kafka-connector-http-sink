package com.devinblack.kafka.connect.http.response;

import org.apache.kafka.connect.header.Headers;

/**
 * Represents a response record to be sent to Kafka.
 *
 * This is a simple POJO that encapsulates all the data needed to produce
 * a Kafka record from an HTTP response.
 */
public class ResponseRecord {
    private final String topic;
    private final Object key;
    private final byte[] value;
    private final Headers headers;
    private final ResponseMetadata metadata;

    public ResponseRecord(
            String topic,
            Object key,
            byte[] value,
            Headers headers,
            ResponseMetadata metadata) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.metadata = metadata;
    }

    public String getTopic() {
        return topic;
    }

    public Object getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public Headers getHeaders() {
        return headers;
    }

    public ResponseMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "ResponseRecord{" +
                "topic='" + topic + '\'' +
                ", key=" + key +
                ", valueLength=" + (value != null ? value.length : 0) +
                ", headersCount=" + countHeaders() +
                ", metadata=" + metadata +
                '}';
    }

    private int countHeaders() {
        if (headers == null) {
            return 0;
        }
        int count = 0;
        for (@SuppressWarnings("unused") org.apache.kafka.connect.header.Header h : headers) {
            count++;
        }
        return count;
    }
}
