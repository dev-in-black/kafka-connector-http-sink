package com.devinblack.kafka.connect.http.error;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages the Kafka Producer lifecycle for sending error records to Kafka error topics.
 *
 * This producer is separate from the main connector flow and is used exclusively
 * for publishing failed records to error topics.
 *
 * Features:
 * - Fire-and-forget async sends (never blocks)
 * - Simplified reliability config (lower acks, fewer retries)
 * - Never throws exceptions (prevents infinite error loops)
 * - Proper error handling and logging
 */
public class ErrorProducer {
    private static final Logger log = LoggerFactory.getLogger(ErrorProducer.class);

    private final HttpSinkConnectorConfig config;
    private Producer<String, String> producer;

    public ErrorProducer(HttpSinkConnectorConfig config) {
        this.config = config;
        initProducer();
    }

    /**
     * Initialize the Kafka Producer with error-specific configurations.
     */
    private void initProducer() {
        Map<String, Object> producerProps = new HashMap<>();

        // Copy Kafka Connect worker configs (bootstrap.servers, etc.)
        producerProps.putAll(config.originalsWithPrefix(""));

        // Producer-specific overrides
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Set client ID for tracking
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "http-sink-error-producer");

        // Configure for speed over reliability (errors are best-effort)
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1"); // Only wait for leader
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1); // Only retry once
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // Allow more parallelism

        // Shorter timeout configurations
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000); // 10 seconds
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000); // 30 seconds

        // Compression
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        log.info("Initializing ErrorProducer with bootstrap.servers: {}",
                producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        this.producer = new KafkaProducer<>(producerProps);

        log.info("ErrorProducer initialized successfully");
    }

    /**
     * Send an error record to Kafka asynchronously (fire-and-forget).
     *
     * This method returns immediately without waiting for acknowledgment.
     * CRITICAL: This method NEVER throws exceptions to prevent infinite error loops.
     *
     * @param errorRecord The error record to send
     */
    public void sendAsync(ErrorRecord errorRecord) {
        try {
            ProducerRecord<String, String> producerRecord = toProducerRecord(errorRecord);

            log.debug("Sending error record asynchronously to topic={}, key={}",
                    producerRecord.topic(), producerRecord.key());

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    // CRITICAL: Log but NEVER throw - prevents infinite error loops
                    log.error("Failed to send error record to topic={}: {}",
                            producerRecord.topic(), exception.getMessage(), exception);
                } else {
                    log.debug("Error record sent successfully: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            // CRITICAL: Catch any exception to prevent cascading failures
            log.error("Exception while sending error record: {}", e.getMessage(), e);
            // Do NOT throw - this is a best-effort operation
        }
    }

    /**
     * Convert ErrorRecord to Kafka ProducerRecord.
     */
    private ProducerRecord<String, String> toProducerRecord(ErrorRecord errorRecord) {
        String topic = errorRecord.getTopic();
        String key = errorRecord.getKey() != null ? errorRecord.getKey().toString() : null;

        // Convert byte[] value to String using UTF-8 encoding
        byte[] valueBytes = errorRecord.getValue();
        String value = valueBytes != null ? new String(valueBytes, StandardCharsets.UTF_8) : null;

        // Convert Connect Headers to Kafka Headers
        List<Header> kafkaHeaders = new ArrayList<>();
        if (errorRecord.getHeaders() != null) {
            errorRecord.getHeaders().forEach(header -> {
                kafkaHeaders.add(new org.apache.kafka.common.header.internals.RecordHeader(
                        header.key(),
                        convertHeaderValue(header)
                ));
            });
        }

        return new ProducerRecord<>(
                topic,
                null, // partition - let Kafka decide based on key
                key,
                value,
                kafkaHeaders
        );
    }

    /**
     * Convert Connect header value to byte array using UTF-8 encoding.
     * All values are serialized as strings (converted to UTF-8 bytes).
     */
    private byte[] convertHeaderValue(org.apache.kafka.connect.header.Header header) {
        Object value = header.value();
        if (value == null) {
            return null;
        }

        // All header values are serialized as UTF-8 strings
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Number) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Boolean) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        } else {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Flush all pending sends.
     *
     * Blocks until all asynchronous sends are completed or timeout is reached.
     */
    public void flush() {
        if (producer != null) {
            try {
                log.debug("Flushing ErrorProducer");
                producer.flush();
                log.debug("ErrorProducer flushed successfully");
            } catch (Exception e) {
                // Log but don't throw - flush is best-effort
                log.error("Error flushing ErrorProducer: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Close the producer and release resources.
     *
     * This should be called when the connector is stopped.
     */
    public void close() {
        if (producer != null) {
            log.info("Closing ErrorProducer");
            try {
                producer.close(Duration.ofSeconds(10));
                log.info("ErrorProducer closed successfully");
            } catch (Exception e) {
                log.error("Error closing ErrorProducer", e);
            } finally {
                producer = null;
            }
        }
    }
}
