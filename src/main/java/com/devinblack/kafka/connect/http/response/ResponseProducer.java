package com.devinblack.kafka.connect.http.response;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manages the Kafka Producer lifecycle for sending HTTP responses to Kafka topics.
 *
 * This producer is separate from the Kafka Connect framework's consumer and is used
 * exclusively for publishing HTTP responses back to Kafka.
 *
 * Features:
 * - Manages producer lifecycle (init, flush, close)
 * - Handles both synchronous and asynchronous sends
 * - Configurable acks, retries, and timeouts
 * - Proper error handling and logging
 */
public class ResponseProducer {
    private static final Logger log = LoggerFactory.getLogger(ResponseProducer.class);

    private final HttpSinkConnectorConfig config;
    private Producer<String, byte[]> producer;
    private final Duration sendTimeout;

    public ResponseProducer(HttpSinkConnectorConfig config) {
        this.config = config;
        this.sendTimeout = Duration.ofMillis(30000); // 30 seconds default timeout
        initProducer();
    }

    /**
     * Initialize the Kafka Producer with appropriate configurations.
     */
    private void initProducer() {
        Map<String, Object> producerProps = new HashMap<>();

        // Copy Kafka Connect worker configs (bootstrap.servers, etc.)
        // These are typically passed through from the worker config
        producerProps.putAll(config.originalsWithPrefix(""));

        // Producer-specific overrides
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Set client ID for tracking
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "http-sink-response-producer");

        // Configure for reliability
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3); // Retry up to 3 times
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Ensure ordering
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Exactly-once semantics

        // Timeout configurations
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);

        // Compression
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        log.info("Initializing ResponseProducer with bootstrap.servers: {}",
                producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        this.producer = new KafkaProducer<>(producerProps);

        log.info("ResponseProducer initialized successfully");
    }

    /**
     * Send a response record to Kafka synchronously.
     *
     * This method blocks until the record is acknowledged by Kafka or times out.
     *
     * @param responseRecord The response record to send
     * @return RecordMetadata containing information about the sent record
     * @throws ConnectException if sending fails
     */
    public RecordMetadata send(ResponseRecord responseRecord) {
        ProducerRecord<String, byte[]> producerRecord = toProducerRecord(responseRecord);

        log.debug("Sending response to topic={}, partition=calculated, key={}",
                producerRecord.topic(), producerRecord.key());

        try {
            // Send and wait for acknowledgment
            Future<RecordMetadata> future = producer.send(producerRecord);
            RecordMetadata metadata = future.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

            log.debug("Response sent successfully: topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());

            return metadata;

        } catch (ExecutionException e) {
            log.error("Failed to send response to topic={}: {}",
                    producerRecord.topic(), e.getCause().getMessage(), e.getCause());
            throw new ConnectException("Failed to send response to Kafka", e.getCause());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sending response to topic={}", producerRecord.topic(), e);
            throw new ConnectException("Interrupted while sending response", e);

        } catch (TimeoutException e) {
            log.error("Timeout while sending response to topic={} after {}ms",
                    producerRecord.topic(), sendTimeout.toMillis(), e);
            throw new ConnectException("Timeout while sending response", e);
        }
    }

    /**
     * Send a response record to Kafka asynchronously.
     *
     * This method returns immediately without waiting for acknowledgment.
     * Use flush() to ensure all async sends are completed.
     *
     * @param responseRecord The response record to send
     */
    public void sendAsync(ResponseRecord responseRecord) {
        ProducerRecord<String, byte[]> producerRecord = toProducerRecord(responseRecord);

        log.debug("Sending response asynchronously to topic={}, key={}",
                producerRecord.topic(), producerRecord.key());

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send response to topic={}: {}",
                        producerRecord.topic(), exception.getMessage(), exception);
            } else {
                log.debug("Response sent successfully: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Convert ResponseRecord to Kafka ProducerRecord.
     */
    private ProducerRecord<String, byte[]> toProducerRecord(ResponseRecord responseRecord) {
        String topic = responseRecord.getTopic();
        String key = responseRecord.getKey() != null ? responseRecord.getKey().toString() : null;
        byte[] value = responseRecord.getValue();

        // Convert Connect Headers to Kafka Headers
        List<Header> kafkaHeaders = new ArrayList<>();
        if (responseRecord.getHeaders() != null) {
            responseRecord.getHeaders().forEach(header -> {
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
     * Convert Connect header value to byte array.
     */
    private byte[] convertHeaderValue(org.apache.kafka.connect.header.Header header) {
        Object value = header.value();
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof String) {
            return ((String) value).getBytes();
        } else if (value instanceof Number) {
            return value.toString().getBytes();
        } else if (value instanceof Boolean) {
            return value.toString().getBytes();
        } else {
            return value.toString().getBytes();
        }
    }

    /**
     * Flush all pending sends.
     *
     * Blocks until all asynchronous sends are completed or timeout is reached.
     */
    public void flush() {
        if (producer != null) {
            log.debug("Flushing ResponseProducer");
            producer.flush();
            log.debug("ResponseProducer flushed successfully");
        }
    }

    /**
     * Close the producer and release resources.
     *
     * This should be called when the connector is stopped.
     */
    public void close() {
        if (producer != null) {
            log.info("Closing ResponseProducer");
            try {
                producer.close(Duration.ofSeconds(10));
                log.info("ResponseProducer closed successfully");
            } catch (Exception e) {
                log.error("Error closing ResponseProducer", e);
            } finally {
                producer = null;
            }
        }
    }
}
