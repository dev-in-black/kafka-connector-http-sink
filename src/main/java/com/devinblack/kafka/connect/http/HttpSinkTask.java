package com.devinblack.kafka.connect.http;

import com.devinblack.kafka.connect.http.auth.AuthenticationProvider;
import com.devinblack.kafka.connect.http.auth.AuthenticationProviderFactory;
import com.devinblack.kafka.connect.http.client.HttpClient;
import com.devinblack.kafka.connect.http.client.HttpRequestBuilder;
import com.devinblack.kafka.connect.http.client.HttpResponse;
import com.devinblack.kafka.connect.http.response.ResponseHandler;
import com.devinblack.kafka.connect.http.response.ResponseProducer;
import com.devinblack.kafka.connect.http.response.ResponseRecord;
import com.devinblack.kafka.connect.http.retry.ExponentialBackoffRetry;
import com.devinblack.kafka.connect.http.retry.RetryPolicy;
import okhttp3.Request;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * HttpSinkTask processes records from Kafka and sends them to HTTP endpoints.
 *
 * Processing flow:
 * 1. Receive batch of records
 * 2. For each record (one at a time - no batching):
 *    a. Convert record to HTTP request
 *    b. Forward Kafka headers to HTTP headers
 *    c. Add authentication headers
 *    d. Send HTTP request
 *    e. Handle response
 *    f. Send response to Kafka response topic (if enabled)
 * 3. Commit offsets after flush
 */
public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private HttpSinkConnectorConfig config;

    // Core components
    private HttpClient httpClient;
    private AuthenticationProvider authProvider;
    private RecordConverter recordConverter;
    private HeaderConverter headerConverter;
    private ResponseProducer responseProducer;
    private ResponseHandler responseHandler;
    private RetryPolicy retryPolicy;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HTTP Sink Task");
        this.config = new HttpSinkConnectorConfig(props);

        // Initialize HTTP client
        log.debug("Initializing HTTP client");
        this.httpClient = new HttpClient(config);

        // Initialize authentication provider
        log.debug("Initializing authentication provider");
        this.authProvider = AuthenticationProviderFactory.create(config);
        this.authProvider.authenticate(); // Perform initial auth (important for OAuth2)

        // Initialize record and header converters
        log.debug("Initializing converters");
        this.recordConverter = new RecordConverter(config);
        this.headerConverter = new HeaderConverter(config);

        // Initialize retry policy (if enabled)
        if (config.isRetryEnabled()) {
            log.debug("Initializing retry policy");
            this.retryPolicy = new ExponentialBackoffRetry(config);
        }

        // Initialize response handler and producer (if enabled)
        if (config.isResponseTopicEnabled()) {
            log.debug("Initializing response handler and producer");
            this.responseHandler = new ResponseHandler(config);
            this.responseProducer = new ResponseProducer(config);
        }

        log.info("HTTP Sink Task started successfully");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.debug("Processing {} records", records.size());

        for (SinkRecord record : records) {
            processRecord(record);
        }
    }

    private void processRecord(SinkRecord record) {
        log.debug("Processing record from topic={} partition={} offset={}",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());

        try {
            // 1. Check for null values
            if (record.value() == null) {
                handleNullValue(record);
                return;
            }

            // 2. Convert record to HTTP request
            Request request = buildHttpRequest(record);

            // 3. Execute request with retry logic
            HttpResponse response = executeWithRetry(request, record);

            log.debug("HTTP request completed: status={}, responseTime={}ms",
                    response.getStatusCode(), response.getResponseTimeMs());

            // 4. Handle response - send to response topic if enabled
            if (config.isResponseTopicEnabled() && responseHandler != null && responseProducer != null) {
                sendToResponseTopic(response, record);
            }

            // 5. Check if response indicates an error
            if (response.isError()) {
                handleHttpError(response, record);
            }

            log.debug("Record processed successfully: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());

        } catch (RecordConverter.ConversionException e) {
            log.error("Failed to convert record: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            throw new RuntimeException("Record conversion failed", e);

        } catch (Exception e) {
            log.error("Error processing record: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            throw new RuntimeException("Failed to process record", e);
        }
    }

    /**
     * Execute HTTP request with retry logic.
     */
    private HttpResponse executeWithRetry(Request request, SinkRecord record) throws Exception {
        // If retry is not enabled, execute once
        if (retryPolicy == null || !config.isRetryEnabled()) {
            return httpClient.execute(request);
        }

        // Execute with retry
        int attemptNumber = 0;
        Exception lastException = null;
        HttpResponse lastResponse = null;

        while (true) {
            try {
                log.debug("Executing HTTP request (attempt {}) for topic={} partition={} offset={}",
                        attemptNumber + 1, record.topic(), record.kafkaPartition(), record.kafkaOffset());

                HttpResponse response = httpClient.execute(request);

                // Check if response status indicates we should retry
                if (response.isError() && retryPolicy.shouldRetry(response.getStatusCode())) {
                    lastResponse = response;

                    if (retryPolicy.hasMoreAttempts(attemptNumber)) {
                        long delayMs = retryPolicy.getDelayMs(attemptNumber);
                        log.warn("HTTP request failed with status {} (attempt {}), retrying in {}ms: topic={} partition={} offset={}",
                                response.getStatusCode(), attemptNumber + 1, delayMs,
                                record.topic(), record.kafkaPartition(), record.kafkaOffset());

                        Thread.sleep(delayMs);
                        attemptNumber++;
                        continue;
                    } else {
                        log.error("HTTP request failed with status {} after {} attempts: topic={} partition={} offset={}",
                                response.getStatusCode(), attemptNumber + 1,
                                record.topic(), record.kafkaPartition(), record.kafkaOffset());
                        return response; // Return the failed response
                    }
                }

                // Success or non-retryable error
                if (attemptNumber > 0) {
                    log.info("HTTP request succeeded on attempt {}: topic={} partition={} offset={}",
                            attemptNumber + 1, record.topic(), record.kafkaPartition(), record.kafkaOffset());
                }
                return response;

            } catch (Exception e) {
                lastException = e;

                // Check if exception is retryable
                if (retryPolicy.shouldRetry(e)) {
                    if (retryPolicy.hasMoreAttempts(attemptNumber)) {
                        long delayMs = retryPolicy.getDelayMs(attemptNumber);
                        log.warn("HTTP request failed with exception (attempt {}), retrying in {}ms: topic={} partition={} offset={}, error={}",
                                attemptNumber + 1, delayMs,
                                record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                                e.getMessage());

                        Thread.sleep(delayMs);
                        attemptNumber++;
                        continue;
                    } else {
                        log.error("HTTP request failed with exception after {} attempts: topic={} partition={} offset={}",
                                attemptNumber + 1, record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
                        throw e;
                    }
                } else {
                    // Non-retryable exception, throw immediately
                    log.error("HTTP request failed with non-retryable exception: topic={} partition={} offset={}",
                            record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
                    throw e;
                }
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.debug("Flushing offsets: {}", currentOffsets);

        // Flush response producer if enabled
        if (responseProducer != null) {
            log.debug("Flushing response producer");
            responseProducer.flush();
        }

        // Ensure all HTTP requests have completed before committing offsets
        // Since we process records synchronously, this is already guaranteed
    }

    @Override
    public void stop() {
        log.info("Stopping HTTP Sink Task");

        // Close response producer
        if (responseProducer != null) {
            log.debug("Closing response producer");
            responseProducer.close();
        }

        // Close HTTP client
        if (httpClient != null) {
            log.debug("Closing HTTP client");
            httpClient.close();
        }

        // Close authentication provider
        if (authProvider != null) {
            log.debug("Closing authentication provider");
            authProvider.close();
        }

        log.info("HTTP Sink Task stopped");
    }

    /**
     * Build an HTTP request from a Kafka SinkRecord.
     */
    private Request buildHttpRequest(SinkRecord record) throws RecordConverter.ConversionException {
        // 1. Convert record value to HTTP body
        String body = recordConverter.convert(record);

        // 2. Build request with method and URL
        HttpRequestBuilder builder = new HttpRequestBuilder()
            .url(config.getHttpApiUrl())
            .method(config.getHttpMethod())
            .body(body);

        // 3. Add static headers
        Map<String, String> staticHeaders = config.getHeadersStatic();
        if (staticHeaders != null && !staticHeaders.isEmpty()) {
            staticHeaders.forEach(builder::header);
            log.trace("Added {} static headers", staticHeaders.size());
        }

        // 4. Forward Kafka headers
        if (config.isHeaderForwardEnabled() && record.headers() != null) {
            Map<String, String> forwardedHeaders = headerConverter.convert(record.headers());
            forwardedHeaders.forEach(builder::header);
            log.trace("Forwarded {} Kafka headers", forwardedHeaders.size());
        }

        // 5. Add authentication headers
        Map<String, String> authHeaders = authProvider.getAuthHeaders();
        if (authHeaders != null && !authHeaders.isEmpty()) {
            authHeaders.forEach(builder::header);
            log.trace("Added {} authentication headers", authHeaders.size());
        }

        return builder.build();
    }

    /**
     * Send HTTP response to the configured response topic.
     */
    private void sendToResponseTopic(HttpResponse response, SinkRecord originalRecord) {
        try {
            // Resolve the response topic name
            String responseTopic = responseHandler.resolveResponseTopic(originalRecord);

            // Create response record
            ResponseRecord responseRecord = responseHandler.createResponseRecord(
                    response,
                    originalRecord,
                    responseTopic
            );

            // Send to Kafka
            responseProducer.send(responseRecord);

            log.debug("Sent response to topic={}, status={}", responseTopic, response.getStatusCode());

        } catch (Exception e) {
            log.error("Failed to send response to topic: {}", e.getMessage(), e);
            // Don't fail the main record processing if response publishing fails
            // This is a best-effort operation
        }
    }

    /**
     * Handle null record values based on configuration.
     */
    private void handleNullValue(SinkRecord record) {
        String behavior = config.getBehaviorOnNullValues();

        if ("fail".equals(behavior)) {
            log.error("Null value encountered in record: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());
            throw new RuntimeException("Null value encountered in record");
        } else {
            log.debug("Ignoring null value in record: topic={} partition={} offset={}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
    }

    /**
     * Handle HTTP error responses based on configuration.
     */
    private void handleHttpError(HttpResponse response, SinkRecord record) {
        String behavior = config.getBehaviorOnError();

        log.warn("HTTP request returned error status: {} for record topic={} partition={} offset={}",
                response.getStatusCode(), record.topic(), record.kafkaPartition(), record.kafkaOffset());

        if ("fail".equals(behavior)) {
            throw new RuntimeException(
                    "HTTP request failed with status " + response.getStatusCode() +
                    ": " + response.getBody()
            );
        } else {
            // Log the error but continue processing
            log.error("HTTP error (ignored): status={}, body={}",
                    response.getStatusCode(), response.getBody());
        }
    }
}
