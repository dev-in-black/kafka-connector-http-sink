package com.devinblack.kafka.connect.http;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HttpSinkConnector is a Kafka Connect sink connector that sends records to HTTP endpoints.
 *
 * Key features:
 * - Supports POST, PUT, DELETE HTTP methods
 * - Multiple authentication methods (Basic, Bearer, API Key, OAuth2)
 * - Header forwarding from Kafka messages to HTTP requests
 * - Response topic publishing (HTTP responses sent back to Kafka)
 * - Configurable error handling and retry logic
 */
public class HttpSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting HTTP Sink Connector");
        this.configProps = new HashMap<>(props);

        // Validate configuration
        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        log.info("HTTP Sink Connector configuration validated successfully");
        log.info("HTTP endpoint: {}", config.getHttpApiUrl());
        log.info("HTTP method: {}", config.getHttpMethod());
        log.info("Authentication type: {}", config.getAuthType());
        log.info("Response topic enabled: {}", config.isResponseTopicEnabled());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        // All tasks get the same configuration
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(configProps));
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping HTTP Sink Connector");
        // No resources to clean up at connector level
    }

    @Override
    public ConfigDef config() {
        return HttpSinkConnectorConfig.config();
    }
}
