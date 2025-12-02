package com.devinblack.kafka.connect.http.auth;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * API Key authentication provider.
 * Adds API key as a custom header or query parameter.
 * Note: Query parameter support requires URL modification in the request builder.
 */
public class ApiKeyAuthProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthProvider.class);

    private String keyName;
    private String keyValue;
    private String location; // "header" or "query"

    @Override
    public void configure(Map<String, String> config) {
        this.keyName = config.get(HttpSinkConnectorConfig.AUTH_APIKEY_NAME);
        this.keyValue = config.get(HttpSinkConnectorConfig.AUTH_APIKEY_VALUE);
        this.location = config.getOrDefault(
                HttpSinkConnectorConfig.AUTH_APIKEY_LOCATION,
                "header"
        );

        if (keyName == null || keyName.isEmpty()) {
            throw new IllegalArgumentException("API key name is required");
        }

        if (keyValue == null || keyValue.isEmpty()) {
            throw new IllegalArgumentException("API key value is required");
        }

        if (!"header".equalsIgnoreCase(location) && !"query".equalsIgnoreCase(location)) {
            throw new IllegalArgumentException("API key location must be 'header' or 'query'");
        }

        log.info("API key authentication configured: name={}, location={}", keyName, location);
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        Map<String, String> headers = new HashMap<>();

        // For header-based API keys
        if ("header".equalsIgnoreCase(location)) {
            headers.put(keyName, keyValue);
        }
        // For query parameter, this would need to be handled in the URL construction
        // We'll add it as a special header that the request builder can check
        else if ("query".equalsIgnoreCase(location)) {
            headers.put("X-API-Key-Query-Param-Name", keyName);
            headers.put("X-API-Key-Query-Param-Value", keyValue);
        }

        return headers;
    }

    @Override
    public void authenticate() {
        // No-op for API key - key is static
    }

    @Override
    public void close() {
        // No resources to clean up
        log.debug("API key auth provider closed");
    }

    @Override
    public boolean isConfigured() {
        return keyName != null && keyValue != null;
    }

    /**
     * Get the API key location (header or query).
     */
    public String getLocation() {
        return location;
    }

    /**
     * Get the API key name.
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * Get the API key value.
     */
    public String getKeyValue() {
        return keyValue;
    }
}
