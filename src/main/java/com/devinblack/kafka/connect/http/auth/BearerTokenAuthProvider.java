package com.devinblack.kafka.connect.http.auth;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Bearer Token authentication provider.
 * Adds a static bearer token to the Authorization header.
 */
public class BearerTokenAuthProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(BearerTokenAuthProvider.class);

    private String token;

    @Override
    public void configure(Map<String, String> config) {
        this.token = config.get(HttpSinkConnectorConfig.AUTH_BEARER_TOKEN);

        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("Bearer token is required");
        }

        log.info("Bearer token authentication configured");
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + token);
        return headers;
    }

    @Override
    public void authenticate() {
        // No-op for bearer token - token is static
    }

    @Override
    public void close() {
        // No resources to clean up
        log.debug("Bearer token auth provider closed");
    }

    @Override
    public boolean isConfigured() {
        return token != null && !token.isEmpty();
    }
}
