package com.devinblack.kafka.connect.http.auth;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic Authentication provider.
 * Encodes username:password in Base64 and adds as Authorization header.
 */
public class BasicAuthProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(BasicAuthProvider.class);

    private String username;
    private String password;
    private String encodedCredentials;

    @Override
    public void configure(Map<String, String> config) {
        this.username = config.get(HttpSinkConnectorConfig.AUTH_BASIC_USERNAME);
        this.password = config.get(HttpSinkConnectorConfig.AUTH_BASIC_PASSWORD);

        if (username == null || password == null) {
            throw new IllegalArgumentException("Basic auth requires both username and password");
        }

        // Encode credentials
        String credentials = username + ":" + password;
        this.encodedCredentials = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

        log.info("Basic authentication configured for user: {}", username);
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Basic " + encodedCredentials);
        return headers;
    }

    @Override
    public void authenticate() {
        // No-op for basic auth - credentials are static
    }

    @Override
    public void close() {
        // No resources to clean up
        log.debug("Basic auth provider closed");
    }

    @Override
    public boolean isConfigured() {
        return encodedCredentials != null;
    }
}
