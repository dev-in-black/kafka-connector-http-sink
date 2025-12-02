package com.devinblack.kafka.connect.http.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * No authentication provider.
 *
 * This provider is used when no authentication is required.
 * It returns empty headers and does nothing on authenticate/close.
 */
public class NoAuthProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(NoAuthProvider.class);

    @Override
    public void configure(Map<String, String> config) {
        log.info("No authentication configured");
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        return Collections.emptyMap();
    }

    @Override
    public void authenticate() {
        // No-op
    }

    @Override
    public void close() {
        log.debug("No auth provider closed");
    }

    @Override
    public boolean isConfigured() {
        return true; // Always configured (as "no auth")
    }
}
