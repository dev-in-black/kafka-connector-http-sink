package com.devinblack.kafka.connect.http.auth;

import java.util.Map;

/**
 * Interface for authentication providers.
 * Implementations handle different authentication mechanisms (Basic, Bearer, API Key, OAuth2).
 */
public interface AuthenticationProvider {

    /**
     * Configure the authentication provider with connector configuration.
     *
     * @param config Configuration map
     */
    void configure(Map<String, String> config);

    /**
     * Get authentication headers to add to HTTP requests.
     *
     * @return Map of header name to header value
     */
    Map<String, String> getAuthHeaders();

    /**
     * Perform authentication or token refresh.
     * For OAuth2, this will fetch or refresh the access token.
     * For other auth types, this is typically a no-op.
     */
    void authenticate();

    /**
     * Close the authentication provider and release resources.
     */
    void close();

    /**
     * Check if authentication is required/configured.
     *
     * @return true if authentication is active, false otherwise
     */
    boolean isConfigured();
}
