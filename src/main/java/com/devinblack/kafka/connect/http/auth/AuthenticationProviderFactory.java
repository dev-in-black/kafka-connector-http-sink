package com.devinblack.kafka.connect.http.auth;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating authentication providers based on configuration.
 */
public class AuthenticationProviderFactory {
    private static final Logger log = LoggerFactory.getLogger(AuthenticationProviderFactory.class);

    /**
     * Create an authentication provider based on the configuration.
     *
     * @param config Connector configuration
     * @return Configured authentication provider
     */
    public static AuthenticationProvider create(HttpSinkConnectorConfig config) {
        String authType = config.getAuthType();
        log.info("Creating authentication provider for type: {}", authType);

        AuthenticationProvider provider = createProvider(authType);

        // Configure the provider
        Map<String, String> configMap = new HashMap<>();

        // Add all configuration values needed by the provider
        switch (authType.toLowerCase()) {
            case "basic":
                configMap.put(HttpSinkConnectorConfig.AUTH_BASIC_USERNAME, config.getAuthBasicUsername());
                configMap.put(HttpSinkConnectorConfig.AUTH_BASIC_PASSWORD, config.getAuthBasicPassword());
                break;

            case "bearer":
                configMap.put(HttpSinkConnectorConfig.AUTH_BEARER_TOKEN, config.getAuthBearerToken());
                break;

            case "apikey":
                configMap.put(HttpSinkConnectorConfig.AUTH_APIKEY_NAME, config.getAuthApiKeyName());
                configMap.put(HttpSinkConnectorConfig.AUTH_APIKEY_VALUE, config.getAuthApiKeyValue());
                configMap.put(HttpSinkConnectorConfig.AUTH_APIKEY_LOCATION, config.getAuthApiKeyLocation());
                break;

            case "oauth2":
                configMap.put(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_URL, config.getAuthOAuth2TokenUrl());
                configMap.put(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_ID, config.getAuthOAuth2ClientId());
                configMap.put(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_SECRET, config.getAuthOAuth2ClientSecret());
                if (config.getAuthOAuth2Scope() != null) {
                    configMap.put(HttpSinkConnectorConfig.AUTH_OAUTH2_SCOPE, config.getAuthOAuth2Scope());
                }
                configMap.put(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS,
                        String.valueOf(config.getAuthOAuth2TokenExpiryBufferSeconds()));
                break;

            case "none":
                // No configuration needed
                break;

            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }

        // Configure the provider if it's not NoAuthProvider
        if (!"none".equals(authType.toLowerCase())) {
            provider.configure(configMap);
        }

        return provider;
    }

    private static AuthenticationProvider createProvider(String authType) {
        switch (authType.toLowerCase()) {
            case "basic":
                return new BasicAuthProvider();

            case "bearer":
                return new BearerTokenAuthProvider();

            case "apikey":
                return new ApiKeyAuthProvider();

            case "oauth2":
                return new OAuth2ClientCredentialsProvider();

            case "none":
                return new NoAuthProvider();

            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }

    /**
     * No-op authentication provider for when auth is disabled.
     */
    private static class NoAuthProvider implements AuthenticationProvider {
        @Override
        public void configure(Map<String, String> config) {
            // No-op
        }

        @Override
        public Map<String, String> getAuthHeaders() {
            return new HashMap<>();
        }

        @Override
        public void authenticate() {
            // No-op
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        public boolean isConfigured() {
            return true; // Always configured (no auth)
        }
    }
}
