package com.devinblack.kafka.connect.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * OAuth2 Client Credentials authentication provider.
 * Handles token fetching, caching, and automatic refresh.
 */
public class OAuth2ClientCredentialsProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(OAuth2ClientCredentialsProvider.class);

    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private String scope;
    private int expiryBufferSeconds;

    private String accessToken;
    private long tokenExpiryTime; // System.currentTimeMillis() when token expires

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public OAuth2ClientCredentialsProvider() {
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, String> config) {
        this.tokenUrl = config.get(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_URL);
        this.clientId = config.get(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_ID);
        this.clientSecret = config.get(HttpSinkConnectorConfig.AUTH_OAUTH2_CLIENT_SECRET);
        this.scope = config.get(HttpSinkConnectorConfig.AUTH_OAUTH2_SCOPE);

        String bufferStr = config.get(HttpSinkConnectorConfig.AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS);
        this.expiryBufferSeconds = bufferStr != null ? Integer.parseInt(bufferStr) : 300;

        if (tokenUrl == null || tokenUrl.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 token URL is required");
        }

        if (clientId == null || clientId.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 client ID is required");
        }

        if (clientSecret == null || clientSecret.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 client secret is required");
        }

        log.info("OAuth2 client credentials authentication configured: tokenUrl={}, clientId={}, scope={}",
                tokenUrl, clientId, scope);

        // Fetch initial token
        try {
            authenticate();
        } catch (Exception e) {
            log.error("Failed to fetch initial OAuth2 token", e);
            throw new RuntimeException("Failed to fetch initial OAuth2 token", e);
        }
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        // Check if token needs refresh
        if (isTokenExpired()) {
            log.info("OAuth2 token expired or expiring soon, refreshing...");
            try {
                authenticate();
            } catch (Exception e) {
                log.error("Failed to refresh OAuth2 token", e);
                // Return existing token even if expired - request might still work
            }
        }

        Map<String, String> headers = new HashMap<>();
        if (accessToken != null) {
            headers.put("Authorization", "Bearer " + accessToken);
        }
        return headers;
    }

    @Override
    public void authenticate() {
        try {
            log.debug("Fetching OAuth2 access token from: {}", tokenUrl);

            // Build token request
            FormBody.Builder formBuilder = new FormBody.Builder()
                    .add("grant_type", "client_credentials")
                    .add("client_id", clientId)
                    .add("client_secret", clientSecret);

            if (scope != null && !scope.isEmpty()) {
                formBuilder.add("scope", scope);
            }

            Request request = new Request.Builder()
                    .url(tokenUrl)
                    .post(formBuilder.build())
                    .build();

            // Execute request
            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    String body = response.body() != null ? response.body().string() : "";
                    throw new IOException("Token request failed: " + response.code() + " - " + body);
                }

                // Parse response
                String responseBody = response.body() != null ? response.body().string() : "{}";
                JsonNode jsonNode = objectMapper.readTree(responseBody);

                // Extract access token
                this.accessToken = jsonNode.has("access_token")
                        ? jsonNode.get("access_token").asText()
                        : null;

                if (accessToken == null || accessToken.isEmpty()) {
                    throw new IOException("No access_token in response: " + responseBody);
                }

                // Extract expiry time
                int expiresIn = jsonNode.has("expires_in")
                        ? jsonNode.get("expires_in").asInt()
                        : 3600; // Default to 1 hour

                // Calculate token expiry time (with buffer)
                this.tokenExpiryTime = System.currentTimeMillis()
                        + ((expiresIn - expiryBufferSeconds) * 1000L);

                log.info("OAuth2 token obtained successfully, expires in {} seconds", expiresIn);
            }

        } catch (IOException e) {
            log.error("Failed to fetch OAuth2 token", e);
            throw new RuntimeException("Failed to fetch OAuth2 token", e);
        }
    }

    @Override
    public void close() {
        // Cleanup HTTP client
        if (httpClient != null) {
            httpClient.connectionPool().evictAll();
            httpClient.dispatcher().executorService().shutdown();
        }
        log.debug("OAuth2 auth provider closed");
    }

    @Override
    public boolean isConfigured() {
        return accessToken != null;
    }

    /**
     * Check if the current token is expired or will expire soon.
     */
    private boolean isTokenExpired() {
        if (accessToken == null) {
            return true;
        }
        return System.currentTimeMillis() >= tokenExpiryTime;
    }

    /**
     * Get the current access token (for testing).
     */
    protected String getAccessToken() {
        return accessToken;
    }
}
