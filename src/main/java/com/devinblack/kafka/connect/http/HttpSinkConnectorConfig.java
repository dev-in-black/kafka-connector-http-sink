package com.devinblack.kafka.connect.http;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration for the HTTP Sink Connector.
 * Defines all configuration properties with validation and defaults.
 */
public class HttpSinkConnectorConfig extends AbstractConfig {

    // =============================
    // HTTP ENDPOINT CONFIGURATION
    // =============================
    public static final String HTTP_API_URL = "http.api.url";
    private static final String HTTP_API_URL_DOC = "The HTTP endpoint URL to send records to";

    public static final String HTTP_METHOD = "http.method";
    private static final String HTTP_METHOD_DEFAULT = "POST";
    private static final String HTTP_METHOD_DOC = "HTTP method to use (POST, PUT, DELETE)";

    public static final String HTTP_REQUEST_TIMEOUT_MS = "http.request.timeout.ms";
    private static final int HTTP_REQUEST_TIMEOUT_MS_DEFAULT = 30000;
    private static final String HTTP_REQUEST_TIMEOUT_MS_DOC = "HTTP request timeout in milliseconds";

    public static final String HTTP_CONNECTION_TIMEOUT_MS = "http.connection.timeout.ms";
    private static final int HTTP_CONNECTION_TIMEOUT_MS_DEFAULT = 5000;
    private static final String HTTP_CONNECTION_TIMEOUT_MS_DOC = "HTTP connection timeout in milliseconds";

    public static final String HTTP_MAX_CONNECTIONS_PER_ROUTE = "http.max.connections.per.route";
    private static final int HTTP_MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 20;
    private static final String HTTP_MAX_CONNECTIONS_PER_ROUTE_DOC = "Maximum connections per route";

    public static final String HTTP_MAX_CONNECTIONS_TOTAL = "http.max.connections.total";
    private static final int HTTP_MAX_CONNECTIONS_TOTAL_DEFAULT = 100;
    private static final String HTTP_MAX_CONNECTIONS_TOTAL_DOC = "Maximum total connections";

    // =============================
    // AUTHENTICATION CONFIGURATION
    // =============================
    public static final String AUTH_TYPE = "auth.type";
    private static final String AUTH_TYPE_DEFAULT = "none";
    private static final String AUTH_TYPE_DOC = "Authentication type (none, basic, bearer, apikey, oauth2)";

    // Basic Auth
    public static final String AUTH_BASIC_USERNAME = "auth.basic.username";
    private static final String AUTH_BASIC_USERNAME_DOC = "Username for basic authentication";

    public static final String AUTH_BASIC_PASSWORD = "auth.basic.password";
    private static final String AUTH_BASIC_PASSWORD_DOC = "Password for basic authentication";

    // Bearer Token
    public static final String AUTH_BEARER_TOKEN = "auth.bearer.token";
    private static final String AUTH_BEARER_TOKEN_DOC = "Bearer token for authentication";

    // API Key
    public static final String AUTH_APIKEY_NAME = "auth.apikey.name";
    private static final String AUTH_APIKEY_NAME_DOC = "API key header/parameter name";

    public static final String AUTH_APIKEY_VALUE = "auth.apikey.value";
    private static final String AUTH_APIKEY_VALUE_DOC = "API key value";

    public static final String AUTH_APIKEY_LOCATION = "auth.apikey.location";
    private static final String AUTH_APIKEY_LOCATION_DEFAULT = "header";
    private static final String AUTH_APIKEY_LOCATION_DOC = "API key location (header or query)";

    // OAuth2 Client Credentials
    public static final String AUTH_OAUTH2_TOKEN_URL = "auth.oauth2.token.url";
    private static final String AUTH_OAUTH2_TOKEN_URL_DOC = "OAuth2 token endpoint URL";

    public static final String AUTH_OAUTH2_CLIENT_ID = "auth.oauth2.client.id";
    private static final String AUTH_OAUTH2_CLIENT_ID_DOC = "OAuth2 client ID";

    public static final String AUTH_OAUTH2_CLIENT_SECRET = "auth.oauth2.client.secret";
    private static final String AUTH_OAUTH2_CLIENT_SECRET_DOC = "OAuth2 client secret";

    public static final String AUTH_OAUTH2_SCOPE = "auth.oauth2.scope";
    private static final String AUTH_OAUTH2_SCOPE_DOC = "OAuth2 scope (optional)";

    public static final String AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS = "auth.oauth2.token.expiry.buffer.seconds";
    private static final int AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS_DEFAULT = 300;
    private static final String AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS_DOC = "Buffer time before token expiry to refresh (seconds)";

    // =============================
    // HEADER FORWARDING CONFIGURATION
    // =============================
    public static final String HEADERS_FORWARD_ENABLED = "headers.forward.enabled";
    private static final boolean HEADERS_FORWARD_ENABLED_DEFAULT = true;
    private static final String HEADERS_FORWARD_ENABLED_DOC = "Enable forwarding Kafka headers to HTTP request";

    public static final String HEADERS_FORWARD_INCLUDE = "headers.forward.include";
    private static final String HEADERS_FORWARD_INCLUDE_DEFAULT = "";
    private static final String HEADERS_FORWARD_INCLUDE_DOC = "Comma-separated list of headers to include (empty = all)";

    public static final String HEADERS_FORWARD_EXCLUDE = "headers.forward.exclude";
    private static final String HEADERS_FORWARD_EXCLUDE_DEFAULT = "";
    private static final String HEADERS_FORWARD_EXCLUDE_DOC = "Comma-separated list of headers to exclude";

    public static final String HEADERS_FORWARD_PREFIX = "headers.forward.prefix";
    private static final String HEADERS_FORWARD_PREFIX_DEFAULT = "";
    private static final String HEADERS_FORWARD_PREFIX_DOC = "Prefix to add to forwarded headers";

    public static final String HEADERS_STATIC = "headers.static";
    private static final String HEADERS_STATIC_DEFAULT = "";
    private static final String HEADERS_STATIC_DOC = "Static headers to add to all requests (format: key1:value1,key2:value2)";

    // =============================
    // RESPONSE HANDLING CONFIGURATION
    // =============================
    public static final String RESPONSE_TOPIC_ENABLED = "response.topic.enabled";
    private static final boolean RESPONSE_TOPIC_ENABLED_DEFAULT = false;
    private static final String RESPONSE_TOPIC_ENABLED_DOC = "Enable sending HTTP responses to a Kafka topic";

    public static final String RESPONSE_TOPIC_NAME = "response.topic.name";
    private static final String RESPONSE_TOPIC_NAME_DOC = "Response topic name (supports ${topic} variable)";

    public static final String RESPONSE_INCLUDE_ORIGINAL_KEY = "response.include.original.key";
    private static final boolean RESPONSE_INCLUDE_ORIGINAL_KEY_DEFAULT = true;
    private static final String RESPONSE_INCLUDE_ORIGINAL_KEY_DOC = "Include original record key in response";

    public static final String RESPONSE_INCLUDE_ORIGINAL_HEADERS = "response.include.original.headers";
    private static final boolean RESPONSE_INCLUDE_ORIGINAL_HEADERS_DEFAULT = true;
    private static final String RESPONSE_INCLUDE_ORIGINAL_HEADERS_DOC = "Include original record headers in response";

    public static final String RESPONSE_INCLUDE_REQUEST_METADATA = "response.include.request.metadata";
    private static final boolean RESPONSE_INCLUDE_REQUEST_METADATA_DEFAULT = true;
    private static final String RESPONSE_INCLUDE_REQUEST_METADATA_DOC = "Include request metadata in response headers";

    public static final String RESPONSE_VALUE_FORMAT = "response.value.format";
    private static final String RESPONSE_VALUE_FORMAT_DEFAULT = "string";
    private static final String RESPONSE_VALUE_FORMAT_DOC =
            "Format for response values sent to Kafka (string or json). " +
            "JSON format validates response body is valid JSON and fails/warns if invalid.";

    public static final String RESPONSE_ORIGINAL_HEADERS_INCLUDE = "response.original.headers.include";
    private static final String RESPONSE_ORIGINAL_HEADERS_INCLUDE_DEFAULT = "";
    private static final String RESPONSE_ORIGINAL_HEADERS_INCLUDE_DOC =
            "Comma-separated list of original Kafka record header names to include in response. " +
            "Empty means include all headers when response.include.original.headers is enabled.";

    // =============================
    // ERROR HANDLING CONFIGURATION
    // =============================
    public static final String BEHAVIOR_ON_NULL_VALUES = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = "fail";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "Behavior when encountering null values (fail or ignore)";

    public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
    private static final String BEHAVIOR_ON_ERROR_DEFAULT = "fail";
    private static final String BEHAVIOR_ON_ERROR_DOC = "Behavior on HTTP errors (fail or log)";

    public static final String ERRORS_TOLERANCE = "errors.tolerance";
    private static final String ERRORS_TOLERANCE_DEFAULT = "none";
    private static final String ERRORS_TOLERANCE_DOC = "Error tolerance (none or all)";

    public static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME = "errors.deadletterqueue.topic.name";
    private static final String ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DOC = "Dead letter queue topic name";

    // =============================
    // RETRY CONFIGURATION
    // =============================
    public static final String RETRY_ENABLED = "retry.enabled";
    private static final boolean RETRY_ENABLED_DEFAULT = true;
    private static final String RETRY_ENABLED_DOC = "Enable retry on failure";

    public static final String RETRY_MAX_ATTEMPTS = "retry.max.attempts";
    private static final int RETRY_MAX_ATTEMPTS_DEFAULT = 5;
    private static final String RETRY_MAX_ATTEMPTS_DOC = "Maximum retry attempts";

    public static final String RETRY_BACKOFF_INITIAL_MS = "retry.backoff.initial.ms";
    private static final long RETRY_BACKOFF_INITIAL_MS_DEFAULT = 1000;
    private static final String RETRY_BACKOFF_INITIAL_MS_DOC = "Initial retry backoff in milliseconds";

    public static final String RETRY_BACKOFF_MAX_MS = "retry.backoff.max.ms";
    private static final long RETRY_BACKOFF_MAX_MS_DEFAULT = 60000;
    private static final String RETRY_BACKOFF_MAX_MS_DOC = "Maximum retry backoff in milliseconds";

    public static final String RETRY_BACKOFF_MULTIPLIER = "retry.backoff.multiplier";
    private static final double RETRY_BACKOFF_MULTIPLIER_DEFAULT = 2.0;
    private static final String RETRY_BACKOFF_MULTIPLIER_DOC = "Retry backoff multiplier";

    public static final String RETRY_ON_STATUS_CODES = "retry.on.status.codes";
    private static final String RETRY_ON_STATUS_CODES_DEFAULT = "429,500,502,503,504";
    private static final String RETRY_ON_STATUS_CODES_DOC = "HTTP status codes to retry on (comma-separated)";

    // =============================
    // CONFIG DEFINITION
    // =============================
    public static ConfigDef config() {
        ConfigDef configDef = new ConfigDef();

        // HTTP Endpoint Configuration
        configDef.define(
                HTTP_API_URL,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                HTTP_API_URL_DOC
        ).define(
                HTTP_METHOD,
                ConfigDef.Type.STRING,
                HTTP_METHOD_DEFAULT,
                ConfigDef.ValidString.in("POST", "PUT", "DELETE"),
                ConfigDef.Importance.HIGH,
                HTTP_METHOD_DOC
        ).define(
                HTTP_REQUEST_TIMEOUT_MS,
                ConfigDef.Type.INT,
                HTTP_REQUEST_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(1000),
                ConfigDef.Importance.MEDIUM,
                HTTP_REQUEST_TIMEOUT_MS_DOC
        ).define(
                HTTP_CONNECTION_TIMEOUT_MS,
                ConfigDef.Type.INT,
                HTTP_CONNECTION_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(1000),
                ConfigDef.Importance.MEDIUM,
                HTTP_CONNECTION_TIMEOUT_MS_DOC
        ).define(
                HTTP_MAX_CONNECTIONS_PER_ROUTE,
                ConfigDef.Type.INT,
                HTTP_MAX_CONNECTIONS_PER_ROUTE_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                HTTP_MAX_CONNECTIONS_PER_ROUTE_DOC
        ).define(
                HTTP_MAX_CONNECTIONS_TOTAL,
                ConfigDef.Type.INT,
                HTTP_MAX_CONNECTIONS_TOTAL_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                HTTP_MAX_CONNECTIONS_TOTAL_DOC
        );

        // Authentication Configuration
        configDef.define(
                AUTH_TYPE,
                ConfigDef.Type.STRING,
                AUTH_TYPE_DEFAULT,
                ConfigDef.ValidString.in("none", "basic", "bearer", "apikey", "oauth2"),
                ConfigDef.Importance.HIGH,
                AUTH_TYPE_DOC
        ).define(
                AUTH_BASIC_USERNAME,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_BASIC_USERNAME_DOC
        ).define(
                AUTH_BASIC_PASSWORD,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_BASIC_PASSWORD_DOC
        ).define(
                AUTH_BEARER_TOKEN,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_BEARER_TOKEN_DOC
        ).define(
                AUTH_APIKEY_NAME,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_APIKEY_NAME_DOC
        ).define(
                AUTH_APIKEY_VALUE,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_APIKEY_VALUE_DOC
        ).define(
                AUTH_APIKEY_LOCATION,
                ConfigDef.Type.STRING,
                AUTH_APIKEY_LOCATION_DEFAULT,
                ConfigDef.ValidString.in("header", "query"),
                ConfigDef.Importance.LOW,
                AUTH_APIKEY_LOCATION_DOC
        ).define(
                AUTH_OAUTH2_TOKEN_URL,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_OAUTH2_TOKEN_URL_DOC
        ).define(
                AUTH_OAUTH2_CLIENT_ID,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_OAUTH2_CLIENT_ID_DOC
        ).define(
                AUTH_OAUTH2_CLIENT_SECRET,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.MEDIUM,
                AUTH_OAUTH2_CLIENT_SECRET_DOC
        ).define(
                AUTH_OAUTH2_SCOPE,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                AUTH_OAUTH2_SCOPE_DOC
        ).define(
                AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS,
                ConfigDef.Type.INT,
                AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS_DOC
        );

        // Header Forwarding Configuration
        configDef.define(
                HEADERS_FORWARD_ENABLED,
                ConfigDef.Type.BOOLEAN,
                HEADERS_FORWARD_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                HEADERS_FORWARD_ENABLED_DOC
        ).define(
                HEADERS_FORWARD_INCLUDE,
                ConfigDef.Type.STRING,
                HEADERS_FORWARD_INCLUDE_DEFAULT,
                ConfigDef.Importance.LOW,
                HEADERS_FORWARD_INCLUDE_DOC
        ).define(
                HEADERS_FORWARD_EXCLUDE,
                ConfigDef.Type.STRING,
                HEADERS_FORWARD_EXCLUDE_DEFAULT,
                ConfigDef.Importance.LOW,
                HEADERS_FORWARD_EXCLUDE_DOC
        ).define(
                HEADERS_FORWARD_PREFIX,
                ConfigDef.Type.STRING,
                HEADERS_FORWARD_PREFIX_DEFAULT,
                ConfigDef.Importance.LOW,
                HEADERS_FORWARD_PREFIX_DOC
        ).define(
                HEADERS_STATIC,
                ConfigDef.Type.STRING,
                HEADERS_STATIC_DEFAULT,
                ConfigDef.Importance.LOW,
                HEADERS_STATIC_DOC
        );

        // Response Handling Configuration
        configDef.define(
                RESPONSE_TOPIC_ENABLED,
                ConfigDef.Type.BOOLEAN,
                RESPONSE_TOPIC_ENABLED_DEFAULT,
                ConfigDef.Importance.HIGH,
                RESPONSE_TOPIC_ENABLED_DOC
        ).define(
                RESPONSE_TOPIC_NAME,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                RESPONSE_TOPIC_NAME_DOC
        ).define(
                RESPONSE_INCLUDE_ORIGINAL_KEY,
                ConfigDef.Type.BOOLEAN,
                RESPONSE_INCLUDE_ORIGINAL_KEY_DEFAULT,
                ConfigDef.Importance.LOW,
                RESPONSE_INCLUDE_ORIGINAL_KEY_DOC
        ).define(
                RESPONSE_INCLUDE_ORIGINAL_HEADERS,
                ConfigDef.Type.BOOLEAN,
                RESPONSE_INCLUDE_ORIGINAL_HEADERS_DEFAULT,
                ConfigDef.Importance.LOW,
                RESPONSE_INCLUDE_ORIGINAL_HEADERS_DOC
        ).define(
                RESPONSE_INCLUDE_REQUEST_METADATA,
                ConfigDef.Type.BOOLEAN,
                RESPONSE_INCLUDE_REQUEST_METADATA_DEFAULT,
                ConfigDef.Importance.LOW,
                RESPONSE_INCLUDE_REQUEST_METADATA_DOC
        ).define(
                RESPONSE_VALUE_FORMAT,
                ConfigDef.Type.STRING,
                RESPONSE_VALUE_FORMAT_DEFAULT,
                ConfigDef.ValidString.in("string", "json"),
                ConfigDef.Importance.MEDIUM,
                RESPONSE_VALUE_FORMAT_DOC
        ).define(
                RESPONSE_ORIGINAL_HEADERS_INCLUDE,
                ConfigDef.Type.STRING,
                RESPONSE_ORIGINAL_HEADERS_INCLUDE_DEFAULT,
                ConfigDef.Importance.LOW,
                RESPONSE_ORIGINAL_HEADERS_INCLUDE_DOC
        );

        // Error Handling Configuration
        configDef.define(
                BEHAVIOR_ON_NULL_VALUES,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_NULL_VALUES_DEFAULT,
                ConfigDef.ValidString.in("fail", "ignore"),
                ConfigDef.Importance.MEDIUM,
                BEHAVIOR_ON_NULL_VALUES_DOC
        ).define(
                BEHAVIOR_ON_ERROR,
                ConfigDef.Type.STRING,
                BEHAVIOR_ON_ERROR_DEFAULT,
                ConfigDef.ValidString.in("fail", "log"),
                ConfigDef.Importance.HIGH,
                BEHAVIOR_ON_ERROR_DOC
        ).define(
                ERRORS_TOLERANCE,
                ConfigDef.Type.STRING,
                ERRORS_TOLERANCE_DEFAULT,
                ConfigDef.ValidString.in("none", "all"),
                ConfigDef.Importance.HIGH,
                ERRORS_TOLERANCE_DOC
        ).define(
                ERRORS_DEADLETTERQUEUE_TOPIC_NAME,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                ERRORS_DEADLETTERQUEUE_TOPIC_NAME_DOC
        );

        // Retry Configuration
        configDef.define(
                RETRY_ENABLED,
                ConfigDef.Type.BOOLEAN,
                RETRY_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                RETRY_ENABLED_DOC
        ).define(
                RETRY_MAX_ATTEMPTS,
                ConfigDef.Type.INT,
                RETRY_MAX_ATTEMPTS_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM,
                RETRY_MAX_ATTEMPTS_DOC
        ).define(
                RETRY_BACKOFF_INITIAL_MS,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_INITIAL_MS_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                RETRY_BACKOFF_INITIAL_MS_DOC
        ).define(
                RETRY_BACKOFF_MAX_MS,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_MAX_MS_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                RETRY_BACKOFF_MAX_MS_DOC
        ).define(
                RETRY_BACKOFF_MULTIPLIER,
                ConfigDef.Type.DOUBLE,
                RETRY_BACKOFF_MULTIPLIER_DEFAULT,
                ConfigDef.Range.atLeast(1.0),
                ConfigDef.Importance.LOW,
                RETRY_BACKOFF_MULTIPLIER_DOC
        ).define(
                RETRY_ON_STATUS_CODES,
                ConfigDef.Type.STRING,
                RETRY_ON_STATUS_CODES_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                RETRY_ON_STATUS_CODES_DOC
        );

        return configDef;
    }

    public HttpSinkConnectorConfig(Map<String, String> props) {
        super(config(), props);
        validate();
    }

    private void validate() {
        // Validate response topic configuration
        if (isResponseTopicEnabled() && getResponseTopicName() == null) {
            throw new ConfigException("response.topic.name is required when response.topic.enabled is true");
        }

        // Validate authentication configuration
        String authType = getAuthType();
        switch (authType) {
            case "basic":
                if (getAuthBasicUsername() == null || getAuthBasicPassword() == null) {
                    throw new ConfigException("auth.basic.username and auth.basic.password are required when auth.type is 'basic'");
                }
                break;
            case "bearer":
                if (getAuthBearerToken() == null) {
                    throw new ConfigException("auth.bearer.token is required when auth.type is 'bearer'");
                }
                break;
            case "apikey":
                if (getAuthApiKeyName() == null || getAuthApiKeyValue() == null) {
                    throw new ConfigException("auth.apikey.name and auth.apikey.value are required when auth.type is 'apikey'");
                }
                break;
            case "oauth2":
                if (getAuthOAuth2TokenUrl() == null || getAuthOAuth2ClientId() == null || getAuthOAuth2ClientSecret() == null) {
                    throw new ConfigException("auth.oauth2.token.url, auth.oauth2.client.id, and auth.oauth2.client.secret are required when auth.type is 'oauth2'");
                }
                break;
        }

        // Validate DLQ configuration
        if ("all".equals(getErrorsTolerance()) && getErrorsDeadLetterQueueTopicName() == null) {
            throw new ConfigException("errors.deadletterqueue.topic.name is required when errors.tolerance is 'all'");
        }
    }

    // Getters for HTTP Endpoint Configuration
    public String getHttpApiUrl() {
        return getString(HTTP_API_URL);
    }

    public String getHttpMethod() {
        return getString(HTTP_METHOD);
    }

    public int getHttpRequestTimeoutMs() {
        return getInt(HTTP_REQUEST_TIMEOUT_MS);
    }

    public int getHttpConnectionTimeoutMs() {
        return getInt(HTTP_CONNECTION_TIMEOUT_MS);
    }

    public int getHttpMaxConnectionsPerRoute() {
        return getInt(HTTP_MAX_CONNECTIONS_PER_ROUTE);
    }

    public int getHttpMaxConnectionsTotal() {
        return getInt(HTTP_MAX_CONNECTIONS_TOTAL);
    }

    // Getters for Authentication Configuration
    public String getAuthType() {
        return getString(AUTH_TYPE);
    }

    public String getAuthBasicUsername() {
        return getString(AUTH_BASIC_USERNAME);
    }

    public String getAuthBasicPassword() {
        return getPassword(AUTH_BASIC_PASSWORD) != null ? getPassword(AUTH_BASIC_PASSWORD).value() : null;
    }

    public String getAuthBearerToken() {
        return getPassword(AUTH_BEARER_TOKEN) != null ? getPassword(AUTH_BEARER_TOKEN).value() : null;
    }

    public String getAuthApiKeyName() {
        return getString(AUTH_APIKEY_NAME);
    }

    public String getAuthApiKeyValue() {
        return getPassword(AUTH_APIKEY_VALUE) != null ? getPassword(AUTH_APIKEY_VALUE).value() : null;
    }

    public String getAuthApiKeyLocation() {
        return getString(AUTH_APIKEY_LOCATION);
    }

    public String getAuthOAuth2TokenUrl() {
        return getString(AUTH_OAUTH2_TOKEN_URL);
    }

    public String getAuthOAuth2ClientId() {
        return getString(AUTH_OAUTH2_CLIENT_ID);
    }

    public String getAuthOAuth2ClientSecret() {
        return getPassword(AUTH_OAUTH2_CLIENT_SECRET) != null ? getPassword(AUTH_OAUTH2_CLIENT_SECRET).value() : null;
    }

    public String getAuthOAuth2Scope() {
        return getString(AUTH_OAUTH2_SCOPE);
    }

    public int getAuthOAuth2TokenExpiryBufferSeconds() {
        return getInt(AUTH_OAUTH2_TOKEN_EXPIRY_BUFFER_SECONDS);
    }

    // Getters for Header Forwarding Configuration
    public boolean isHeaderForwardEnabled() {
        return getBoolean(HEADERS_FORWARD_ENABLED);
    }

    public List<String> getHeadersForwardInclude() {
        String include = getString(HEADERS_FORWARD_INCLUDE);
        if (include == null || include.trim().isEmpty()) {
            return null; // null means include all
        }
        return Arrays.stream(include.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public List<String> getHeadersForwardExclude() {
        String exclude = getString(HEADERS_FORWARD_EXCLUDE);
        if (exclude == null || exclude.trim().isEmpty()) {
            return null;
        }
        return Arrays.stream(exclude.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public String getHeadersForwardPrefix() {
        return getString(HEADERS_FORWARD_PREFIX);
    }

    public Map<String, String> getHeadersStatic() {
        String staticHeaders = getString(HEADERS_STATIC);
        if (staticHeaders == null || staticHeaders.trim().isEmpty()) {
            return Map.of();
        }
        return Arrays.stream(staticHeaders.split(","))
                .map(String::trim)
                .filter(s -> s.contains(":"))
                .map(s -> s.split(":", 2))
                .collect(Collectors.toMap(parts -> parts[0].trim(), parts -> parts[1].trim()));
    }

    // Getters for Response Handling Configuration
    public boolean isResponseTopicEnabled() {
        return getBoolean(RESPONSE_TOPIC_ENABLED);
    }

    public String getResponseTopicName() {
        return getString(RESPONSE_TOPIC_NAME);
    }

    public boolean isResponseIncludeOriginalKey() {
        return getBoolean(RESPONSE_INCLUDE_ORIGINAL_KEY);
    }

    public boolean isResponseIncludeOriginalHeaders() {
        return getBoolean(RESPONSE_INCLUDE_ORIGINAL_HEADERS);
    }

    public boolean isResponseIncludeRequestMetadata() {
        return getBoolean(RESPONSE_INCLUDE_REQUEST_METADATA);
    }

    public String getResponseValueFormat() {
        return getString(RESPONSE_VALUE_FORMAT);
    }

    public List<String> getResponseOriginalHeadersInclude() {
        String include = getString(RESPONSE_ORIGINAL_HEADERS_INCLUDE);
        if (include == null || include.trim().isEmpty()) {
            return null;  // null means include all
        }
        return Arrays.stream(include.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    // Getters for Error Handling Configuration
    public String getBehaviorOnNullValues() {
        return getString(BEHAVIOR_ON_NULL_VALUES);
    }

    public String getBehaviorOnError() {
        return getString(BEHAVIOR_ON_ERROR);
    }

    public String getErrorsTolerance() {
        return getString(ERRORS_TOLERANCE);
    }

    public String getErrorsDeadLetterQueueTopicName() {
        return getString(ERRORS_DEADLETTERQUEUE_TOPIC_NAME);
    }

    // Getters for Retry Configuration
    public boolean isRetryEnabled() {
        return getBoolean(RETRY_ENABLED);
    }

    public int getRetryMaxAttempts() {
        return getInt(RETRY_MAX_ATTEMPTS);
    }

    public long getRetryBackoffInitialMs() {
        return getLong(RETRY_BACKOFF_INITIAL_MS);
    }

    public long getRetryBackoffMaxMs() {
        return getLong(RETRY_BACKOFF_MAX_MS);
    }

    public double getRetryBackoffMultiplier() {
        return getDouble(RETRY_BACKOFF_MULTIPLIER);
    }

    public List<Integer> getRetryOnStatusCodes() {
        String codes = getString(RETRY_ON_STATUS_CODES);
        return Arrays.stream(codes.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }
}
