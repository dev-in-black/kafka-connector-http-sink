package com.devinblack.kafka.connect.http.client;

import com.devinblack.kafka.connect.http.HttpSinkConnectorConfig;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HTTP client wrapper around OkHttp with connection pooling and configurable timeouts.
 * Thread-safe for concurrent use by multiple tasks.
 */
public class HttpClient {
    private static final Logger log = LoggerFactory.getLogger(HttpClient.class);

    private final OkHttpClient client;
    private final HttpSinkConnectorConfig config;

    public HttpClient(HttpSinkConnectorConfig config) {
        this.config = config;
        this.client = buildOkHttpClient(config);
        log.info("HTTP client initialized with connection pool: maxPerRoute={}, maxTotal={}",
                config.getHttpMaxConnectionsPerRoute(),
                config.getHttpMaxConnectionsTotal());
    }

    private OkHttpClient buildOkHttpClient(HttpSinkConnectorConfig config) {
        // Configure connection pool
        ConnectionPool connectionPool = new ConnectionPool(
                config.getHttpMaxConnectionsTotal(),
                5,
                TimeUnit.MINUTES
        );

        return new OkHttpClient.Builder()
                .connectionPool(connectionPool)
                .connectTimeout(config.getHttpConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getHttpRequestTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getHttpRequestTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(true)
                .followSslRedirects(true)
                .retryOnConnectionFailure(false) // We handle retries ourselves
                .build();
    }

    /**
     * Execute an HTTP request and return the response.
     *
     * @param request OkHttp Request object
     * @return HttpResponse with status, headers, body, and timing
     * @throws IOException if the request fails
     */
    public HttpResponse execute(Request request) throws IOException {
        long startTime = System.currentTimeMillis();

        log.debug("Executing HTTP request: {} {}", request.method(), request.url());

        try (Response response = client.newCall(request).execute()) {
            long responseTime = System.currentTimeMillis() - startTime;

            // Extract response headers
            Map<String, String> headers = extractHeaders(response);

            // Extract response body
            String body = extractBody(response);

            HttpResponse httpResponse = new HttpResponse(
                    response.code(),
                    headers,
                    body,
                    responseTime
            );

            log.debug("HTTP response received: status={}, responseTime={}ms, bodyLength={}",
                    response.code(), responseTime, body != null ? body.length() : 0);

            return httpResponse;

        } catch (IOException e) {
            long responseTime = System.currentTimeMillis() - startTime;
            log.error("HTTP request failed: {} {}, error={}, time={}ms",
                    request.method(), request.url(), e.getMessage(), responseTime);
            throw e;
        }
    }

    /**
     * Create a new request builder.
     */
    public HttpRequestBuilder requestBuilder() {
        return new HttpRequestBuilder();
    }

    /**
     * Extract headers from OkHttp response.
     */
    private Map<String, String> extractHeaders(Response response) {
        Map<String, String> headers = new HashMap<>();
        response.headers().names().forEach(name -> {
            // If multiple values exist, take the last one
            String value = response.header(name);
            if (value != null) {
                headers.put(name, value);
            }
        });
        return headers;
    }

    /**
     * Extract body from OkHttp response.
     */
    private String extractBody(Response response) throws IOException {
        ResponseBody responseBody = response.body();
        if (responseBody == null) {
            return null;
        }

        try {
            return responseBody.string();
        } catch (IOException e) {
            log.warn("Failed to read response body: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Close the HTTP client and release resources.
     */
    public void close() {
        log.info("Closing HTTP client");

        // Shutdown connection pool
        client.connectionPool().evictAll();

        // Shutdown dispatcher
        client.dispatcher().executorService().shutdown();

        // Cancel any pending calls
        client.dispatcher().cancelAll();

        log.info("HTTP client closed");
    }
}
