package com.devinblack.kafka.connect.http.client;

import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Fluent builder for creating HTTP requests.
 * Supports POST, PUT, and DELETE methods with headers and body.
 */
public class HttpRequestBuilder {
    private String url;
    private String method = "POST";
    private final Map<String, String> headers = new HashMap<>();
    private String body;
    private String contentType = "application/json";

    public HttpRequestBuilder url(String url) {
        this.url = url;
        return this;
    }

    public HttpRequestBuilder method(String method) {
        this.method = method.toUpperCase();
        if (!method.equals("POST") && !method.equals("PUT") && !method.equals("DELETE")) {
            throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }
        return this;
    }

    public HttpRequestBuilder header(String name, String value) {
        if (name != null && value != null) {
            this.headers.put(name, value);
        }
        return this;
    }

    public HttpRequestBuilder headers(Map<String, String> headers) {
        if (headers != null) {
            this.headers.putAll(headers);
        }
        return this;
    }

    public HttpRequestBuilder body(String body) {
        this.body = body;
        return this;
    }

    public HttpRequestBuilder contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Build the OkHttp Request object.
     */
    public Request build() {
        if (url == null || url.isEmpty()) {
            throw new IllegalStateException("URL is required");
        }

        Request.Builder builder = new Request.Builder().url(url);

        // Add headers
        headers.forEach(builder::addHeader);

        // Add Content-Type if body is present
        if (body != null && !headers.containsKey("Content-Type")) {
            builder.addHeader("Content-Type", contentType);
        }

        // Build request body
        RequestBody requestBody = null;
        if (body != null) {
            MediaType mediaType = MediaType.parse(contentType);
            // Use byte array to prevent OkHttp from appending charset=utf-8
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            requestBody = RequestBody.create(bodyBytes, mediaType);
        } else if ("POST".equals(method) || "PUT".equals(method)) {
            // Empty body for POST/PUT if no body specified
            requestBody = RequestBody.create(new byte[0], MediaType.parse("application/json"));
        }

        // Set method and body
        switch (method) {
            case "POST":
                builder.post(requestBody);
                break;
            case "PUT":
                builder.put(requestBody);
                break;
            case "DELETE":
                if (requestBody != null) {
                    builder.delete(requestBody);
                } else {
                    builder.delete();
                }
                break;
            default:
                throw new IllegalStateException("Unsupported method: " + method);
        }

        return builder.build();
    }

    /**
     * Get the configured URL.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Get the configured method.
     */
    public String getMethod() {
        return method;
    }

    /**
     * Get the configured headers.
     */
    public Map<String, String> getHeaders() {
        return new HashMap<>(headers);
    }

    /**
     * Get the configured body.
     */
    public String getBody() {
        return body;
    }
}
