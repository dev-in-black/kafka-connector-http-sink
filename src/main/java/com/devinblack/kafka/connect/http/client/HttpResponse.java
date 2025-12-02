package com.devinblack.kafka.connect.http.client;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

/**
 * Encapsulates an HTTP response with status code, headers, body, and timing information.
 */
public class HttpResponse {
    private final int statusCode;
    private final Map<String, String> headers;
    private final String body;
    private final long responseTimeMs;

    public HttpResponse(int statusCode, Map<String, String> headers, String body, long responseTimeMs) {
        this.statusCode = statusCode;
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
        this.body = body;
        this.responseTimeMs = responseTimeMs;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Map<String, String> getHeaders() {
        return Collections.unmodifiableMap(headers);
    }

    public String getBody() {
        return body;
    }

    public long getResponseTimeMs() {
        return responseTimeMs;
    }

    /**
     * Check if the response status code indicates success (2xx).
     */
    public boolean isSuccess() {
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * Check if the response status code indicates a client error (4xx).
     */
    public boolean isClientError() {
        return statusCode >= 400 && statusCode < 500;
    }

    /**
     * Check if the response status code indicates a server error (5xx).
     */
    public boolean isServerError() {
        return statusCode >= 500 && statusCode < 600;
    }

    /**
     * Check if the response indicates an error (4xx or 5xx).
     */
    public boolean isError() {
        return isClientError() || isServerError();
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "statusCode=" + statusCode +
                ", headers=" + headers.size() +
                ", bodyLength=" + (body != null ? body.length() : 0) +
                ", responseTimeMs=" + responseTimeMs +
                '}';
    }
}
