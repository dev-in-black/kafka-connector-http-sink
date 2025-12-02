package com.devinblack.kafka.connect.http;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Converts Kafka record headers to HTTP request headers.
 *
 * Supports:
 * - Include/exclude filtering (whitelist/blacklist)
 * - Header name prefix
 * - Multiple value types (String, byte[], primitives)
 * - Header name transformation (lowercase, sanitization)
 *
 * Filtering rules:
 * 1. If include list is not empty, only included headers are forwarded
 * 2. Exclude list is applied after include list
 * 3. Both lists support wildcards (*)
 */
public class HeaderConverter {
    private static final Logger log = LoggerFactory.getLogger(HeaderConverter.class);

    private final HttpSinkConnectorConfig config;
    private final List<String> includePatterns;
    private final List<String> excludePatterns;
    private final String prefix;
    private final boolean enabled;

    public HeaderConverter(HttpSinkConnectorConfig config) {
        this.config = config;
        this.enabled = config.isHeaderForwardEnabled();

        // Handle potential null values from config
        List<String> includes = config.getHeadersForwardInclude();
        List<String> excludes = config.getHeadersForwardExclude();

        this.includePatterns = (includes != null) ? includes : java.util.Collections.emptyList();
        this.excludePatterns = (excludes != null) ? excludes : java.util.Collections.emptyList();
        this.prefix = config.getHeadersForwardPrefix();

        log.debug("HeaderConverter initialized: enabled={}, include={}, exclude={}, prefix={}",
            enabled, includePatterns, excludePatterns, prefix);
    }

    /**
     * Convert Kafka headers to HTTP headers.
     *
     * @param kafkaHeaders Kafka record headers
     * @return Map of HTTP header names to values
     */
    public Map<String, String> convert(Headers kafkaHeaders) {
        Map<String, String> httpHeaders = new HashMap<>();

        if (!enabled || kafkaHeaders == null) {
            return httpHeaders;
        }

        for (Header header : kafkaHeaders) {
            String headerName = header.key();

            // Apply filtering rules
            if (!shouldInclude(headerName)) {
                log.trace("Header '{}' excluded by filtering rules", headerName);
                continue;
            }

            // Convert header value
            String headerValue = convertHeaderValue(header);
            if (headerValue == null) {
                log.trace("Skipping header '{}' with null value", headerName);
                continue;
            }

            // Apply prefix and sanitize name
            String httpHeaderName = transformHeaderName(headerName);

            // Add to HTTP headers
            // If header already exists, concatenate with comma (HTTP spec)
            if (httpHeaders.containsKey(httpHeaderName)) {
                String existingValue = httpHeaders.get(httpHeaderName);
                httpHeaders.put(httpHeaderName, existingValue + "," + headerValue);
                log.trace("Appended to existing header '{}': {}", httpHeaderName, headerValue);
            } else {
                httpHeaders.put(httpHeaderName, headerValue);
                log.trace("Forwarding header '{}': {}", httpHeaderName, headerValue);
            }
        }

        log.debug("Converted {} Kafka headers to {} HTTP headers",
            getHeaderCount(kafkaHeaders), httpHeaders.size());

        return httpHeaders;
    }

    /**
     * Check if a header should be included based on filtering rules.
     */
    private boolean shouldInclude(String headerName) {
        // If include list is not empty, header must match at least one pattern
        if (!includePatterns.isEmpty()) {
            boolean matched = false;
            for (String pattern : includePatterns) {
                if (matchesPattern(headerName, pattern)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }

        // Check exclude list
        for (String pattern : excludePatterns) {
            if (matchesPattern(headerName, pattern)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a header name matches a pattern.
     * Supports wildcards (*).
     */
    private boolean matchesPattern(String headerName, String pattern) {
        if (pattern.equals("*")) {
            return true;
        }

        // Convert wildcard pattern to regex
        String regex = pattern
            .replace(".", "\\.")
            .replace("*", ".*");

        return Pattern.matches(regex, headerName);
    }

    /**
     * Transform header name: add prefix and sanitize.
     */
    private String transformHeaderName(String headerName) {
        String transformed = headerName;

        // Add prefix if configured
        if (prefix != null && !prefix.isEmpty()) {
            transformed = prefix + transformed;
        }

        // Sanitize header name:
        // - HTTP headers are case-insensitive, but we preserve case
        // - Replace invalid characters with hyphens
        transformed = sanitizeHeaderName(transformed);

        return transformed;
    }

    /**
     * Sanitize header name to conform to HTTP specification.
     * Replaces invalid characters with hyphens.
     */
    private String sanitizeHeaderName(String name) {
        // HTTP header names can contain: letters, digits, and hyphens
        // First character must be a letter
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (isValidHeaderChar(c)) {
                sb.append(c);
            } else {
                sb.append('-');
            }
        }

        String sanitized = sb.toString();

        // Ensure first character is a letter
        if (!sanitized.isEmpty() && !Character.isLetter(sanitized.charAt(0))) {
            sanitized = "X-" + sanitized;
        }

        return sanitized;
    }

    /**
     * Check if a character is valid for HTTP header names.
     */
    private boolean isValidHeaderChar(char c) {
        return Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.';
    }

    /**
     * Convert header value to string.
     * Handles various Kafka header value types.
     */
    private String convertHeaderValue(Header header) {
        if (header.value() == null) {
            return null;
        }

        Object value = header.value();

        try {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof byte[]) {
                return new String((byte[]) value, StandardCharsets.UTF_8);
            } else if (value instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            } else if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            } else {
                // Fallback: use toString()
                return value.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to convert header '{}' value of type {}: {}",
                header.key(), value.getClass().getName(), e.getMessage());
            return null;
        }
    }

    /**
     * Count the number of headers.
     */
    private int getHeaderCount(Headers headers) {
        int count = 0;
        for (Header h : headers) {
            count++;
        }
        return count;
    }

    /**
     * Check if header forwarding is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }
}
