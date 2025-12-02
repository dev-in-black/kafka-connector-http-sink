package com.devinblack.kafka.connect.http;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HeaderConverter.
 */
class HeaderConverterTest {

    @Test
    void testConvertBasicHeaders() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("header1"));
        assertEquals("value2", result.get("header2"));
    }

    @Test
    void testConvertWithPrefix() {
        HeaderConverter converter = createConverter(true, "", "", "X-Kafka-");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("X-Kafka-header1"));
        assertEquals("value2", result.get("X-Kafka-header2"));
    }

    @Test
    void testConvertDisabled() {
        HeaderConverter converter = createConverter(false, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(0, result.size());
    }

    @Test
    void testConvertWithIncludeFilter() {
        HeaderConverter converter = createConverter(true, "header1,header3", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2")
            .addString("header3", "value3");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("header1"));
        assertEquals("value3", result.get("header3"));
        assertFalse(result.containsKey("header2"));
    }

    @Test
    void testConvertWithExcludeFilter() {
        HeaderConverter converter = createConverter(true, "", "header2", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2")
            .addString("header3", "value3");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("header1"));
        assertEquals("value3", result.get("header3"));
        assertFalse(result.containsKey("header2"));
    }

    @Test
    void testConvertWithIncludeAndExcludeFilter() {
        // Include takes precedence, then exclude is applied
        HeaderConverter converter = createConverter(true, "header*", "header2", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2")
            .addString("header3", "value3")
            .addString("other", "value4");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("value1", result.get("header1"));
        assertEquals("value3", result.get("header3"));
        assertFalse(result.containsKey("header2"));
        assertFalse(result.containsKey("other"));
    }

    @Test
    void testConvertWithWildcardInclude() {
        HeaderConverter converter = createConverter(true, "x-*", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("x-trace-id", "123")
            .addString("x-request-id", "456")
            .addString("content-type", "application/json");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("123", result.get("x-trace-id"));
        assertEquals("456", result.get("x-request-id"));
        assertFalse(result.containsKey("content-type"));
    }

    @Test
    void testConvertWithWildcardExclude() {
        HeaderConverter converter = createConverter(true, "", "internal-*", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("internal-debug", "debug")
            .addString("internal-trace", "trace");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        assertEquals("value1", result.get("header1"));
        assertFalse(result.containsKey("internal-debug"));
        assertFalse(result.containsKey("internal-trace"));
    }

    @Test
    void testConvertByteArrayHeader() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addBytes("header1", "value1".getBytes());

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        assertEquals("value1", result.get("header1"));
    }

    @Test
    void testConvertNumericHeader() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addInt("count", 42)
            .addLong("timestamp", 1234567890L);

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("42", result.get("count"));
        assertEquals("1234567890", result.get("timestamp"));
    }

    @Test
    void testConvertBooleanHeader() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addBoolean("active", true);

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        assertEquals("true", result.get("active"));
    }

    @Test
    void testConvertNullHeaders() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Map<String, String> result = converter.convert(null);

        assertEquals(0, result.size());
    }

    @Test
    void testConvertEmptyHeaders() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders();

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(0, result.size());
    }

    @Test
    void testConvertDuplicateHeaders() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header1", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        // Duplicate headers should be concatenated with comma
        assertEquals(1, result.size());
        assertEquals("value1,value2", result.get("header1"));
    }

    @Test
    void testHeaderNameSanitization() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header.with.dots", "value1")
            .addString("header_with_underscores", "value2")
            .addString("header-with-hyphens", "value3");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(3, result.size());
        assertTrue(result.containsKey("header.with.dots"));
        assertTrue(result.containsKey("header_with_underscores"));
        assertTrue(result.containsKey("header-with-hyphens"));
    }

    @Test
    void testHeaderNameSanitizationInvalidChars() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header@with#special", "value1");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        // Invalid characters should be replaced with hyphens
        assertTrue(result.containsKey("header-with-special"));
    }

    @Test
    void testHeaderNameSanitizationStartsWithNonLetter() {
        HeaderConverter converter = createConverter(true, "", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("123-header", "value1");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        // Should add "X-" prefix if starts with non-letter
        assertTrue(result.containsKey("X-123-header"));
    }

    @Test
    void testConvertWithPrefixAndSanitization() {
        HeaderConverter converter = createConverter(true, "", "", "Kafka-");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header@with#special", "value1");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(1, result.size());
        assertTrue(result.containsKey("Kafka-header-with-special"));
    }

    @Test
    void testIsEnabled() {
        HeaderConverter enabledConverter = createConverter(true, "", "", "");
        assertTrue(enabledConverter.isEnabled());

        HeaderConverter disabledConverter = createConverter(false, "", "", "");
        assertFalse(disabledConverter.isEnabled());
    }

    @Test
    void testConvertAllWildcard() {
        HeaderConverter converter = createConverter(true, "*", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
    }

    @Test
    void testConvertExcludeAllWildcard() {
        HeaderConverter converter = createConverter(true, "", "*", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("header1", "value1")
            .addString("header2", "value2");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(0, result.size());
    }

    @Test
    void testConvertMultipleWildcardPatterns() {
        HeaderConverter converter = createConverter(true, "x-*,trace-*", "", "");

        Headers kafkaHeaders = new ConnectHeaders()
            .addString("x-request-id", "123")
            .addString("trace-id", "456")
            .addString("content-type", "json")
            .addString("other", "value");

        Map<String, String> result = converter.convert(kafkaHeaders);

        assertEquals(2, result.size());
        assertEquals("123", result.get("x-request-id"));
        assertEquals("456", result.get("trace-id"));
    }

    // Helper method to create HeaderConverter with config
    private HeaderConverter createConverter(boolean enabled, String include, String exclude, String prefix) {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_ENABLED, String.valueOf(enabled));
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_INCLUDE, include);
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_EXCLUDE, exclude);
        props.put(HttpSinkConnectorConfig.HEADERS_FORWARD_PREFIX, prefix);

        HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
        return new HeaderConverter(config);
    }
}
