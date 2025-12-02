package com.devinblack.kafka.connect.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts Kafka SinkRecords to HTTP request body format.
 *
 * Handles multiple data formats:
 * - String values (JSON strings or plain text)
 * - Struct values (Kafka Connect Schema)
 * - Map values
 * - Primitive types (numbers, booleans)
 * - byte[] values
 *
 * The converter always produces JSON output for structured data.
 */
public class RecordConverter {
    private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

    private final ObjectMapper objectMapper;
    private final JsonConverter jsonConverter;
    private final HttpSinkConnectorConfig config;

    public RecordConverter(HttpSinkConnectorConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.jsonConverter = new JsonConverter();

        // Configure JsonConverter for value conversion
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", false);
        this.jsonConverter.configure(converterConfig, false);
    }

    /**
     * Convert a SinkRecord value to HTTP request body.
     *
     * @param record The Kafka sink record
     * @return The HTTP request body as a string
     * @throws ConversionException if conversion fails
     */
    public String convert(SinkRecord record) throws ConversionException {
        if (record.value() == null) {
            throw new ConversionException("Cannot convert null record value");
        }

        try {
            Object value = record.value();

            // Handle different value types
            if (value instanceof String) {
                return convertString((String) value);
            } else if (value instanceof Struct) {
                return convertStruct((Struct) value, record);
            } else if (value instanceof Map) {
                return convertMap((Map<?, ?>) value);
            } else if (value instanceof byte[]) {
                return convertBytes((byte[]) value);
            } else if (isPrimitive(value)) {
                return convertPrimitive(value);
            } else {
                // Fallback: try to serialize with Jackson
                return objectMapper.writeValueAsString(value);
            }

        } catch (Exception e) {
            throw new ConversionException(
                "Failed to convert record from topic=" + record.topic() +
                " partition=" + record.kafkaPartition() +
                " offset=" + record.kafkaOffset(), e);
        }
    }

    /**
     * Convert a String value.
     * If the string is already valid JSON, return as-is.
     * Otherwise, wrap it in a JSON object.
     */
    private String convertString(String value) throws Exception {
        // Try to parse as JSON
        try {
            JsonNode node = objectMapper.readTree(value);
            // If it's a valid JSON object or array, return as-is
            if (node.isObject() || node.isArray()) {
                return value;
            }
            // If it's a primitive JSON value (string, number, boolean), wrap it
            ObjectNode wrapper = objectMapper.createObjectNode();
            wrapper.set("value", node);
            return objectMapper.writeValueAsString(wrapper);
        } catch (Exception e) {
            // Not valid JSON, treat as plain string and wrap it
            ObjectNode wrapper = objectMapper.createObjectNode();
            wrapper.put("value", value);
            return objectMapper.writeValueAsString(wrapper);
        }
    }

    /**
     * Convert a Struct (Kafka Connect Schema) value.
     */
    private String convertStruct(Struct struct, SinkRecord record) throws Exception {
        // Use JsonConverter to convert Struct to JSON
        byte[] jsonBytes = jsonConverter.fromConnectData(
            record.topic(),
            record.valueSchema(),
            struct
        );
        return new String(jsonBytes, StandardCharsets.UTF_8);
    }

    /**
     * Convert a Map value.
     */
    private String convertMap(Map<?, ?> map) throws Exception {
        return objectMapper.writeValueAsString(map);
    }

    /**
     * Convert a byte array.
     * Attempts to interpret as UTF-8 string first, then as JSON.
     */
    private String convertBytes(byte[] bytes) throws Exception {
        String str = new String(bytes, StandardCharsets.UTF_8);
        // Try to parse as JSON
        try {
            JsonNode node = objectMapper.readTree(str);
            if (node.isObject() || node.isArray()) {
                return str;
            }
        } catch (Exception e) {
            // Not JSON, wrap the string
        }

        // Wrap in JSON object
        ObjectNode wrapper = objectMapper.createObjectNode();
        wrapper.put("value", str);
        return objectMapper.writeValueAsString(wrapper);
    }

    /**
     * Convert a primitive value (number, boolean, null).
     */
    private String convertPrimitive(Object value) throws Exception {
        ObjectNode wrapper = objectMapper.createObjectNode();

        if (value instanceof Integer) {
            wrapper.put("value", (Integer) value);
        } else if (value instanceof Long) {
            wrapper.put("value", (Long) value);
        } else if (value instanceof Double) {
            wrapper.put("value", (Double) value);
        } else if (value instanceof Float) {
            wrapper.put("value", (Float) value);
        } else if (value instanceof Boolean) {
            wrapper.put("value", (Boolean) value);
        } else if (value instanceof Short) {
            wrapper.put("value", (Short) value);
        } else if (value instanceof Byte) {
            wrapper.put("value", (Byte) value);
        } else {
            wrapper.put("value", value.toString());
        }

        return objectMapper.writeValueAsString(wrapper);
    }

    /**
     * Check if a value is a primitive type.
     */
    private boolean isPrimitive(Object value) {
        return value instanceof Number ||
               value instanceof Boolean ||
               value instanceof Character;
    }

    /**
     * Exception thrown when record conversion fails.
     */
    public static class ConversionException extends Exception {
        public ConversionException(String message) {
            super(message);
        }

        public ConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
