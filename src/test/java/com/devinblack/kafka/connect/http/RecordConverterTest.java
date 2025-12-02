package com.devinblack.kafka.connect.http;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RecordConverter.
 */
class RecordConverterTest {

    private RecordConverter converter;
    private ObjectMapper objectMapper;
    private HttpSinkConnectorConfig config;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL, "http://localhost:8080");
        config = new HttpSinkConnectorConfig(props);
        converter = new RecordConverter(config);
        objectMapper = new ObjectMapper();
    }

    @Test
    void testConvertJsonString() throws Exception {
        // Valid JSON object
        String jsonValue = "{\"name\":\"John\",\"age\":30}";
        SinkRecord record = createRecord(jsonValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals("John", node.get("name").asText());
        assertEquals(30, node.get("age").asInt());
    }

    @Test
    void testConvertJsonArray() throws Exception {
        // Valid JSON array
        String jsonValue = "[{\"id\":1},{\"id\":2}]";
        SinkRecord record = createRecord(jsonValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isArray());
        assertEquals(2, node.size());
    }

    @Test
    void testConvertPlainString() throws Exception {
        // Plain string (not JSON)
        String plainValue = "Hello World";
        SinkRecord record = createRecord(plainValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals("Hello World", node.get("value").asText());
    }

    @Test
    void testConvertMap() throws Exception {
        // Map value
        Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("key1", "value1");
        mapValue.put("key2", 123);
        mapValue.put("key3", true);

        SinkRecord record = createRecord(mapValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals("value1", node.get("key1").asText());
        assertEquals(123, node.get("key2").asInt());
        assertTrue(node.get("key3").asBoolean());
    }

    @Test
    void testConvertStruct() throws Exception {
        // Create schema and struct
        Schema schema = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .build();

        Struct structValue = new Struct(schema)
            .put("name", "Alice")
            .put("age", 25)
            .put("active", true);

        SinkRecord record = new SinkRecord(
            "test-topic",
            0,
            null,
            null,
            schema,
            structValue,
            0L
        );

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals("Alice", node.get("name").asText());
        assertEquals(25, node.get("age").asInt());
        assertTrue(node.get("active").asBoolean());
    }

    @Test
    void testConvertInteger() throws Exception {
        Integer intValue = 42;
        SinkRecord record = createRecord(intValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals(42, node.get("value").asInt());
    }

    @Test
    void testConvertLong() throws Exception {
        Long longValue = 123456789L;
        SinkRecord record = createRecord(longValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals(123456789L, node.get("value").asLong());
    }

    @Test
    void testConvertDouble() throws Exception {
        Double doubleValue = 3.14159;
        SinkRecord record = createRecord(doubleValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals(3.14159, node.get("value").asDouble(), 0.00001);
    }

    @Test
    void testConvertBoolean() throws Exception {
        Boolean boolValue = true;
        SinkRecord record = createRecord(boolValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertTrue(node.get("value").asBoolean());
    }

    @Test
    void testConvertByteArrayJson() throws Exception {
        // Byte array containing JSON
        String jsonString = "{\"message\":\"test\"}";
        byte[] byteValue = jsonString.getBytes();
        SinkRecord record = createRecord(byteValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals("test", node.get("message").asText());
    }

    @Test
    void testConvertByteArrayPlainText() throws Exception {
        // Byte array containing plain text
        String plainString = "plain text message";
        byte[] byteValue = plainString.getBytes();
        SinkRecord record = createRecord(byteValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals("plain text message", node.get("value").asText());
    }

    @Test
    void testConvertNullValue() {
        SinkRecord record = createRecord(null);

        assertThrows(RecordConverter.ConversionException.class, () -> {
            converter.convert(record);
        });
    }

    @Test
    void testConvertComplexNestedMap() throws Exception {
        // Complex nested structure
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("city", "New York");
        innerMap.put("zip", "10001");

        Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("name", "John");
        mapValue.put("age", 30);
        mapValue.put("address", innerMap);

        SinkRecord record = createRecord(mapValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals("John", node.get("name").asText());
        assertEquals(30, node.get("age").asInt());
        assertTrue(node.get("address").isObject());
        assertEquals("New York", node.get("address").get("city").asText());
        assertEquals("10001", node.get("address").get("zip").asText());
    }

    @Test
    void testConvertEmptyMap() throws Exception {
        Map<String, Object> emptyMap = new HashMap<>();
        SinkRecord record = createRecord(emptyMap);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        assertTrue(node.isObject());
        assertEquals(0, node.size());
    }

    @Test
    void testConvertJsonPrimitiveString() throws Exception {
        // JSON string primitive (quoted string)
        String jsonValue = "\"hello\"";
        SinkRecord record = createRecord(jsonValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        // Should be wrapped in an object
        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals("hello", node.get("value").asText());
    }

    @Test
    void testConvertJsonNumber() throws Exception {
        // JSON number
        String jsonValue = "42";
        SinkRecord record = createRecord(jsonValue);

        String result = converter.convert(record);
        JsonNode node = objectMapper.readTree(result);

        // Should be wrapped in an object
        assertTrue(node.isObject());
        assertTrue(node.has("value"));
        assertEquals(42, node.get("value").asInt());
    }

    // Helper method to create a SinkRecord
    private SinkRecord createRecord(Object value) {
        return new SinkRecord(
            "test-topic",
            0,
            null,
            null,
            null,
            value,
            0L
        );
    }
}
