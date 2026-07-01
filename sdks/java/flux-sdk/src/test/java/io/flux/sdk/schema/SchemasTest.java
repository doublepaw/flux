// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk.schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemasTest {

    // ---- test model classes ----

    @FluxSchema
    public static class Primitives {
        public String name;
        public int count;
        public long bigCount;
        public float ratio;
        public double precise;
        public boolean active;
        public byte[] data;

        public Primitives() {}
    }

    @FluxSchema
    public static class WithNonNull {
        @NonNull public String required;
        public String optional;

        public WithNonNull() {}
    }

    @FluxSchema
    public static class WithCollections {
        public List<String> tags;
        public Map<String, Integer> scores;

        public WithCollections() {}
    }

    public enum Color { RED, GREEN, BLUE }

    @FluxSchema
    public static class WithEnum {
        public Color color;

        public WithEnum() {}
    }

    @FluxSchema
    public static class Address {
        public String street;
        public String city;

        public Address() {}
    }

    @FluxSchema
    public static class WithNested {
        public Address address;

        public WithNested() {}
    }

    @FluxSchema(
        namespace = "com.example",
        renames = {@Rename(from = "old_name", to = "newName")},
        deletions = {"removedField"}
    )
    public static class WithEvolution {
        public String id;
        public int newName;

        public WithEvolution() {}
    }

    // Bad models for validation tests
    @FluxSchema(renames = {@Rename(from = "x", to = "missing")})
    public static class BadRenameTarget {
        public String id;
        public BadRenameTarget() {}
    }

    @FluxSchema(deletions = {"id"})
    public static class BadDeletion {
        public String id;
        public BadDeletion() {}
    }

    public static class NoAnnotation {
        public String id;
    }

    @FluxSchema
    public static class BadMapKey {
        public Map<Integer, String> badMap;
        public BadMapKey() {}
    }

    // ---- schema generation tests ----

    @Test
    void testPrimitiveTypes() {
        Map<String, Object> schema = Schemas.schema(Primitives.class);
        assertEquals("record", schema.get("type"));
        assertEquals("Primitives", schema.get("name"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");
        assertEquals(7, fields.size());

        // All fields nullable by default
        assertFieldType(fields, "name", "string");
        assertFieldType(fields, "count", "int");
        assertFieldType(fields, "bigCount", "long");
        assertFieldType(fields, "ratio", "float");
        assertFieldType(fields, "precise", "double");
        assertFieldType(fields, "active", "boolean");
        assertFieldType(fields, "data", "bytes");
    }

    @Test
    void testNonNullOverride() {
        Map<String, Object> schema = Schemas.schema(WithNonNull.class);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");

        // @NonNull field: plain type, no union
        Map<String, Object> required = findField(fields, "required");
        assertEquals("string", required.get("type"));
        assertFalse(required.containsKey("default"));

        // Default field: nullable union
        Map<String, Object> optional = findField(fields, "optional");
        assertNullableUnion(optional, "string");
    }

    @Test
    void testCollections() {
        Map<String, Object> schema = Schemas.schema(WithCollections.class);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");

        // tags: nullable array
        Map<String, Object> tags = findField(fields, "tags");
        @SuppressWarnings("unchecked")
        List<Object> tagsType = (List<Object>) tags.get("type");
        assertEquals("null", tagsType.get(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> arraySchema = (Map<String, Object>) tagsType.get(1);
        assertEquals("array", arraySchema.get("type"));
        assertEquals("string", arraySchema.get("items"));

        // scores: nullable map
        Map<String, Object> scores = findField(fields, "scores");
        @SuppressWarnings("unchecked")
        List<Object> scoresType = (List<Object>) scores.get("type");
        assertEquals("null", scoresType.get(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> mapSchema = (Map<String, Object>) scoresType.get(1);
        assertEquals("map", mapSchema.get("type"));
        assertEquals("int", mapSchema.get("values"));
    }

    @Test
    void testNonStringMapKeyThrows() {
        assertThrows(SchemaException.class, () -> Schemas.schema(BadMapKey.class));
    }

    @Test
    void testEnum() {
        Map<String, Object> schema = Schemas.schema(WithEnum.class);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");
        Map<String, Object> color = findField(fields, "color");

        @SuppressWarnings("unchecked")
        List<Object> colorType = (List<Object>) color.get("type");
        assertEquals("null", colorType.get(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> enumSchema = (Map<String, Object>) colorType.get(1);
        assertEquals("enum", enumSchema.get("type"));
        assertEquals("Color", enumSchema.get("name"));
        assertEquals(List.of("RED", "GREEN", "BLUE"), enumSchema.get("symbols"));
    }

    @Test
    void testNestedSchema() {
        Map<String, Object> schema = Schemas.schema(WithNested.class);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");
        Map<String, Object> address = findField(fields, "address");

        @SuppressWarnings("unchecked")
        List<Object> addrType = (List<Object>) address.get("type");
        assertEquals("null", addrType.get(0));
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedRecord = (Map<String, Object>) addrType.get(1);
        assertEquals("record", nestedRecord.get("type"));
        assertEquals("Address", nestedRecord.get("name"));
    }

    @Test
    void testEvolutionMetadata() {
        Map<String, Object> schema = Schemas.schema(WithEvolution.class);
        assertEquals("com.example", schema.get("namespace"));

        @SuppressWarnings("unchecked")
        Map<String, String> renames = (Map<String, String>) schema.get("flux.renames");
        assertEquals("newName", renames.get("old_name"));

        @SuppressWarnings("unchecked")
        List<String> deletions = (List<String>) schema.get("flux.deletions");
        assertTrue(deletions.contains("removedField"));

        // Check alias on renamed field
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");
        Map<String, Object> newName = findField(fields, "newName");
        @SuppressWarnings("unchecked")
        List<String> aliases = (List<String>) newName.get("aliases");
        assertEquals(List.of("old_name"), aliases);
    }

    @Test
    void testSchemaJsonRoundtrips() {
        String json = Schemas.schemaJson(Primitives.class);
        assertNotNull(json);
        assertTrue(json.contains("\"name\": \"Primitives\""));
        assertTrue(json.contains("\"type\": \"record\""));
    }

    // ---- validation tests ----

    @Test
    void testNoAnnotationThrows() {
        assertThrows(SchemaException.class, () -> Schemas.schema(NoAnnotation.class));
    }

    @Test
    void testBadRenameTargetThrows() {
        assertThrows(SchemaException.class, () -> Schemas.schema(BadRenameTarget.class));
    }

    @Test
    void testDeletionStillExistsThrows() {
        assertThrows(SchemaException.class, () -> Schemas.schema(BadDeletion.class));
    }

    // ---- serde roundtrip tests ----

    @Test
    void testRoundtripPrimitives() {
        Primitives obj = new Primitives();
        obj.name = "test";
        obj.count = 42;
        obj.bigCount = 123456789L;
        obj.ratio = 3.14f;
        obj.precise = 2.718281828;
        obj.active = true;
        obj.data = new byte[]{1, 2, 3};

        byte[] bytes = Schemas.toBytes(obj);
        Primitives restored = Schemas.fromBytes(Primitives.class, bytes);

        assertEquals("test", restored.name);
        assertEquals(42, restored.count);
        assertEquals(123456789L, restored.bigCount);
        assertEquals(3.14f, restored.ratio);
        assertEquals(2.718281828, restored.precise);
        assertTrue(restored.active);
        assertArrayEquals(new byte[]{1, 2, 3}, restored.data);
    }

    @Test
    void testRoundtripNullableFields() {
        WithNonNull obj = new WithNonNull();
        obj.required = "hello";
        obj.optional = null;

        byte[] bytes = Schemas.toBytes(obj);
        WithNonNull restored = Schemas.fromBytes(WithNonNull.class, bytes);

        assertEquals("hello", restored.required);
        assertNull(restored.optional);
    }

    @Test
    void testRoundtripCollections() {
        WithCollections obj = new WithCollections();
        obj.tags = Arrays.asList("a", "b", "c");
        obj.scores = new HashMap<>();
        obj.scores.put("x", 10);
        obj.scores.put("y", 20);

        byte[] bytes = Schemas.toBytes(obj);
        WithCollections restored = Schemas.fromBytes(WithCollections.class, bytes);

        assertEquals(Arrays.asList("a", "b", "c"), restored.tags);
        assertEquals(10, restored.scores.get("x"));
        assertEquals(20, restored.scores.get("y"));
    }

    @Test
    void testRoundtripEnum() {
        WithEnum obj = new WithEnum();
        obj.color = Color.GREEN;

        byte[] bytes = Schemas.toBytes(obj);
        WithEnum restored = Schemas.fromBytes(WithEnum.class, bytes);

        assertEquals(Color.GREEN, restored.color);
    }

    @Test
    void testRoundtripNested() {
        Address addr = new Address();
        addr.street = "123 Main St";
        addr.city = "Springfield";

        WithNested obj = new WithNested();
        obj.address = addr;

        byte[] bytes = Schemas.toBytes(obj);
        WithNested restored = Schemas.fromBytes(WithNested.class, bytes);

        assertNotNull(restored.address);
        assertEquals("123 Main St", restored.address.street);
        assertEquals("Springfield", restored.address.city);
    }

    @Test
    void testRoundtripNullValues() {
        Primitives obj = new Primitives();
        obj.name = null;
        obj.data = null;

        byte[] bytes = Schemas.toBytes(obj);
        Primitives restored = Schemas.fromBytes(Primitives.class, bytes);

        assertNull(restored.name);
        assertNull(restored.data);
    }

    // ---- helpers ----

    @SuppressWarnings("unchecked")
    private void assertFieldType(List<Map<String, Object>> fields, String name, String expectedInner) {
        Map<String, Object> field = findField(fields, name);
        // All fields nullable by default
        List<Object> union = (List<Object>) field.get("type");
        assertEquals("null", union.get(0));
        assertEquals(expectedInner, union.get(1));
    }

    private void assertNullableUnion(Map<String, Object> field, String expectedInner) {
        @SuppressWarnings("unchecked")
        List<Object> union = (List<Object>) field.get("type");
        assertEquals("null", union.get(0));
        assertEquals(expectedInner, union.get(1));
    }

    private Map<String, Object> findField(List<Map<String, Object>> fields, String name) {
        return fields.stream()
            .filter(f -> name.equals(f.get("name")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("field not found: " + name));
    }
}