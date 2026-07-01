// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk.schema;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Static utility for schema generation and schemaless serde
 * from classes annotated with {@link FluxSchema}.
 */
public final class Schemas {

    private Schemas() {}

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final ConcurrentHashMap<Class<?>, CachedSchema> CACHE = new ConcurrentHashMap<>();

    private static class CachedSchema {
        final Map<String, Object> schemaMap;
        final Schema parsed;

        CachedSchema(Map<String, Object> schemaMap, Schema parsed) {
            this.schemaMap = schemaMap;
            this.parsed = parsed;
        }
    }

    /** Return the Avro schema as a map for the given annotated class. */
    public static Map<String, Object> schema(Class<?> cls) {
        return cached(cls).schemaMap;
    }

    /** Return the Avro schema as formatted JSON. */
    public static String schemaJson(Class<?> cls) {
        return GSON.toJson(cached(cls).schemaMap);
    }

    /** Serialize an annotated object to binary (schemaless Avro). */
    public static byte[] toBytes(Object obj) {
        Class<?> cls = obj.getClass();
        CachedSchema cs = cached(cls);
        GenericRecord record = toGenericRecord(obj, cs.parsed);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(cs.parsed).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new SchemaException("serialization failed: " + e.getMessage());
        }
    }

    /** Deserialize binary back into an annotated class instance. */
    public static <T> T fromBytes(Class<T> cls, byte[] data) {
        CachedSchema cs = cached(cls);
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(cs.parsed).read(null, decoder);
            return fromGenericRecord(cls, record, cs.parsed);
        } catch (Exception e) {
            throw new SchemaException("deserialization failed: " + e.getMessage());
        }
    }

    // ---- schema generation ----

    private static CachedSchema cached(Class<?> cls) {
        return CACHE.computeIfAbsent(cls, Schemas::buildAndValidate);
    }

    private static CachedSchema buildAndValidate(Class<?> cls) {
        FluxSchema ann = cls.getAnnotation(FluxSchema.class);
        if (ann == null) {
            throw new SchemaException(cls.getName() + " is not annotated with @FluxSchema");
        }

        List<Field> fields = schemaFields(cls);
        Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());

        Map<String, String> renames = new LinkedHashMap<>();
        for (Rename r : ann.renames()) {
            renames.put(r.from(), r.to());
        }
        List<String> deletions = Arrays.asList(ann.deletions());

        // validate rename targets exist
        for (Map.Entry<String, String> e : renames.entrySet()) {
            if (!fieldNames.contains(e.getValue())) {
                throw new SchemaException(
                    "rename target '" + e.getValue() + "' is not a field on " + cls.getSimpleName());
            }
        }
        // validate deletions are not current fields
        for (String d : deletions) {
            if (fieldNames.contains(d)) {
                throw new SchemaException(
                    "deletion '" + d + "' still exists as a field on " + cls.getSimpleName());
            }
        }
        // validate no overlap
        for (String old : renames.keySet()) {
            if (deletions.contains(old)) {
                throw new SchemaException(
                    "'" + old + "' appears in both renames and deletions");
            }
        }

        Map<String, String> reverseRenames = new HashMap<>();
        for (Map.Entry<String, String> e : renames.entrySet()) {
            reverseRenames.put(e.getValue(), e.getKey());
        }

        Map<String, Object> schemaMap = buildSchemaMap(cls, ann, fields, renames, deletions, reverseRenames);
        Schema parsed = new Schema.Parser().parse(GSON.toJson(schemaMap));
        return new CachedSchema(schemaMap, parsed);
    }

    private static Map<String, Object> buildSchemaMap(
            Class<?> cls,
            FluxSchema ann,
            List<Field> fields,
            Map<String, String> renames,
            List<String> deletions,
            Map<String, String> reverseRenames) {

        List<Map<String, Object>> avroFields = new ArrayList<>();
        for (Field f : fields) {
            Map<String, Object> fieldDef = new LinkedHashMap<>();
            fieldDef.put("name", f.getName());
            boolean nullable = !f.isAnnotationPresent(NonNull.class);
            fieldDef.put("type", javaTypeToAvro(f.getType(), f.getGenericType(), nullable));

            if (reverseRenames.containsKey(f.getName())) {
                fieldDef.put("aliases", List.of(reverseRenames.get(f.getName())));
            }
            if (nullable) {
                fieldDef.put("default", null);
            }
            avroFields.add(fieldDef);
        }

        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "record");
        schema.put("name", cls.getSimpleName());
        if (!ann.namespace().isEmpty()) {
            schema.put("namespace", ann.namespace());
        }
        if (!renames.isEmpty()) {
            schema.put("flux.renames", renames);
        }
        if (!deletions.isEmpty()) {
            schema.put("flux.deletions", deletions);
        }
        schema.put("fields", avroFields);
        return schema;
    }

    private static Object javaTypeToAvro(Class<?> raw, Type generic, boolean nullable) {
        Object avro = javaTypeToAvroInner(raw, generic);
        if (nullable) {
            return List.of("null", avro);
        }
        return avro;
    }

    @SuppressWarnings("unchecked")
    private static Object javaTypeToAvroInner(Class<?> raw, Type generic) {
        // primitives and wrappers
        if (raw == String.class) return "string";
        if (raw == int.class || raw == Integer.class) return "int";
        if (raw == long.class || raw == Long.class) return "long";
        if (raw == float.class || raw == Float.class) return "float";
        if (raw == double.class || raw == Double.class) return "double";
        if (raw == boolean.class || raw == Boolean.class) return "boolean";
        if (raw == byte[].class) return "bytes";

        // List<T>
        if (List.class.isAssignableFrom(raw)) {
            if (!(generic instanceof ParameterizedType)) {
                throw new SchemaException("List fields must be parameterized");
            }
            Type itemType = ((ParameterizedType) generic).getActualTypeArguments()[0];
            Class<?> itemRaw = typeToClass(itemType);
            Map<String, Object> arr = new LinkedHashMap<>();
            arr.put("type", "array");
            arr.put("items", javaTypeToAvroInner(itemRaw, itemType));
            return arr;
        }

        // Map<String, T>
        if (Map.class.isAssignableFrom(raw)) {
            if (!(generic instanceof ParameterizedType)) {
                throw new SchemaException("Map fields must be parameterized");
            }
            Type[] args = ((ParameterizedType) generic).getActualTypeArguments();
            Class<?> keyRaw = typeToClass(args[0]);
            if (keyRaw != String.class) {
                throw new SchemaException("Avro maps require String keys, got " + keyRaw.getSimpleName());
            }
            Type valType = args[1];
            Class<?> valRaw = typeToClass(valType);
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("type", "map");
            map.put("values", javaTypeToAvroInner(valRaw, valType));
            return map;
        }

        // Java enum
        if (raw.isEnum()) {
            Object[] constants = raw.getEnumConstants();
            List<String> symbols = new ArrayList<>();
            for (Object c : constants) {
                symbols.add(((Enum<?>) c).name());
            }
            Map<String, Object> enumSchema = new LinkedHashMap<>();
            enumSchema.put("type", "enum");
            enumSchema.put("name", raw.getSimpleName());
            enumSchema.put("symbols", symbols);
            return enumSchema;
        }

        // Nested @FluxSchema class
        if (raw.isAnnotationPresent(FluxSchema.class)) {
            return schema(raw);
        }

        throw new SchemaException("unsupported type: " + raw.getName());
    }

    private static Class<?> typeToClass(Type type) {
        if (type instanceof Class) return (Class<?>) type;
        if (type instanceof ParameterizedType) return (Class<?>) ((ParameterizedType) type).getRawType();
        throw new SchemaException("unsupported generic type: " + type);
    }

    private static List<Field> schemaFields(Class<?> cls) {
        List<Field> result = new ArrayList<>();
        for (Field f : cls.getDeclaredFields()) {
            int mods = f.getModifiers();
            if (Modifier.isStatic(mods) || Modifier.isTransient(mods)) continue;
            f.setAccessible(true);
            result.add(f);
        }
        return result;
    }

    // ---- serde ----

    private static GenericRecord toGenericRecord(Object obj, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field sf : schema.getFields()) {
            try {
                Field javaField = obj.getClass().getDeclaredField(sf.name());
                javaField.setAccessible(true);
                Object val = javaField.get(obj);
                record.put(sf.name(), convertToAvro(val, sf.schema()));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new SchemaException("failed to read field " + sf.name() + ": " + e.getMessage());
            }
        }
        return record;
    }

    private static Object convertToAvro(Object val, Schema schema) {
        if (val == null) return null;
        switch (schema.getType()) {
            case UNION:
                // find the non-null branch
                for (Schema branch : schema.getTypes()) {
                    if (branch.getType() != Schema.Type.NULL) {
                        return convertToAvro(val, branch);
                    }
                }
                return null;
            case RECORD:
                return toGenericRecord(val, schema);
            case ARRAY:
                List<?> list = (List<?>) val;
                List<Object> avroList = new ArrayList<>(list.size());
                for (Object item : list) {
                    avroList.add(convertToAvro(item, schema.getElementType()));
                }
                return avroList;
            case MAP:
                @SuppressWarnings("unchecked")
                Map<String, ?> map = (Map<String, ?>) val;
                Map<String, Object> avroMap = new HashMap<>();
                for (Map.Entry<String, ?> e : map.entrySet()) {
                    avroMap.put(e.getKey(), convertToAvro(e.getValue(), schema.getValueType()));
                }
                return avroMap;
            case ENUM:
                return new GenericData.EnumSymbol(schema, ((Enum<?>) val).name());
            case BYTES:
                return ByteBuffer.wrap((byte[]) val);
            default:
                return val;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T fromGenericRecord(Class<T> cls, GenericRecord record, Schema schema) {
        try {
            T obj = cls.getDeclaredConstructor().newInstance();
            for (Schema.Field sf : schema.getFields()) {
                Field javaField = cls.getDeclaredField(sf.name());
                javaField.setAccessible(true);
                Object val = record.get(sf.name());
                javaField.set(obj, convertFromAvro(val, javaField.getType(), javaField.getGenericType(), sf.schema()));
            }
            return obj;
        } catch (ReflectiveOperationException e) {
            throw new SchemaException("deserialization failed: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static Object convertFromAvro(Object val, Class<?> raw, Type generic, Schema schema) {
        if (val == null) return null;

        // unwrap union
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() != Schema.Type.NULL) {
                    return convertFromAvro(val, raw, generic, branch);
                }
            }
            return null;
        }

        switch (schema.getType()) {
            case RECORD:
                if (val instanceof GenericRecord) {
                    return fromGenericRecord(raw, (GenericRecord) val, schema);
                }
                return val;
            case ARRAY:
                List<?> avroList = (List<?>) val;
                List<Object> javaList = new ArrayList<>(avroList.size());
                Type itemType = Object.class;
                Class<?> itemRaw = Object.class;
                if (generic instanceof ParameterizedType) {
                    itemType = ((ParameterizedType) generic).getActualTypeArguments()[0];
                    itemRaw = typeToClass(itemType);
                }
                for (Object item : avroList) {
                    javaList.add(convertFromAvro(item, itemRaw, itemType, schema.getElementType()));
                }
                return javaList;
            case MAP:
                Map<?, ?> avroMap = (Map<?, ?>) val;
                Map<String, Object> javaMap = new HashMap<>();
                Type valType = Object.class;
                Class<?> valRaw = Object.class;
                if (generic instanceof ParameterizedType) {
                    valType = ((ParameterizedType) generic).getActualTypeArguments()[1];
                    valRaw = typeToClass(valType);
                }
                for (Map.Entry<?, ?> e : avroMap.entrySet()) {
                    javaMap.put(e.getKey().toString(), convertFromAvro(e.getValue(), valRaw, valType, schema.getValueType()));
                }
                return javaMap;
            case ENUM:
                if (raw.isEnum()) {
                    String name = val.toString();
                    for (Object c : raw.getEnumConstants()) {
                        if (((Enum<?>) c).name().equals(name)) return c;
                    }
                }
                return val;
            case BYTES:
                if (val instanceof ByteBuffer) {
                    ByteBuffer bb = (ByteBuffer) val;
                    byte[] bytes = new byte[bb.remaining()];
                    bb.get(bytes);
                    return bytes;
                }
                return val;
            case STRING:
                // Avro may return CharSequence (Utf8)
                return val.toString();
            case INT:
                if (val instanceof Number) return ((Number) val).intValue();
                return val;
            case LONG:
                if (val instanceof Number) return ((Number) val).longValue();
                return val;
            case FLOAT:
                if (val instanceof Number) return ((Number) val).floatValue();
                return val;
            case DOUBLE:
                if (val instanceof Number) return ((Number) val).doubleValue();
                return val;
            default:
                return val;
        }
    }
}