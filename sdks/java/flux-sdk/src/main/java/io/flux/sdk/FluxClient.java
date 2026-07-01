// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import com.google.protobuf.ByteString;
import io.flux.sdk.proto.BatchAck;
import io.flux.sdk.proto.TopicResult;
import io.flux.sdk.proto.Record;
import io.flux.sdk.proto.RecordBatch;
import io.flux.sdk.schema.FluxSchema;
import io.flux.sdk.schema.Schemas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * High-level typed client. Handles schema registration, serialization,
 * and topic resolution automatically.
 *
 * <pre>{@code
 * try (FluxClient client = FluxClient.connect(new ClientConfig().apiKey("tb_..."))) {
 *     client.send(new OrderEvent("abc", 100));
 *     client.consume(OrderEvent.class, "my-group", event -> {
 *         System.out.println(event.orderId);
 *     });
 * }
 * }</pre>
 */
public class FluxClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FluxClient.class);

    private final Writer writer;
    private final AdminClient admin;
    private final ClientConfig config;
    private final ConcurrentHashMap<String, Integer> topicCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, Integer> schemaCache = new ConcurrentHashMap<>();

    private FluxClient(Writer writer, AdminClient admin, ClientConfig config) {
        this.writer = writer;
        this.admin = admin;
        this.config = config;
    }

    public static FluxClient connect(ClientConfig config) throws FluxException {
        WriterConfig writerConfig = new WriterConfig()
                .url(config.getWsUrl())
                .apiKey(config.getApiKey())
                .maxInFlight(config.getMaxInFlight())
                .timeout(config.getTimeout());
        Writer writer = Writer.connect(writerConfig);
        AdminClient admin = new AdminClient(config.getAdminUrl(), config.getApiKey(), config.getTimeout());
        return new FluxClient(writer, admin, config);
    }

    /** Send a single typed object. Topic resolved from @FluxSchema annotation. */
    public BatchAck send(Object obj) throws FluxException {
        return send(obj, (byte[]) null, null);
    }

    /** Send with key-based routing. */
    public BatchAck send(Object obj, byte[] key) throws FluxException {
        return send(obj, key, null);
    }

    /** Send with topic override. */
    public BatchAck sendToTopic(Object obj, String topic) throws FluxException {
        return send(obj, null, topic);
    }

    /**
     * Send a single typed object with full control over routing.
     *
     * @param obj   the object to send (must be annotated with @FluxSchema)
     * @param key   optional record key
     * @param topic optional topic name override
     */
    public BatchAck send(Object obj, byte[] key, String topic) throws FluxException {
        Class<?> cls = obj.getClass();
        String topicName = resolveTopic(cls, topic);
        int topicId = resolveTopicInfo(topicName);
        int schemaId = resolveSchemaId(cls, topicId);

        byte[] value = Schemas.toBytes(obj);
        Record.Builder recordBuilder = Record.newBuilder().setValue(ByteString.copyFrom(value));
        if (key != null) {
            recordBuilder.setKey(ByteString.copyFrom(key));
        }

        return writer.send(topicId, schemaId, List.of(recordBuilder.build()));
    }

    /** Send a batch of typed objects (all same type). */
    public List<BatchAck> sendBatch(List<?> objects) throws FluxException {
        return sendBatch(objects, null, null);
    }

    public List<BatchAck> sendBatch(List<?> objects, byte[] key, String topic)
            throws FluxException {
        if (objects.isEmpty()) {
            return List.of();
        }

        Class<?> cls = objects.get(0).getClass();
        String topicName = resolveTopic(cls, topic);
        int topicId = resolveTopicInfo(topicName);
        int schemaId = resolveSchemaId(cls, topicId);

        List<Record> records = new ArrayList<>(objects.size());
        for (Object obj : objects) {
            Record.Builder rb = Record.newBuilder().setValue(ByteString.copyFrom(Schemas.toBytes(obj)));
            if (key != null) {
                rb.setKey(ByteString.copyFrom(key));
            }
            records.add(rb.build());
        }

        RecordBatch batch = RecordBatch.newBuilder()
                .setTopicId(topicId)
                .setSchemaId(schemaId)
                .addAllRecords(records)
                .build();
        return writer.sendBatch(List.of(batch));
    }

    /**
     * Consume typed objects from a topic. Blocks the calling thread.
     *
     * @param cls      the class to deserialize into
     * @param groupId  consumer group ID
     * @param callback called for each deserialized object
     */
    public <T> void consume(Class<T> cls, String groupId, Consumer<T> callback) throws FluxException {
        consume(cls, groupId, null, callback);
    }

    public <T> void consume(Class<T> cls, String groupId, String topic, Consumer<T> callback)
            throws FluxException {
        String topicName = resolveTopic(cls, topic);
        int topicId = resolveTopicInfo(topicName);

        ReaderConfig readerConfig = new ReaderConfig()
                .url(config.getWsUrl())
                .apiKey(config.getApiKey())
                .groupId(groupId)
                .topicId(topicId)
                .timeout(config.getTimeout());

        try (GroupReader reader = GroupReader.join(readerConfig)) {
            reader.startHeartbeat();
            while (true) {
                GroupReader.PollBatch batch = reader.poll();
                for (TopicResult pr : batch.getResults()) {
                    for (io.flux.sdk.proto.Record record : pr.getRecordsList()) {
                        T obj = Schemas.fromBytes(cls, record.getValue().toByteArray());
                        callback.accept(obj);
                    }
                }
                if (batch.getResults().isEmpty()) {
                    Thread.sleep(100);
                } else {
                    reader.commit(batch);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String resolveTopic(Class<?> cls, String override) throws FluxException {
        if (override != null && !override.isEmpty()) {
            return override;
        }
        FluxSchema ann = cls.getAnnotation(FluxSchema.class);
        if (ann == null || ann.topic().isEmpty()) {
            throw new FluxException(
                    cls.getSimpleName() + " has no topic. Use @FluxSchema(topic=...) or pass topic parameter"
            );
        }
        return ann.topic();
    }

    private int resolveTopicInfo(String name) throws FluxException {
        Integer cached = topicCache.get(name);
        if (cached != null) {
            return cached;
        }

        List<Map<String, Object>> topics = admin.listTopics();
        for (Map<String, Object> t : topics) {
            String tName = (String) t.get("name");
            int tId = ((Number) t.get("topic_id")).intValue();
            topicCache.put(tName, tId);
        }

        Integer topicId = topicCache.get(name);
        if (topicId == null) {
            throw new FluxException("Topic not found: " + name);
        }
        return topicId;
    }

    private int resolveSchemaId(Class<?> cls, int topicId) throws FluxException {
        Integer cached = schemaCache.get(cls);
        if (cached != null) {
            return cached;
        }
        int schemaId = admin.registerSchema(topicId, Schemas.schema(cls));
        schemaCache.put(cls, schemaId);
        return schemaId;
    }

    @Override
    public void close() {
        writer.close();
    }
}