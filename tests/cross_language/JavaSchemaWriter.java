package io.flux.test;

import io.flux.sdk.*;
import io.flux.sdk.proto.BatchAck;
import io.flux.sdk.proto.Record;
import io.flux.sdk.schema.*;
import com.google.gson.*;
import com.google.protobuf.ByteString;

import java.util.*;

/**
 * Cross-language schema E2E test: Java writer with Avro-encoded values.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaSchemaWriter <url> <topic_id>
 */
public class JavaSchemaWriter {

    @FluxSchema
    public static class TestOrder {
        public String name;
        public long amount;
        public boolean active;
        public List<String> tags;

        public TestOrder() {}
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: JavaSchemaWriter <url> <topic_id>");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);

        try {
            TestOrder order = new TestOrder();
            order.name = "widget";
            order.amount = 42L;
            order.active = true;
            order.tags = Arrays.asList("rush", "fragile");

            byte[] valueBytes = Schemas.toBytes(order);

            WriterConfig config = new WriterConfig().url(url);
            try (Writer writer = Writer.connect(config)) {
                Record record = Record.newBuilder()
                        .setKey(ByteString.copyFrom("order-1".getBytes()))
                        .setValue(ByteString.copyFrom(valueBytes))
                        .build();

                BatchAck ack = writer.send(topicId, 100, List.of(record));

                Gson gson = new Gson();
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("writer", "java");
                result.put("topic_id", topicId);
                result.put("start_offset", ack.getStartOffset());
                result.put("end_offset", ack.getEndOffset());
                result.put("record_count", 1);
                result.put("schema_json", Schemas.schemaJson(TestOrder.class));

                Map<String, Object> fields = new LinkedHashMap<>();
                fields.put("name", "widget");
                fields.put("amount", 42);
                fields.put("active", true);
                fields.put("tags", Arrays.asList("rush", "fragile"));
                result.put("fields", fields);

                System.out.println(gson.toJson(result));
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
