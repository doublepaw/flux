package io.flux.test;

import io.flux.sdk.*;
import io.flux.sdk.proto.TopicResult;
import io.flux.sdk.proto.Record;
import io.flux.sdk.schema.*;
import com.google.gson.*;

import java.util.*;

/**
 * Cross-language schema E2E test: Java reader with Avro-decoded values.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaSchemaReader <url> <topic_id> <expected_count>
 */
public class JavaSchemaReader {

    @FluxSchema
    public static class TestOrder {
        public String name;
        public long amount;
        public boolean active;
        public List<String> tags;

        public TestOrder() {}
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: JavaSchemaReader <url> <topic_id> <expected_count>");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);
        int expectedCount = Integer.parseInt(args[2]);

        try {
            String groupId = "schema-test-java-" + ProcessHandle.current().pid();

            ReaderConfig config = new ReaderConfig()
                    .url(url)
                    .groupId(groupId)
                    .topicId(topicId);

            List<Map<String, Object>> decodedRecords = new ArrayList<>();
            Gson gson = new Gson();

            try (GroupReader reader = GroupReader.join(config)) {
                int maxAttempts = 10;

                for (int attempt = 0; attempt < maxAttempts; attempt++) {
                    GroupReader.PollBatch batch = reader.poll();

                    for (TopicResult result : batch.getResults()) {
                        for (Record record : result.getRecordsList()) {
                            byte[] valueBytes = record.getValue().toByteArray();
                            TestOrder order = Schemas.fromBytes(TestOrder.class, valueBytes);

                            Map<String, Object> decoded = new LinkedHashMap<>();
                            if (record.hasKey()) {
                                decoded.put("key", new String(record.getKey().toByteArray()));
                            } else {
                                decoded.put("key", null);
                            }
                            decoded.put("name", order.name);
                            decoded.put("amount", order.amount);
                            decoded.put("active", order.active);
                            decoded.put("tags", order.tags);

                            decodedRecords.add(decoded);
                        }
                    }

                    if (decodedRecords.size() >= expectedCount) {
                        break;
                    }

                    Thread.sleep(500);
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("reader", "java");
            result.put("topic_id", topicId);
            result.put("record_count", decodedRecords.size());
            result.put("records", decodedRecords);

            System.out.println(gson.toJson(result));
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
