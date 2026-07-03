package io.flux.test;

import io.flux.sdk.*;
import io.flux.sdk.proto.TopicResult;
import io.flux.sdk.proto.Record;
import com.google.gson.*;

import java.util.*;

/**
 * Cross-language E2E test: Java group reader using zero-copy (raw) polls.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaRawGroupReader <url> <topic_id> <expected_count>
 *
 * Joins a reader group, polls raw segments (decoded locally), commits each
 * batch, and prints the records as JSON to stdout.
 */
public class JavaRawGroupReader {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: JavaRawGroupReader <url> <topic_id> <expected_count>");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);
        int expectedCount = Integer.parseInt(args[2]);

        try {
            String groupId = "cross-lang-java-raw-group-" + ProcessHandle.current().pid();

            ReaderConfig config = new ReaderConfig()
                    .url(url)
                    .groupId(groupId)
                    .topicId(topicId)
                    .rawPoll(true);

            List<Map<String, Object>> recordsReceived = new ArrayList<>();
            Gson gson = new Gson();

            try (GroupReader reader = GroupReader.join(config)) {
                int maxAttempts = 10;

                for (int attempt = 0; attempt < maxAttempts; attempt++) {
                    GroupReader.PollBatch batch = reader.poll();

                    for (TopicResult result : batch.getResults()) {
                        for (Record record : result.getRecordsList()) {
                            Map<String, Object> recordMap = new LinkedHashMap<>();

                            if (record.hasKey()) {
                                recordMap.put("key", new String(record.getKey().toByteArray()));
                            } else {
                                recordMap.put("key", null);
                            }

                            try {
                                String valueStr = new String(record.getValue().toByteArray());
                                @SuppressWarnings("unchecked")
                                Map<String, Object> valueJson = gson.fromJson(valueStr, Map.class);
                                recordMap.put("value", valueJson);
                            } catch (Exception e) {
                                // Hex encode if not valid JSON
                                Map<String, String> rawMap = new HashMap<>();
                                rawMap.put("raw", bytesToHex(record.getValue().toByteArray()));
                                recordMap.put("value", rawMap);
                            }

                            recordsReceived.add(recordMap);
                        }
                    }

                    reader.commit(batch);

                    if (recordsReceived.size() >= expectedCount) {
                        break;
                    }

                    Thread.sleep(500);
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("reader", "java-raw-group");
            result.put("topic_id", topicId);
            result.put("record_count", recordsReceived.size());
            result.put("records", recordsReceived);

            System.out.println(gson.toJson(result));
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
