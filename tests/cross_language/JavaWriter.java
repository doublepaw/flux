package io.flux.test;

import io.flux.sdk.*;
import io.flux.sdk.proto.BatchAck;
import io.flux.sdk.proto.Record;
import com.google.gson.*;
import com.google.protobuf.ByteString;

import java.util.*;

/**
 * Cross-language E2E test: Java writer.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaWriter <url> <topic_id> <num_records>
 *
 * Appends records and prints the ack JSON to stdout.
 */
public class JavaWriter {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: JavaWriter <url> <topic_id> <num_records>");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);
        int numRecords = Integer.parseInt(args[2]);

        try {
            WriterConfig config = new WriterConfig().url(url);

            try (Writer writer = Writer.connect(config)) {
                List<Record> records = new ArrayList<>();
                Gson gson = new Gson();

                for (int i = 0; i < numRecords; i++) {
                    byte[] key = String.format("java-key-%d", i).getBytes();

                    Map<String, Object> valueMap = new LinkedHashMap<>();
                    valueMap.put("source", "java");
                    valueMap.put("index", i);
                    valueMap.put("data", String.format("hello from java %d", i));
                    byte[] value = gson.toJson(valueMap).getBytes();

                    records.add(
                            Record.newBuilder()
                                    .setKey(ByteString.copyFrom(key))
                                    .setValue(ByteString.copyFrom(value))
                                    .build()
                    );
                }

                BatchAck ack = writer.send(topicId, 100, records);

                Map<String, Object> result = new LinkedHashMap<>();
                result.put("writer", "java");
                result.put("writer_id", writer.getWriterId().toString());
                result.put("topic_id", topicId);
                result.put("start_offset", ack.getStartOffset());
                result.put("end_offset", ack.getEndOffset());
                result.put("record_count", numRecords);

                System.out.println(gson.toJson(result));
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
