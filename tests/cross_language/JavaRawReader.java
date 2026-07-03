package io.flux.test;

import io.flux.sdk.*;
import com.google.gson.*;

import java.util.*;

/**
 * Cross-language E2E test: Java raw (zero-copy) reader.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaRawReader <url> <topic_id> <expected_count>
 *
 * Reads records via raw segment reads and prints them as JSON to stdout.
 */
public class JavaRawReader {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: JavaRawReader <url> <topic_id> <expected_count>");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);
        int expectedCount = Integer.parseInt(args[2]);

        try {
            ReaderConfig config = new ReaderConfig()
                    .url(url)
                    .topicId(topicId);

            List<Map<String, Object>> recordsReceived = new ArrayList<>();
            List<Map<String, Object>> segmentsReceived = new ArrayList<>();
            long highWatermark = 0;
            Gson gson = new Gson();

            try (RawReader reader = RawReader.connect(config)) {
                int maxAttempts = 10;
                long offset = 0;

                for (int attempt = 0; attempt < maxAttempts; attempt++) {
                    RawReader.RawReadBatch batch = reader.rawRead(topicId, offset, 1024 * 1024);
                    highWatermark = batch.getHighWatermark();

                    for (SegmentDecoder.DecodedRecord record : batch.getRecords()) {
                        Map<String, Object> recordMap = new LinkedHashMap<>();

                        if (record.getKey() != null) {
                            recordMap.put("key", new String(record.getKey()));
                        } else {
                            recordMap.put("key", null);
                        }

                        try {
                            String valueStr = new String(record.getValue());
                            @SuppressWarnings("unchecked")
                            Map<String, Object> valueJson = gson.fromJson(valueStr, Map.class);
                            recordMap.put("value", valueJson);
                        } catch (Exception e) {
                            // Hex encode if not valid JSON
                            Map<String, String> rawMap = new HashMap<>();
                            rawMap.put("raw", bytesToHex(record.getValue()));
                            recordMap.put("value", rawMap);
                        }

                        recordsReceived.add(recordMap);
                    }

                    for (RawReader.SegmentRange segment : batch.getSegments()) {
                        Map<String, Object> segmentMap = new LinkedHashMap<>();
                        segmentMap.put("start_offset", segment.getStartOffset());
                        segmentMap.put("end_offset", segment.getEndOffset());
                        segmentsReceived.add(segmentMap);
                        offset = Math.max(offset, segment.getEndOffset());
                    }

                    if (recordsReceived.size() >= expectedCount) {
                        break;
                    }

                    Thread.sleep(500);
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("reader", "java");
            result.put("topic_id", topicId);
            result.put("record_count", recordsReceived.size());
            result.put("records", recordsReceived);
            result.put("segments", segmentsReceived);
            result.put("high_watermark", highWatermark);

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
