package io.flux.test;

import io.flux.sdk.*;
import io.flux.sdk.proto.Record;
import io.flux.sdk.proto.TopicResult;

import java.time.Duration;
import java.util.Locale;

/**
 * Group consumer throughput benchmark: poll -> count -> commit until the
 * expected number of records is consumed or a 120s timeout expires.
 *
 * Usage:
 *     java -cp <classpath> io.flux.test.JavaGroupConsumerBench \
 *         <url> <topic_id> <expected_records> <raw|classic> [max_bytes]
 *
 * Prints one JSON line to stdout:
 *     {"consumer":"java-<mode>","records":N,"elapsed_secs":S,"records_per_sec":R,"mib_per_sec":M}
 */
public class JavaGroupConsumerBench {
    private static final long TIMEOUT_SECS = 120;
    private static final int DEFAULT_MAX_BYTES = 8 * 1024 * 1024;

    public static void main(String[] args) {
        if (args.length < 4 || args.length > 6) {
            System.err.println(
                    "Usage: JavaGroupConsumerBench <url> <topic_id> <expected_records> <raw|classic> [max_bytes]");
            System.exit(1);
        }

        String url = args[0];
        int topicId = Integer.parseInt(args[1]);
        long expectedRecords = Long.parseLong(args[2]);
        String mode = args[3];
        if (!mode.equals("raw") && !mode.equals("classic")) {
            System.err.println("Mode must be 'raw' or 'classic', got: " + mode);
            System.exit(1);
        }
        int maxBytes = args.length >= 5 ? Integer.parseInt(args[4]) : DEFAULT_MAX_BYTES;
        int pipelineDepth = args.length >= 6 ? Integer.parseInt(args[5]) : 2;

        try {
            String groupId = "bench-java-" + mode + "-" + System.currentTimeMillis();
            ReaderConfig config = new ReaderConfig()
                    .url(url)
                    .groupId(groupId)
                    .topicId(topicId)
                    .maxBytes(maxBytes)
                .pipelineDepth(pipelineDepth)
                    .rawPoll(mode.equals("raw"))
                    .heartbeatInterval(Duration.ofSeconds(5));

            long records = 0;
            long payloadBytes = 0;
            double elapsedSecs;

            try (GroupReader reader = GroupReader.join(config)) {
                reader.startHeartbeat();

                long startNanos = System.nanoTime();
                long deadlineNanos = startNanos + TIMEOUT_SECS * 1_000_000_000L;

                while (records < expectedRecords && System.nanoTime() < deadlineNanos) {
                    GroupReader.PollBatch batch = reader.poll();

                    for (TopicResult result : batch.getResults()) {
                        for (Record record : result.getRecordsList()) {
                            records++;
                            payloadBytes += record.getValue().size();
                            if (record.hasKey()) {
                                payloadBytes += record.getKey().size();
                            }
                        }
                    }

                    reader.commit(batch);

                    if (batch.getStartOffset() == batch.getEndOffset()) {
                        Thread.sleep(50); // no work leased; back off briefly
                    }
                }

                elapsedSecs = (System.nanoTime() - startNanos) / 1e9;
            }

            double recordsPerSec = records / elapsedSecs;
            double mibPerSec = payloadBytes / (1024.0 * 1024.0) / elapsedSecs;
            System.out.println(String.format(Locale.ROOT,
                    "{\"consumer\":\"java-%s\",\"records\":%d,\"elapsed_secs\":%.3f,"
                            + "\"records_per_sec\":%.1f,\"mib_per_sec\":%.2f}",
                    mode, records, elapsedSecs, recordsPerSec, mibPerSec));

            if (records < expectedRecords) {
                System.err.println("Timed out: consumed " + records + " of " + expectedRecords + " records");
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
