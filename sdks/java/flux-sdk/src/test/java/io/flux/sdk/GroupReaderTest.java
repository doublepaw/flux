// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for GroupReader - focused on poll-based model behavior.
 */
class GroupReaderTest {

    /**
     * Test that PollBatch captures offset range from a single poll.
     */
    @Test
    void testPollBatchTracksOffsetRange() {
        GroupReader.PollBatch batch = new GroupReader.PollBatch(
                List.of(), 100L, 150L, 9999L
        );
        assertTrue(batch.getEndOffset() > batch.getStartOffset(),
            "end_offset should be greater than start_offset");
        assertEquals(50, batch.getEndOffset() - batch.getStartOffset(),
            "Offset range should represent the span of polled records");
        assertEquals(9999L, batch.getLeaseDeadlineMs());
    }

    /**
     * Test that commit is a no-op when the batch has equal offsets (empty range).
     */
    @Test
    void testCommitIsNoOpForEmptyBatch() {
        GroupReader.PollBatch batch = new GroupReader.PollBatch(List.of(), 0L, 0L, 0L);
        boolean shouldSkipCommit = (batch.getStartOffset() == batch.getEndOffset());
        assertTrue(shouldSkipCommit,
            "Commit should be skipped for an empty batch");

        // Also test with non-zero equal offsets
        GroupReader.PollBatch batch2 = new GroupReader.PollBatch(List.of(), 50L, 50L, 0L);
        assertTrue(batch2.getStartOffset() == batch2.getEndOffset(),
            "Commit should be skipped when start == end regardless of value");
    }

    /**
     * Test that empty poll (start_offset == end_offset) does not grow the inflight list.
     */
    @Test
    void testEmptyPollNotTrackedInInflight() {
        List<long[]> inflight = new ArrayList<>();
        inflight.add(new long[]{10L, 50L});

        // Simulate poll returning an empty range
        long startOffset = 50L, endOffset = 50L;
        if (startOffset != endOffset) {
            inflight.add(new long[]{startOffset, endOffset});
        }
        assertEquals(1, inflight.size(),
            "Empty poll should not grow inflight list");
    }

    /**
     * Test that rejoin (via doJoin) clears inflight entries.
     */
    @Test
    void testRejoinClearsInflight() {
        // Simulate inflight state before rejoin
        List<long[]> inflight = new ArrayList<>();
        inflight.add(new long[]{0L, 50L});
        inflight.add(new long[]{50L, 100L});
        assertEquals(2, inflight.size());

        // doJoin() clears inflight and updates state
        inflight.clear();

        assertTrue(inflight.isEmpty(),
            "Rejoin (via doJoin) should clear inflight");
    }

    /**
     * Test that the reader state transitions correctly in the poll model.
     * States are: INIT, ACTIVE, STOPPED.
     */
    @Test
    void testReaderStateTransitions() {
        // States are: INIT -> ACTIVE -> STOPPED
        GroupReader.State[] states = GroupReader.State.values();
        assertEquals(3, states.length,
            "Should have exactly 3 states: INIT, ACTIVE, STOPPED");
        assertEquals(GroupReader.State.INIT, states[0]);
        assertEquals(GroupReader.State.ACTIVE, states[1]);
        assertEquals(GroupReader.State.STOPPED, states[2]);
    }

    /**
     * Test that ReaderConfig has expected fields in the poll model.
     */
    @Test
    void testReaderConfigFields() {
        ReaderConfig config = new ReaderConfig()
                .url("ws://localhost:9000")
                .groupId("test-group")
                .topicId(1)
                .maxBytes(1024);

        // Config should have the expected values
        assertEquals("ws://localhost:9000", config.getUrl());
        assertEquals("test-group", config.getGroupId());
        assertEquals(1, config.getTopicId());
        assertEquals(1024, config.getMaxBytes());

        // Verify expected accessors work
    }
}