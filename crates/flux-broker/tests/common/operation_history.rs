// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Operation history for linearizability checking in Jepsen-style tests.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::Mutex;

use flux_common::ids::{Offset, TopicId, WriterId};

/// Thread-safe wrapper for OperationHistory.
pub type SharedHistory = Arc<Mutex<OperationHistory>>;

/// A write operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct WriteOp {
    pub writer_id: WriterId,
    pub topic_id: TopicId,
    pub offset: Option<Offset>,
    pub value: Bytes,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// A read operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct ReadOp {
    pub topic_id: TopicId,
    pub reader_id: String,
    pub requested_offset: Offset,
    pub returned_values: Vec<Bytes>,
    pub high_watermark: Offset,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// Track operation history for linearizability checking.
#[derive(Default)]
pub struct OperationHistory {
    pub writes: Vec<WriteOp>,
    pub reads: Vec<ReadOp>,
}

impl OperationHistory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a thread-safe shared instance.
    pub fn shared() -> SharedHistory {
        Arc::new(Mutex::new(Self::new()))
    }

    pub fn record_write_start(&mut self, writer_id: WriterId, value: Bytes) -> usize {
        self.record_write(writer_id, TopicId(0), value)
    }

    pub fn record_write(&mut self, writer_id: WriterId, topic_id: TopicId, value: Bytes) -> usize {
        let idx = self.writes.len();
        self.writes.push(WriteOp {
            writer_id,
            topic_id,
            offset: None,
            value,
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_write_complete(&mut self, idx: usize, offset: Option<Offset>, success: bool) {
        if let Some(op) = self.writes.get_mut(idx) {
            op.offset = offset;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    pub fn record_read_start(&mut self, offset: Offset) -> usize {
        self.record_read(TopicId(0), String::new(), offset)
    }

    pub fn record_read(&mut self, topic_id: TopicId, reader_id: String, offset: Offset) -> usize {
        let idx = self.reads.len();
        self.reads.push(ReadOp {
            topic_id,
            reader_id,
            requested_offset: offset,
            returned_values: vec![],
            high_watermark: Offset(0),
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_read_complete(
        &mut self,
        idx: usize,
        values: Vec<Bytes>,
        high_watermark: Offset,
        success: bool,
    ) {
        if let Some(op) = self.reads.get_mut(idx) {
            op.returned_values = values;
            op.high_watermark = high_watermark;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    /// Check that all acknowledged writes are eventually visible.
    ///
    /// A write is "visible" if its value appears in any successful read whose
    /// offset range covers the write's offset.
    pub fn verify_acknowledged_writes_visible(&self) -> Result<(), String> {
        let successful_writes: Vec<_> = self
            .writes
            .iter()
            .filter(|w| w.success && w.offset.is_some())
            .collect();

        if successful_writes.is_empty() {
            return Ok(());
        }

        let successful_reads: Vec<_> = self.reads.iter().filter(|r| r.success).collect();
        if successful_reads.is_empty() {
            return Err("No successful reads to verify against".to_string());
        }

        // Collect all observed values across every successful read into one set.
        let all_observed: HashSet<&[u8]> = successful_reads
            .iter()
            .flat_map(|r| r.returned_values.iter().map(|v| v.as_ref()))
            .collect();

        // Find the latest read start time so we can bound the "happened-before" check.
        let latest_read_start = successful_reads.iter().map(|r| r.start_time).max().unwrap();

        // The highest watermark across all reads is the upper bound of what
        // the system has confirmed as committed.
        let max_watermark = successful_reads
            .iter()
            .map(|r| r.high_watermark.0)
            .max()
            .unwrap();

        for write in &successful_writes {
            if let (Some(offset), Some(write_end_time)) = (write.offset, write.end_time) {
                // Only check writes that finished before the latest read started
                // and whose offset is below the highest observed watermark.
                if write_end_time < latest_read_start
                    && offset.0 < max_watermark
                    && !all_observed.contains(write.value.as_ref())
                {
                    return Err(format!(
                        "Acknowledged write at offset {} (topic {}) with value {:?} \
                         not visible in any read (max watermark {})",
                        offset.0, write.topic_id.0, write.value, max_watermark
                    ));
                }
            }
        }

        Ok(())
    }

    /// Check that offsets are unique per topic (no two writes got the same offset).
    pub fn verify_unique_offsets(&self) -> Result<(), String> {
        let mut seen: HashSet<(u32, u64)> = HashSet::new();
        for write in &self.writes {
            if write.success
                && let Some(offset) = write.offset
                && !seen.insert((write.topic_id.0, offset.0))
            {
                return Err(format!(
                    "Duplicate offset {} in topic {} assigned to multiple writes",
                    offset.0, write.topic_id.0
                ));
            }
        }
        Ok(())
    }

    /// Check that watermark never regresses across reads for the same
    /// (reader_id, topic_id).
    pub fn verify_watermark_monotonic(&self) -> Result<(), String> {
        let mut reads_by_stream: HashMap<(&str, u32), Vec<&ReadOp>> = HashMap::new();
        for read in &self.reads {
            if read.success {
                reads_by_stream
                    .entry((&read.reader_id, read.topic_id.0))
                    .or_default()
                    .push(read);
            }
        }

        for ((reader_id, topic_id), reads) in &reads_by_stream {
            for window in reads.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                if curr.high_watermark.0 < prev.high_watermark.0 {
                    return Err(format!(
                        "Watermark regressed from {} to {} for reader '{}' topic {}",
                        prev.high_watermark.0, curr.high_watermark.0, reader_id, topic_id
                    ));
                }
            }
        }
        Ok(())
    }

    /// Each value appears at exactly one (topic, offset). No duplicate writes.
    pub fn verify_no_duplicates(&self) -> Result<(), String> {
        let mut value_to_location: HashMap<&[u8], (u32, u64)> = HashMap::new();
        for write in &self.writes {
            if write.success
                && let Some(offset) = write.offset
            {
                let location = (write.topic_id.0, offset.0);
                if let Some(&prev) = value_to_location.get(write.value.as_ref())
                    && prev != location
                {
                    return Err(format!(
                        "Duplicate write: value {:?} at ({}, {}) and ({}, {})",
                        write.value, prev.0, prev.1, location.0, location.1
                    ));
                } else {
                    value_to_location.insert(&write.value, location);
                }
            }
        }
        Ok(())
    }

    /// Sequential reads by same consumer on a topic don't skip offsets.
    pub fn verify_poll_contiguity(&self) -> Result<(), String> {
        let mut reads_by_reader: HashMap<(&str, u32), Vec<&ReadOp>> = HashMap::new();
        for read in &self.reads {
            if read.success && !read.returned_values.is_empty() {
                reads_by_reader
                    .entry((&read.reader_id, read.topic_id.0))
                    .or_default()
                    .push(read);
            }
        }

        for ((reader_id, topic_id), reads) in &reads_by_reader {
            for window in reads.windows(2) {
                let prev = &window[0];
                let curr = &window[1];
                let expected_next = prev.requested_offset.0 + prev.returned_values.len() as u64;
                if curr.requested_offset.0 != expected_next {
                    return Err(format!(
                        "Poll contiguity violation for reader '{}' topic {}: \
                         expected offset {} but got {}",
                        reader_id, topic_id, expected_next, curr.requested_offset.0
                    ));
                }
            }
        }
        Ok(())
    }

    /// If write A completed (acked) before write B was invoked (sent),
    /// and both are on the same topic, then A.offset < B.offset.
    pub fn verify_write_write_causal(&self) -> Result<(), String> {
        let successful: Vec<_> = self
            .writes
            .iter()
            .filter(|w| w.success && w.offset.is_some() && w.end_time.is_some())
            .collect();

        let mut by_topic: HashMap<u32, Vec<&WriteOp>> = HashMap::new();
        for w in &successful {
            by_topic.entry(w.topic_id.0).or_default().push(w);
        }

        for (topic_id, writes) in &by_topic {
            for a in writes {
                for b in writes {
                    if std::ptr::eq(*a, *b) {
                        continue;
                    }
                    let a_end = a.end_time.unwrap();
                    if a_end < b.start_time {
                        let a_off = a.offset.unwrap().0;
                        let b_off = b.offset.unwrap().0;
                        if a_off >= b_off {
                            return Err(format!(
                                "WW causal violation on topic {}: \
                                 write {:?} (offset {}, completed {:?}) \
                                 completed before write {:?} (offset {}, started {:?}), \
                                 but offset is not less",
                                topic_id, a.value, a_off, a.end_time, b.value, b_off, b.start_time,
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Per-producer offsets within a topic are monotonically increasing.
    pub fn verify_monotonic_sends(&self) -> Result<(), String> {
        let mut writes_by_writer: HashMap<(WriterId, u32), Vec<&WriteOp>> = HashMap::new();
        for write in &self.writes {
            if write.success && write.offset.is_some() {
                writes_by_writer
                    .entry((write.writer_id, write.topic_id.0))
                    .or_default()
                    .push(write);
            }
        }

        for ((writer_id, topic_id), writes) in &writes_by_writer {
            for window in writes.windows(2) {
                if let (Some(prev_offset), Some(curr_offset)) = (window[0].offset, window[1].offset)
                    && curr_offset.0 <= prev_offset.0
                {
                    return Err(format!(
                        "Non-monotonic send for writer {:?} topic {}: \
                         offset {} followed by {}",
                        writer_id, topic_id, prev_offset.0, curr_offset.0
                    ));
                }
            }
        }
        Ok(())
    }

    /// Verify that no values appear in reads that were never acknowledged.
    ///
    /// Catches "phantom writes" — data committed to the log but error returned
    /// to the client (false negative from the client's perspective).
    /// Verify that no values appear in reads that were never attempted by any writer.
    /// This catches fabricated data (storage corruption, bugs) but allows "uncertain"
    /// writes — writes that were attempted but not acked (e.g., due to timeout/partition)
    /// which may still be committed by the broker's async flush.
    pub fn verify_no_phantom_writes(&self) -> Result<(), String> {
        let attempted_values: HashSet<&[u8]> =
            self.writes.iter().map(|w| w.value.as_ref()).collect();

        for read in &self.reads {
            if read.success {
                for value in &read.returned_values {
                    if !attempted_values.contains(value.as_ref()) {
                        return Err(format!(
                            "Phantom write detected: value {:?} found in read but \
                             was never attempted by any writer",
                            value
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
