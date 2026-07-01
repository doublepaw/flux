// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Broker buffer for batching append requests.
//!
//! Buffers incoming records from multiple writers, merges them by
//! (topic_id, schema_id), and flushes when size or time thresholds are reached.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{BatchAck, Record, RecordBatch};
use tokio::sync::oneshot;

/// Key for grouping records in the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchKey {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
}

/// A pending writer waiting for acknowledgment.
#[derive(Debug)]
pub struct PendingWriter {
    pub writer_id: WriterId,
    pub append_seq: AppendSeq,
    /// Time when this request was inserted into the broker buffer.
    pub buffered_at: Instant,
    /// Which batches this writer contributed to.
    pub segment_keys: Vec<BatchKey>,
    /// Record count per batch key (to calculate offsets).
    pub record_counts: Vec<usize>,
    /// Starting record index in each batch (for correct offset calculation).
    pub start_indices: Vec<usize>,
    /// Channel to send append_acks back to the writer.
    pub ack_sender: oneshot::Sender<Vec<BatchAck>>,
}

/// Buffered batch data.
#[derive(Debug)]
struct BufferedSegment {
    records: Vec<Record>,
    byte_size: usize,
}

impl BufferedSegment {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            byte_size: 0,
        }
    }

    fn add_record(&mut self, record: Record) {
        self.byte_size += record.value.len();
        if let Some(ref key) = record.key {
            self.byte_size += key.len();
        }
        self.records.push(record);
    }
}

/// Configuration for the broker buffer.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum buffer size in bytes before triggering flush.
    pub max_size_bytes: usize,
    /// Maximum time to wait before flushing.
    pub max_wait: Duration,
    /// High water mark for backpressure (reject new requests).
    pub high_water_bytes: usize,
    /// Low water mark for resuming after backpressure.
    pub low_water_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 256 * 1024 * 1024, // 256 MB
            max_wait: Duration::from_millis(200),
            high_water_bytes: 384 * 1024 * 1024, // 384 MB
            low_water_bytes: 128 * 1024 * 1024,  // 128 MB
        }
    }
}

/// Result of draining the buffer.
#[derive(Debug)]
pub struct DrainResult {
    /// Merged batches ready to write.
    pub batches: Vec<RecordBatch>,
    /// Pending writers to notify after commit.
    pub pending_writers: Vec<PendingWriter>,
    /// Mapping from batch key to index in batches vec.
    pub key_to_index: HashMap<BatchKey, usize>,
}

/// Broker buffer for batching append requests.
pub struct BrokerBuffer {
    /// Buffered batches by key.
    batches: HashMap<BatchKey, BufferedSegment>,
    /// Pending writers waiting for append_acks.
    pending_writers: Vec<PendingWriter>,
    /// Total bytes in buffer.
    total_bytes: usize,
    /// Time when first record was added to current batch.
    batch_start: Option<Instant>,
    /// Configuration.
    config: BufferConfig,
}

impl BrokerBuffer {
    /// Create a new buffer with default config.
    pub fn new() -> Self {
        Self::with_config(BufferConfig::default())
    }

    /// Create a new buffer with custom config.
    pub fn with_config(config: BufferConfig) -> Self {
        Self {
            batches: HashMap::new(),
            pending_writers: Vec::new(),
            total_bytes: 0,
            batch_start: None,
            config,
        }
    }

    /// Insert a append request into the buffer.
    ///
    /// Returns a receiver that will get the append_acks when the batch is committed.
    pub fn insert(
        &mut self,
        writer_id: WriterId,
        append_seq: AppendSeq,
        batches: Vec<RecordBatch>,
    ) -> oneshot::Receiver<Vec<BatchAck>> {
        let (tx, rx) = oneshot::channel();
        self.insert_with_sender(writer_id, append_seq, batches, tx);
        rx
    }

    /// Insert a append request and use a provided sender for acknowledgments.
    pub fn insert_with_sender(
        &mut self,
        writer_id: WriterId,
        append_seq: AppendSeq,
        batches: Vec<RecordBatch>,
        ack_sender: oneshot::Sender<Vec<BatchAck>>,
    ) {
        // Track batch start time
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }

        let mut segment_keys = Vec::with_capacity(batches.len());
        let mut record_counts = Vec::with_capacity(batches.len());
        let mut start_indices = Vec::with_capacity(batches.len());

        for batch in batches {
            let key = BatchKey {
                topic_id: batch.topic_id,
                schema_id: batch.schema_id,
            };

            segment_keys.push(key);
            record_counts.push(batch.records.len());

            let buffered = self.batches.entry(key).or_insert_with(BufferedSegment::new);

            // Capture starting position before adding records
            start_indices.push(buffered.records.len());

            for record in batch.records {
                let record_size =
                    record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
                self.total_bytes += record_size;
                buffered.add_record(record);
            }
        }

        self.pending_writers.push(PendingWriter {
            writer_id,
            append_seq,
            buffered_at: Instant::now(),
            segment_keys,
            record_counts,
            start_indices,
            ack_sender,
        });
    }

    /// Check if the buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        // Flush on size
        if self.total_bytes >= self.config.max_size_bytes {
            return true;
        }

        // Flush on time
        if let Some(start) = self.batch_start
            && start.elapsed() >= self.config.max_wait
        {
            return true;
        }

        false
    }

    /// Check if backpressure should be applied.
    pub fn should_apply_backpressure(&self) -> bool {
        self.total_bytes >= self.config.high_water_bytes
    }

    /// Check if backpressure can be released.
    pub fn can_release_backpressure(&self) -> bool {
        self.total_bytes <= self.config.low_water_bytes
    }

    /// Get current buffer size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Get number of pending writers.
    pub fn pending_count(&self) -> usize {
        self.pending_writers.len()
    }

    /// Get number of batches in buffer.
    pub fn segment_count(&self) -> usize {
        self.batches.len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Drain the buffer, returning merged batches and pending writers.
    pub fn drain(&mut self) -> DrainResult {
        let mut batches = Vec::with_capacity(self.batches.len());
        let mut key_to_index = HashMap::with_capacity(self.batches.len());

        for (key, buffered) in self.batches.drain() {
            let index = batches.len();
            key_to_index.insert(key, index);

            batches.push(RecordBatch {
                topic_id: key.topic_id,
                schema_id: key.schema_id,
                records: buffered.records,
            });
        }

        let pending_writers = std::mem::take(&mut self.pending_writers);

        self.total_bytes = 0;
        self.batch_start = None;

        DrainResult {
            batches,
            pending_writers,
            key_to_index,
        }
    }

    /// Distribute append_acks to pending writers after successful commit.
    ///
    /// `segment_offsets` maps batch index to (start_offset, end_offset).
    pub fn distribute_acks(drain_result: DrainResult, segment_offsets: &[(u64, u64)]) {
        for pending in drain_result.pending_writers {
            let append_acks = Self::calculate_acks_for_pending(
                &pending,
                &drain_result.key_to_index,
                segment_offsets,
            );

            // Send append_acks to writer (ignore error if receiver dropped)
            let _ = pending.ack_sender.send(append_acks);
        }
    }

    /// Calculate append_acks for a single pending writer.
    pub fn calculate_acks_for_pending(
        pending: &PendingWriter,
        key_to_index: &HashMap<BatchKey, usize>,
        segment_offsets: &[(u64, u64)],
    ) -> Vec<BatchAck> {
        let mut append_acks = Vec::with_capacity(pending.segment_keys.len());

        for i in 0..pending.segment_keys.len() {
            let key = &pending.segment_keys[i];
            let record_count = pending.record_counts[i];
            let start_idx = pending.start_indices[i];

            if let Some(&seg_idx) = key_to_index.get(key) {
                let (seg_start, seg_end) = segment_offsets[seg_idx];

                // Calculate actual offset based on where records were inserted
                let writer_start = seg_start + start_idx as u64;
                let writer_end = writer_start + record_count as u64;

                append_acks.push(BatchAck {
                    topic_id: key.topic_id,
                    schema_id: key.schema_id,
                    start_offset: flux_common::ids::Offset(writer_start),
                    end_offset: flux_common::ids::Offset(writer_end),
                });

                // Sanity check
                debug_assert!(writer_end <= seg_end);
            }
        }

        append_acks
    }
}

impl Default for BrokerBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    fn make_record(key: &str, value: &str) -> Record {
        Record {
            key: Some(Bytes::from(key.to_string())),
            value: Bytes::from(value.to_string()),
        }
    }

    fn make_segment(topic: u32, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            topic_id: TopicId(topic),
            schema_id: SchemaId(100),
            records,
        }
    }

    #[test]
    fn test_buffer_insert_single() {
        let mut buffer = BrokerBuffer::new();

        let writer_id = WriterId(Uuid::new_v4());
        let batches = vec![make_segment(1, vec![make_record("k1", "v1")])];

        let _rx = buffer.insert(writer_id, AppendSeq(1), batches);

        assert_eq!(buffer.segment_count(), 1);
        assert_eq!(buffer.pending_count(), 1);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_buffer_merge_same_key() {
        let mut buffer = BrokerBuffer::new();

        // Two writers send to same topic+schema
        let p1 = WriterId(Uuid::new_v4());
        let p2 = WriterId(Uuid::new_v4());

        let _rx1 = buffer.insert(
            p1,
            AppendSeq(1),
            vec![make_segment(
                1,
                vec![make_record("k1", "v1"), make_record("k2", "v2")],
            )],
        );

        let _rx2 = buffer.insert(
            p2,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k3", "v3")])],
        );

        // Should have 1 merged batch with 3 records
        assert_eq!(buffer.segment_count(), 1);
        assert_eq!(buffer.pending_count(), 2);

        let result = buffer.drain();
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].records.len(), 3);
        assert_eq!(result.pending_writers.len(), 2);
    }

    #[test]
    fn test_buffer_multiple_topics() {
        let mut buffer = BrokerBuffer::new();

        let writer = WriterId(Uuid::new_v4());
        let batches = vec![
            make_segment(1, vec![make_record("k1", "v1")]),
            make_segment(2, vec![make_record("k2", "v2")]),
        ];

        let _rx = buffer.insert(writer, AppendSeq(1), batches);

        assert_eq!(buffer.segment_count(), 2);
        assert_eq!(buffer.pending_count(), 1);
    }

    #[test]
    fn test_buffer_flush_on_size() {
        let config = BufferConfig {
            max_size_bytes: 100,
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        let mut buffer = BrokerBuffer::with_config(config);

        // Add small record - should not trigger flush
        let p1 = WriterId(Uuid::new_v4());
        let _rx1 = buffer.insert(
            p1,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k", "small")])],
        );
        assert!(!buffer.should_flush());

        // Add large record - should trigger flush
        let p2 = WriterId(Uuid::new_v4());
        let large_value = "x".repeat(100);
        let _rx2 = buffer.insert(
            p2,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k", &large_value)])],
        );
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_buffer_flush_on_time() {
        let config = BufferConfig {
            max_size_bytes: 1024 * 1024,
            max_wait: Duration::from_millis(1),
            ..Default::default()
        };
        let mut buffer = BrokerBuffer::with_config(config);

        let writer = WriterId(Uuid::new_v4());
        let _rx = buffer.insert(
            writer,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k", "v")])],
        );

        // Wait and check again
        std::thread::sleep(Duration::from_millis(5));
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_buffer_drain_clears() {
        let mut buffer = BrokerBuffer::new();

        let writer = WriterId(Uuid::new_v4());
        let _rx = buffer.insert(
            writer,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k", "v")])],
        );

        assert!(!buffer.is_empty());
        let _result = buffer.drain();
        assert!(buffer.is_empty());
        assert_eq!(buffer.size_bytes(), 0);
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn test_distribute_acks() {
        let mut buffer = BrokerBuffer::new();

        // Writer 1 sends 2 records
        let p1 = WriterId(Uuid::new_v4());
        let mut rx1 = buffer.insert(
            p1,
            AppendSeq(1),
            vec![make_segment(
                1,
                vec![make_record("k1", "v1"), make_record("k2", "v2")],
            )],
        );

        // Writer 2 sends 1 record to same topic
        let p2 = WriterId(Uuid::new_v4());
        let mut rx2 = buffer.insert(
            p2,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k3", "v3")])],
        );

        let result = buffer.drain();

        // Simulate DB assigned offsets 0-3 for the merged batch
        let segment_offsets = vec![(0, 3)];

        BrokerBuffer::distribute_acks(result, &segment_offsets);

        // Writer 1 should get offsets 0-2
        let acks1 = rx1.try_recv().unwrap();
        assert_eq!(acks1.len(), 1);
        assert_eq!(acks1[0].start_offset.0, 0);
        assert_eq!(acks1[0].end_offset.0, 2);

        // Writer 2 should get offsets 2-3
        let acks2 = rx2.try_recv().unwrap();
        assert_eq!(acks2.len(), 1);
        assert_eq!(acks2[0].start_offset.0, 2);
        assert_eq!(acks2[0].end_offset.0, 3);
    }

    #[test]
    fn test_backpressure() {
        let config = BufferConfig {
            max_size_bytes: 1024,
            max_wait: Duration::from_secs(60),
            high_water_bytes: 200,
            low_water_bytes: 100,
        };
        let mut buffer = BrokerBuffer::with_config(config);

        // Add data to exceed high water
        let writer = WriterId(Uuid::new_v4());
        let large_value = "x".repeat(250);
        let _rx = buffer.insert(
            writer,
            AppendSeq(1),
            vec![make_segment(1, vec![make_record("k", &large_value)])],
        );

        assert!(buffer.should_apply_backpressure());
        assert!(!buffer.can_release_backpressure());

        // Drain to release
        let _result = buffer.drain();
        assert!(!buffer.should_apply_backpressure());
        assert!(buffer.can_release_backpressure());
    }
}
