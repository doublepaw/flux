// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Core data types for Flux eventbus

use crate::{Offset, SchemaId, TopicId};
use bytes::Bytes;

/// A single record (key-value pair)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Optional key
    pub key: Option<Bytes>,
    /// Record value (Avro-encoded according to schema)
    pub value: Bytes,
}

impl Record {
    /// Create a new record without a key
    pub fn new(value: impl Into<Bytes>) -> Self {
        Record {
            key: None,
            value: value.into(),
        }
    }

    /// Create a new record with a key
    pub fn with_key(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Record {
            key: Some(key.into()),
            value: value.into(),
        }
    }

    /// Total size in bytes (key + value)
    pub fn size(&self) -> usize {
        self.key.as_ref().map(|k| k.len()).unwrap_or(0) + self.value.len()
    }
}

/// A batch of records for a specific topic/schema
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// Topic this batch belongs to
    pub topic_id: TopicId,
    /// Schema ID for the record values
    pub schema_id: SchemaId,
    /// Records in this batch
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Create a new empty batch
    pub fn new(topic_id: TopicId, schema_id: SchemaId) -> Self {
        RecordBatch {
            topic_id,
            schema_id,
            records: Vec::new(),
        }
    }

    /// Add a record to the batch
    pub fn push(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Number of records in the batch
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Total size of all records in bytes
    pub fn size(&self) -> usize {
        self.records.iter().map(|r| r.size()).sum()
    }
}

/// Acknowledgment for a committed batch
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchAck {
    /// Topic this batch belongs to
    pub topic_id: TopicId,
    /// Schema ID for the record values
    pub schema_id: SchemaId,
    /// First offset in this batch (inclusive)
    pub start_offset: Offset,
    /// Last offset in this batch (inclusive)
    pub end_offset: Offset,
}

impl BatchAck {
    /// Number of records in this acknowledged batch
    pub fn record_count(&self) -> u64 {
        self.end_offset.0 - self.start_offset.0 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_with_key() {
        let record = Record::with_key("user-123", "payload");
        assert!(record.key.is_some());
        assert_eq!(record.key.as_ref().unwrap().as_ref(), b"user-123");
        assert_eq!(record.value.as_ref(), b"payload");
    }

    #[test]
    fn test_record_without_key() {
        let record = Record::new("payload");
        assert!(record.key.is_none());
        assert_eq!(record.value.as_ref(), b"payload");
    }

    #[test]
    fn test_record_size() {
        let record = Record::with_key("key", "value");
        assert_eq!(record.size(), 8); // 3 + 5
    }

    #[test]
    fn test_segment_record_count() {
        let mut batch = RecordBatch::new(TopicId(1), SchemaId(100));
        batch.push(Record::new("a"));
        batch.push(Record::new("b"));
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_segment_empty() {
        let batch = RecordBatch::new(TopicId(1), SchemaId(100));
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_segment_size() {
        let mut batch = RecordBatch::new(TopicId(1), SchemaId(100));
        batch.push(Record::new("hello")); // 5 bytes
        batch.push(Record::new("world")); // 5 bytes
        assert_eq!(batch.size(), 10);
    }

    #[test]
    fn test_segment_ack_record_count() {
        let ack = BatchAck {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            start_offset: Offset(10),
            end_offset: Offset(19),
        };
        assert_eq!(ack.record_count(), 10);
    }
}
