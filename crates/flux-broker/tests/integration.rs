// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Integration tests for the broker.
//!
//! These tests require a running PostgreSQL instance.
//! Set DATABASE_URL environment variable to run.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux_test cargo test --test integration
//! ```

use bytes::Bytes;
use tempfile::TempDir;

use flux_broker::{BrokerBuffer, BufferConfig, FlReader, FlWriter, LocalFsStore, ObjectStore};
use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};

/// Test the full append flow without database (FL + ObjectStore).
#[tokio::test]
async fn test_produce_flow_fl_s3() {
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    // Create batches for two different topics
    let batches = vec![
        RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from("user-1")),
                    value: Bytes::from(r#"{"name":"Alice"}"#),
                },
                Record {
                    key: Some(Bytes::from("user-2")),
                    value: Bytes::from(r#"{"name":"Bob"}"#),
                },
            ],
        },
        RecordBatch {
            topic_id: TopicId(2),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("user-3")),
                value: Bytes::from(r#"{"name":"Charlie"}"#),
            }],
        },
    ];

    // Write FL
    let mut writer = FlWriter::new();
    for batch in &batches {
        writer.add_segment(batch).unwrap();
    }
    let fl_data = writer.finish();

    // Write to "S3" (local filesystem)
    let key = "test/batch-001.fl";
    store.put(key, fl_data.clone()).await.unwrap();

    // Read back from "S3"
    let read_data = store.get(key).await.unwrap();
    assert_eq!(read_data, fl_data);

    // Parse FL
    let metas = FlReader::read_footer(&read_data).unwrap();
    assert_eq!(metas.len(), 2);

    // Verify first batch
    let records0 = FlReader::read_segment(&read_data, &metas[0], true).unwrap();
    assert_eq!(records0.len(), 2);
    assert_eq!(records0[0].key, Some(Bytes::from("user-1")));

    // Verify second batch
    let records1 = FlReader::read_segment(&read_data, &metas[1], true).unwrap();
    assert_eq!(records1.len(), 1);
    assert_eq!(records1[0].key, Some(Bytes::from("user-3")));
}

/// Test buffer merging from multiple writers.
#[tokio::test]
async fn test_buffer_merge_multiple_producers() {
    use std::time::Duration;

    let config = BufferConfig {
        max_size_bytes: 10 * 1024 * 1024, // 10 MB
        max_wait: Duration::from_secs(60),
        high_water_bytes: 20 * 1024 * 1024,
        low_water_bytes: 5 * 1024 * 1024,
    };

    let mut buffer = BrokerBuffer::with_config(config);

    // Writer 1 sends to topic 1
    let p1 = WriterId::new();
    let mut rx1 = buffer.insert(
        p1,
        AppendSeq(1),
        vec![RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from("p1-k1")),
                    value: Bytes::from("p1-v1"),
                },
                Record {
                    key: Some(Bytes::from("p1-k2")),
                    value: Bytes::from("p1-v2"),
                },
            ],
        }],
    );

    // Writer 2 sends to same topic 1
    let p2 = WriterId::new();
    let mut rx2 = buffer.insert(
        p2,
        AppendSeq(1),
        vec![RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("p2-k1")),
                value: Bytes::from("p2-v1"),
            }],
        }],
    );

    // Writer 3 sends to topic 2
    let p3 = WriterId::new();
    let mut rx3 = buffer.insert(
        p3,
        AppendSeq(1),
        vec![RecordBatch {
            topic_id: TopicId(2),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("p3-k1")),
                value: Bytes::from("p3-v1"),
            }],
        }],
    );

    // Drain buffer
    let result = buffer.drain();

    // Should have 2 merged batches (topic 1 and topic 2)
    assert_eq!(result.batches.len(), 2);
    assert_eq!(result.pending_writers.len(), 3);

    // Find topic 1 batch - should have 3 records (2 from p1 + 1 from p2)
    let topic1_idx = result
        .batches
        .iter()
        .position(|s| s.topic_id == TopicId(1))
        .unwrap();
    assert_eq!(result.batches[topic1_idx].records.len(), 3);

    // Find topic 2 batch - should have 1 record
    let topic2_idx = result
        .batches
        .iter()
        .position(|s| s.topic_id == TopicId(2))
        .unwrap();
    assert_eq!(result.batches[topic2_idx].records.len(), 1);

    // Simulate offset assignment
    // Topic 1: offsets 0-3, Topic 2: offsets 0-1
    let mut segment_offsets = vec![(0u64, 0u64); 2];
    segment_offsets[topic1_idx] = (0, 3);
    segment_offsets[topic2_idx] = (0, 1);

    // Distribute append_acks
    BrokerBuffer::distribute_acks(result, &segment_offsets);

    // Verify append_acks
    let acks1 = rx1.try_recv().unwrap();
    assert_eq!(acks1.len(), 1);
    assert_eq!(acks1[0].start_offset.0, 0);
    assert_eq!(acks1[0].end_offset.0, 2); // p1 contributed 2 records

    let acks2 = rx2.try_recv().unwrap();
    assert_eq!(acks2.len(), 1);
    assert_eq!(acks2[0].start_offset.0, 2);
    assert_eq!(acks2[0].end_offset.0, 3); // p2 contributed 1 record

    let acks3 = rx3.try_recv().unwrap();
    assert_eq!(acks3.len(), 1);
    assert_eq!(acks3[0].start_offset.0, 0);
    assert_eq!(acks3[0].end_offset.0, 1); // p3 contributed 1 record to topic 2
}

/// Test FL compression efficiency.
#[tokio::test]
async fn test_fl_compression_efficiency() {
    // Create a batch with repetitive data (should compress well)
    let mut records = Vec::with_capacity(1000);
    for i in 0..1000 {
        records.push(Record {
            key: Some(Bytes::from(format!("key-{:05}", i))),
            value: Bytes::from(r#"{"type":"event","data":"this is some repetitive test data that should compress well"}"#),
        });
    }

    let batch = RecordBatch {
        topic_id: TopicId(1),
        schema_id: SchemaId(100),
        records,
    };

    // Calculate uncompressed size
    let uncompressed_size: usize = batch
        .records
        .iter()
        .map(|r| r.key.as_ref().map(|k| k.len()).unwrap_or(0) + r.value.len())
        .sum();

    // Write FL
    let mut writer = FlWriter::new();
    writer.add_segment(&batch).unwrap();
    let fl_data = writer.finish();

    let compressed_size = fl_data.len();
    let compression_ratio = uncompressed_size as f64 / compressed_size as f64;

    println!(
        "Compression: {} bytes -> {} bytes (ratio: {:.2}x)",
        uncompressed_size, compressed_size, compression_ratio
    );

    // ZSTD should achieve at least 2x compression on repetitive data
    assert!(
        compression_ratio > 2.0,
        "Expected compression ratio > 2.0, got {:.2}",
        compression_ratio
    );

    // Verify we can read it back
    let metas = FlReader::read_footer(&fl_data).unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].record_count, 1000);

    let read_records = FlReader::read_segment(&fl_data, &metas[0], true).unwrap();
    assert_eq!(read_records.len(), 1000);
    assert_eq!(read_records[0].key, Some(Bytes::from("key-00000")));
    assert_eq!(read_records[999].key, Some(Bytes::from("key-00999")));
}

/// Test wire protocol roundtrip with large payloads.
#[tokio::test]
async fn test_wire_protocol_large_payload() {
    use flux_wire::writer;

    // Create a large append request with multiple topics
    let mut batches = Vec::new();
    for topic in 0..50 {
        let mut records = Vec::new();
        for i in 0..100 {
            records.push(Record {
                key: Some(Bytes::from(format!("t{}-k{}", topic, i))),
                value: Bytes::from(format!(r#"{{"topic":{},"index":{}}}"#, topic, i)),
            });
        }
        batches.push(RecordBatch {
            topic_id: TopicId(topic),
            schema_id: SchemaId(100),
            records,
        });
    }

    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(42),
        batches,
    };

    // Encode
    let mut buf = vec![0u8; 4 * 1024 * 1024]; // 4 MB buffer
    let len = writer::encode_request(&req, &mut buf);
    buf.truncate(len);

    println!("Encoded {} batches, {} bytes", req.batches.len(), len);

    // Decode
    let (decoded, decoded_len) = writer::decode_request(&buf).unwrap();

    assert_eq!(decoded_len, len);
    assert_eq!(decoded.append_seq.0, 42);
    assert_eq!(decoded.batches.len(), 50); // 50 topics

    // Verify record counts
    for batch in &decoded.batches {
        assert_eq!(batch.records.len(), 100);
    }
}

/// Test end-to-end flow: buffer -> FL -> S3 -> read.
#[tokio::test]
async fn test_e2e_buffer_to_s3() {
    use std::time::Duration;

    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BufferConfig {
        max_size_bytes: 10 * 1024 * 1024,
        max_wait: Duration::from_secs(60),
        high_water_bytes: 20 * 1024 * 1024,
        low_water_bytes: 5 * 1024 * 1024,
    };

    let mut buffer = BrokerBuffer::with_config(config);

    // Simulate 10 writers sending data spread across 3 topics
    let mut receivers = Vec::new();
    for i in 0..10 {
        let writer_id = WriterId::new();
        let rx = buffer.insert(
            writer_id,
            AppendSeq(i as u64),
            vec![RecordBatch {
                topic_id: TopicId(((i % 3) + 1) as u32), // Spread across 3 topics
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from(format!("writer-{}", i))),
                    value: Bytes::from(format!(r#"{{"id":{}}}"#, i)),
                }],
            }],
        );
        receivers.push(rx);
    }

    // Drain and write to S3
    let result = buffer.drain();
    assert_eq!(result.batches.len(), 3); // 3 topics

    let mut writer = FlWriter::new();
    for batch in &result.batches {
        writer.add_segment(batch).unwrap();
    }
    let fl_data = writer.finish();

    let key = "batches/2024-01-15/batch-001.fl";
    store.put(key, fl_data.clone()).await.unwrap();

    // Simulate offset assignment
    let segment_offsets: Vec<(u64, u64)> = result
        .batches
        .iter()
        .map(|s| (0, s.records.len() as u64))
        .collect();

    // Distribute append_acks
    BrokerBuffer::distribute_acks(result, &segment_offsets);

    // Verify all writers got append_acks
    for mut rx in receivers {
        let append_acks = rx.try_recv().unwrap();
        assert_eq!(append_acks.len(), 1);
    }

    // Simulate reader read: read from S3
    let read_data = store.get(key).await.unwrap();
    let metas = FlReader::read_footer(&read_data).unwrap();
    assert_eq!(metas.len(), 3);

    // Count total records
    let total_records: u32 = metas.iter().map(|m| m.record_count).sum();
    assert_eq!(total_records, 10);
}
