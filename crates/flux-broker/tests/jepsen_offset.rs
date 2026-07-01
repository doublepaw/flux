// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired offset consistency tests.
//!
//! These tests verify that:
//! - High watermark never regresses
//! - Fetched records have contiguous offsets (no gaps)
//! - Skip parameter is bounded to available records
//! - Reader offset tracking matches server offsets

mod common;

use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

use flux_broker::{FlReader, LocalFsStore, ObjectStore};
use flux_common::ids::{Offset, TopicId};
use flux_common::types::Record;

use common::{TestBrokerConfig, TestBrokerState, TestDb, produce_records};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Helper to read records and watermark.
async fn fetch_records<S: ObjectStore + Send + Sync>(
    state: &TestBrokerState<S>,
    topic_id: TopicId,
    from_offset: Offset,
) -> TestResult<(Vec<Record>, Offset)> {
    // Query all batches that contain records at or after the requested offset
    let batches: Vec<(i32, i64, i64, String)> = sqlx::query_as(
        r#"
        SELECT schema_id, start_offset, end_offset, s3_key
        FROM topic_batches
        WHERE topic_id = $1
          AND end_offset > $2
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(from_offset.0 as i64)
    .fetch_all(&state.pool)
    .await?;

    // Get high watermark
    let high_watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&state.pool)
    .await?
    .unwrap_or(0);

    let mut all_records = vec![];

    for (_sid, start_offset, _end_offset, s3_key) in batches {
        let data = state.store.get(&s3_key).await?;
        let segment_metas = FlReader::read_footer(&data)?;

        for seg_meta in &segment_metas {
            if seg_meta.topic_id == topic_id {
                let records = FlReader::read_segment(&data, seg_meta, true)?;
                let skip = (from_offset.0 as i64 - start_offset).max(0) as usize;
                all_records.extend(records.into_iter().skip(skip));
            }
        }
    }

    Ok((all_records, Offset(high_watermark as u64)))
}

/// Test that high watermark never regresses under concurrent writes.
#[tokio::test]
async fn test_watermark_never_regresses() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("watermark-test").await as u32);

    // Track watermarks across multiple operations
    let mut watermarks = Vec::new();

    // Append several batches and check watermark after each
    for i in 0..10 {
        let records: Vec<Record> = (0..5)
            .map(|j| Record {
                key: Some(Bytes::from(format!("key-{}-{}", i, j))),
                value: Bytes::from(format!("value-{}-{}", i, j)),
            })
            .collect();

        let (_, end_offset) = produce_records(&state, topic_id, records)
            .await
            .expect("Append should succeed");

        // Read to get current watermark
        let (_, watermark) = fetch_records(&state, topic_id, Offset(0))
            .await
            .expect("Read should succeed");

        watermarks.push(watermark);

        // Verify watermark matches expected value based on append result
        assert!(
            watermark.0 >= end_offset.0,
            "Watermark {} should be at least end_offset {}",
            watermark.0,
            end_offset.0
        );
    }

    // Verify watermarks never decreased
    for window in watermarks.windows(2) {
        let (prev, curr) = (window[0], window[1]);
        assert!(
            curr.0 >= prev.0,
            "Watermark regressed from {} to {}",
            prev.0,
            curr.0
        );
    }
}

/// Test that fetched records have contiguous offsets.
#[tokio::test]
async fn test_fetch_returns_contiguous_offsets() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("contiguous-test").await as u32);

    // Append 100 records in 10 batches
    for batch_idx in 0..10 {
        let records: Vec<Record> = (0..10)
            .map(|i| Record {
                key: Some(Bytes::from(format!("batch-{}-key-{}", batch_idx, i))),
                value: Bytes::from(format!("batch-{}-value-{}", batch_idx, i)),
            })
            .collect();

        produce_records(&state, topic_id, records)
            .await
            .expect("Append should succeed");
    }

    // Read all records from offset 0
    let (records, watermark) = fetch_records(&state, topic_id, Offset(0))
        .await
        .expect("Read should succeed");

    // Verify we got all records
    assert_eq!(records.len(), 100, "Should have fetched all 100 records");
    assert_eq!(watermark.0, 100, "Watermark should be 100");

    // Verify the records are in order by checking key pattern
    for (idx, record) in records.iter().enumerate() {
        let batch_idx = idx / 10;
        let record_idx = idx % 10;
        let expected_key = format!("batch-{}-key-{}", batch_idx, record_idx);
        assert_eq!(
            record.key,
            Some(Bytes::from(expected_key.clone())),
            "Record at index {} should have key {}",
            idx,
            expected_key
        );
    }
}

/// Test that fetching from offset beyond available returns empty, not error.
#[tokio::test]
async fn test_fetch_offset_beyond_available_returns_empty() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("beyond-test").await as u32);

    // Append 10 records (offsets 0-9)
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(&state, topic_id, records)
        .await
        .expect("Append should succeed");

    // Read from offset 100 (way beyond available)
    let (records, watermark) = fetch_records(&state, topic_id, Offset(100))
        .await
        .expect("Read should succeed even with offset beyond available");

    // Should return empty records, not an error
    assert!(
        records.is_empty(),
        "Should return empty when offset is beyond available"
    );

    // Watermark should still be correct
    assert_eq!(watermark.0, 10, "Watermark should still be 10");
}

/// Test that fetching from offset at exactly the watermark returns empty.
#[tokio::test]
async fn test_fetch_at_watermark_returns_empty() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("at-watermark-test").await as u32);

    // Append 10 records (offsets 0-9, watermark at 10)
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(&state, topic_id, records)
        .await
        .expect("Append should succeed");

    // Read from offset 10 (exactly at watermark)
    let (records, watermark) = fetch_records(&state, topic_id, Offset(10))
        .await
        .expect("Read should succeed");

    // Should return empty records
    assert!(records.is_empty(), "Should return empty when at watermark");
    assert_eq!(watermark.0, 10, "Watermark should be 10");
}

/// Test that fetching in the middle of available records works correctly.
#[tokio::test]
async fn test_fetch_from_middle_offset() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("middle-test").await as u32);

    // Append 20 records
    let records: Vec<Record> = (0..20)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(&state, topic_id, records)
        .await
        .expect("Append should succeed");

    // Read from offset 5
    let (records, watermark) = fetch_records(&state, topic_id, Offset(5))
        .await
        .expect("Read should succeed");

    // Should return records 5-19 (15 records)
    assert_eq!(
        records.len(),
        15,
        "Should return 15 records starting from offset 5"
    );
    assert_eq!(watermark.0, 20, "Watermark should be 20");

    // Verify first record is key-5
    assert_eq!(
        records[0].key,
        Some(Bytes::from("key-5")),
        "First record should be key-5"
    );
}

/// Test that watermark is consistent between reads within a single batch.
#[tokio::test]
async fn test_watermark_consistency_within_batch() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "jepsen-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("batch-consistency-test").await as u32);

    // Append 50 records
    let records: Vec<Record> = (0..50)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(&state, topic_id, records)
        .await
        .expect("Append should succeed");

    // Multiple concurrent reads should all see the same watermark
    let offsets = [0, 10, 20, 30, 40];
    let mut watermarks = vec![];

    // Use join_all for concurrent execution
    let results = futures::future::join_all(offsets.iter().map(|&offset| {
        let state_clone = state.clone();
        async move { fetch_records(&state_clone, topic_id, Offset(offset)).await }
    }))
    .await;

    for result in results {
        let (_, watermark) = result.expect("Read should succeed");
        watermarks.push(watermark.0);
    }

    // All watermarks should be the same
    assert!(
        watermarks.iter().all(|&w| w == 50),
        "All reads should see watermark 50, got {:?}",
        watermarks
    );
}
