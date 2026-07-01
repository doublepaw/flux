// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired linearizability tests.
//!
//! These tests verify:
//! - All acknowledged writes are eventually visible in reads
//! - No two records get the same offset
//! - Final read from offset 0 sees all acknowledged records

mod common;

use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use tokio::sync::Mutex;

use flux_broker::{FlReader, LocalFsStore, ObjectStore};
use flux_common::ids::{Offset, TopicId};
use flux_common::types::Record;

use common::{FaultyObjectStore, TestBrokerConfig, TestBrokerState, TestDb, produce_records};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Read all records from offset 0.
async fn fetch_all_records<S: ObjectStore + Send + Sync>(
    state: &TestBrokerState<S>,
    topic_id: TopicId,
) -> TestResult<(Vec<Record>, Offset)> {
    // Get all batches and watermark in a single transaction to ensure consistency
    let mut tx = state.pool.begin().await?;

    let batches: Vec<(i64, i64, String)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset, s3_key
        FROM topic_batches
        WHERE topic_id = $1
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id.0 as i32)
    .fetch_all(&mut *tx)
    .await?;

    let high_watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(0);

    tx.commit().await?;

    let mut all_records = vec![];

    for (_start_offset, _end_offset, s3_key) in batches {
        let data = state.store.get(&s3_key).await?;
        let segment_metas = FlReader::read_footer(&data)?;

        for seg_meta in &segment_metas {
            if seg_meta.topic_id == topic_id {
                let records = FlReader::read_segment(&data, seg_meta, true)?;
                all_records.extend(records);
            }
        }
    }

    Ok((all_records, Offset(high_watermark as u64)))
}

/// Test that all acknowledged writes are eventually visible.
#[tokio::test]
async fn test_all_acknowledged_writes_visible() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "linearizability-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("ack-visible-test").await as u32);

    // Track all acknowledged writes
    let acked_values: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(vec![]));

    // Multiple writers write concurrently
    let mut handles = vec![];
    for producer_idx in 0..5 {
        let state_clone = state.clone();
        let acked_clone = acked_values.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let value = Bytes::from(format!("writer-{}-record-{}", producer_idx, i));
                let records = vec![Record {
                    key: Some(Bytes::from(format!("key-{}-{}", producer_idx, i))),
                    value: value.clone(),
                }];

                let result = produce_records(&state_clone, topic_id, records).await;
                if result.is_ok() {
                    acked_clone.lock().await.push(value);
                }
            }
        }));
    }

    // Wait for all writers to finish
    for handle in handles {
        handle.await.expect("Writer task should complete");
    }

    // Final read from offset 0
    let (records, _watermark) = fetch_all_records(&state, topic_id)
        .await
        .expect("Read should succeed");

    // All acknowledged values should be present
    let acked = acked_values.lock().await;
    let fetched_values: HashSet<Bytes> = records.iter().map(|r| r.value.clone()).collect();

    for value in acked.iter() {
        assert!(
            fetched_values.contains(value),
            "Acknowledged write {:?} not visible in final read",
            value
        );
    }
}

/// Test that no two records get the same offset.
#[tokio::test]
async fn test_offset_assignment_unique() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "unique-offset-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("unique-offset-test").await as u32);

    let all_offsets: Arc<Mutex<Vec<Offset>>> = Arc::new(Mutex::new(vec![]));

    // Multiple writers write concurrently
    let mut handles = vec![];
    for producer_idx in 0..5 {
        let state_clone = state.clone();
        let offsets_clone = all_offsets.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let records = vec![Record {
                    key: Some(Bytes::from(format!("key-{}-{}", producer_idx, i))),
                    value: Bytes::from(format!("value-{}-{}", producer_idx, i)),
                }];

                if let Ok((start, end)) = produce_records(&state_clone, topic_id, records).await {
                    // Collect all individual offsets in the range
                    let offsets: Vec<Offset> = (start.0..end.0).map(Offset).collect();
                    offsets_clone.lock().await.extend(offsets);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.expect("Writer task should complete");
    }

    // Check all offsets are unique
    let offsets = all_offsets.lock().await;
    let unique: HashSet<u64> = offsets.iter().map(|o| o.0).collect();

    assert_eq!(
        offsets.len(),
        unique.len(),
        "All offsets should be unique, but {} offsets were assigned, only {} unique",
        offsets.len(),
        unique.len()
    );
}

/// Test that final read returns all records up to watermark.
#[tokio::test]
async fn test_final_read_completeness() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "completeness-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("completeness-test").await as u32);

    // Track expected record count
    let expected_count = Arc::new(AtomicU64::new(0));

    // Write records in batches
    let mut handles = vec![];
    for batch_idx in 0..10 {
        let state_clone = state.clone();
        let count_clone = expected_count.clone();
        handles.push(tokio::spawn(async move {
            let batch_size = 5;
            let records: Vec<Record> = (0..batch_size)
                .map(|i| Record {
                    key: Some(Bytes::from(format!("batch-{}-key-{}", batch_idx, i))),
                    value: Bytes::from(format!("batch-{}-value-{}", batch_idx, i)),
                })
                .collect();

            if produce_records(&state_clone, topic_id, records)
                .await
                .is_ok()
            {
                count_clone.fetch_add(batch_size as u64, Ordering::SeqCst);
            }
        }));
    }

    for handle in handles {
        handle.await.expect("Writer task should complete");
    }

    // Final read should see exactly expected_count records
    let (records, watermark) = fetch_all_records(&state, topic_id)
        .await
        .expect("Read should succeed");

    let expected = expected_count.load(Ordering::SeqCst);

    assert_eq!(
        records.len() as u64,
        expected,
        "Final read should return exactly {} records, got {}",
        expected,
        records.len()
    );

    assert_eq!(
        watermark.0, expected,
        "Watermark should be {}, got {}",
        expected, watermark.0
    );
}

/// Test that reads during writes see consistent state.
#[tokio::test]
async fn test_read_write_consistency() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "consistency-test".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store, config));

    let topic_id = TopicId(db.create_topic("rw-consistency-test").await as u32);

    // Writer task
    let state_writer = state.clone();
    let writer_handle = tokio::spawn(async move {
        for i in 0..20 {
            let records = vec![Record {
                key: Some(Bytes::from(format!("key-{}", i))),
                value: Bytes::from(format!("value-{}", i)),
            }];
            let _ = produce_records(&state_writer, topic_id, records).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }
    });

    // Reader task
    let state_reader = state.clone();
    let reader_handle = tokio::spawn(async move {
        let mut watermarks = vec![];
        for _ in 0..10 {
            let result = fetch_all_records(&state_reader, topic_id).await;
            if let Ok((records, watermark)) = result {
                // Count comparison: records visible should be <= watermark
                assert!(
                    records.len() as u64 <= watermark.0,
                    "Records visible ({}) should not exceed watermark ({})",
                    records.len(),
                    watermark.0
                );

                // Contiguity check: verify records are in sequential order with no gaps
                for (i, record) in records.iter().enumerate() {
                    if let Some(ref key) = record.key {
                        let key_str = String::from_utf8_lossy(key);
                        let expected_key = format!("key-{}", i);
                        assert_eq!(
                            key_str, expected_key,
                            "Record {} should have key '{}', but has '{}'",
                            i, expected_key, key_str
                        );
                    }
                }

                watermarks.push(watermark.0);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Watermarks should be monotonically increasing
        for window in watermarks.windows(2) {
            let (prev, curr) = (window[0], window[1]);
            assert!(
                curr >= prev,
                "Watermark should never decrease: {} -> {}",
                prev,
                curr
            );
        }
    });

    writer_handle.await.expect("Writer should complete");
    reader_handle.await.expect("Reader should complete");
}

/// Same as `test_all_acknowledged_writes_visible` but with periodic S3 failures.
#[tokio::test]
async fn test_acknowledged_writes_visible_under_s3_faults() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "ack-visible-s3-fault".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store.clone(), config));

    let topic_id = TopicId(db.create_topic("ack-visible-s3-fault").await as u32);
    let acked_values: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(vec![]));

    // Fault injector: periodically fail S3 puts.
    let store_fault = store.clone();
    let fault_handle = tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            store_fault.fail_next_put();
        }
    });

    let mut handles = vec![];
    for producer_idx in 0..5 {
        let state_clone = state.clone();
        let acked_clone = acked_values.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let value = Bytes::from(format!("writer-{}-record-{}", producer_idx, i));
                let records = vec![Record {
                    key: Some(Bytes::from(format!("key-{}-{}", producer_idx, i))),
                    value: value.clone(),
                }];
                let result = produce_records(&state_clone, topic_id, records).await;
                if result.is_ok() {
                    acked_clone.lock().await.push(value);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.expect("Writer task should complete");
    }
    fault_handle.await.unwrap();

    let (records, _watermark) = fetch_all_records(&state, topic_id)
        .await
        .expect("Read should succeed");

    let acked = acked_values.lock().await;
    let fetched_values: HashSet<Bytes> = records.iter().map(|r| r.value.clone()).collect();

    for value in acked.iter() {
        assert!(
            fetched_values.contains(value),
            "Acknowledged write {:?} not visible under S3 faults",
            value
        );
    }
}

/// Same as `test_offset_assignment_unique` but with S3 failures.
#[tokio::test]
async fn test_offset_uniqueness_under_s3_faults() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "unique-offset-s3-fault".to_string(),
    };
    let state = Arc::new(TestBrokerState::new(db.pool.clone(), store.clone(), config));

    let topic_id = TopicId(db.create_topic("unique-offset-s3-fault").await as u32);
    let all_offsets: Arc<Mutex<Vec<Offset>>> = Arc::new(Mutex::new(vec![]));

    // Periodic S3 failures.
    let store_fault = store.clone();
    let fault_handle = tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            store_fault.fail_next_put();
        }
    });

    let mut handles = vec![];
    for producer_idx in 0..5 {
        let state_clone = state.clone();
        let offsets_clone = all_offsets.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let records = vec![Record {
                    key: Some(Bytes::from(format!("key-{}-{}", producer_idx, i))),
                    value: Bytes::from(format!("value-{}-{}", producer_idx, i)),
                }];
                if let Ok((start, end)) = produce_records(&state_clone, topic_id, records).await {
                    let offsets: Vec<Offset> = (start.0..end.0).map(Offset).collect();
                    offsets_clone.lock().await.extend(offsets);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.expect("Writer task should complete");
    }
    fault_handle.await.unwrap();

    let offsets = all_offsets.lock().await;
    let unique: HashSet<u64> = offsets.iter().map(|o| o.0).collect();
    assert_eq!(
        offsets.len(),
        unique.len(),
        "All offsets should be unique under S3 faults, but {} offsets assigned, only {} unique",
        offsets.len(),
        unique.len()
    );
}
