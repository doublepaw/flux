// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired fault tolerance tests.
//!
//! These tests verify:
//! - S3 failure does not result in false acknowledgment
//! - Reader group state survives broker restart
//! - No partial write visibility
//! - S3 latency does not cause false acks
//!
//! Note: partition assignment tests have been removed as the system
//! no longer uses partitions. Topics have a single offset sequence.

mod common;

use bytes::Bytes;
use std::time::Duration;

use flux_broker::{Coordinator, CoordinatorConfig};
use flux_common::ids::{Offset, TopicId};
use flux_common::types::Record;

use common::{CrashableBroker, FaultyObjectStore, TestDb, produce_records};

/// Test that S3 failure does not result in false acknowledgment.
#[tokio::test]
async fn test_s3_failure_does_not_ack_writes() {
    use flux_broker::ObjectStore;

    let db = TestDb::new().await;
    let broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("s3-failure-test").await as u32);

    // Configure to fail on next put
    broker.faulty_store().fail_next_put();

    let records = vec![Record {
        key: Some(Bytes::from("key")),
        value: Bytes::from("value"),
    }];

    // This should fail because S3 put fails
    let result = produce_records(broker.state(), topic_id, records).await;

    // Should return error, not success
    assert!(
        result.is_err(),
        "S3 failure should propagate as error, not silent success"
    );

    // Verify no records were persisted in DB
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .unwrap();

    assert_eq!(count, 0, "No batches should be recorded when S3 fails");

    // Watermark should still be 0
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    assert_eq!(watermark, 0, "Watermark should be 0 when S3 fails");

    // Verify no ghost files were written to the store
    let files = broker
        .faulty_store()
        .list("")
        .await
        .expect("List should succeed");

    assert!(
        files.is_empty(),
        "No files should be written to store when S3 fails, but found: {:?}",
        files
    );
}

/// Test that reader group state survives coordinator restart.
#[tokio::test]
async fn test_consumer_survives_broker_restart() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("survive-restart").await as u32);

    // First coordinator session
    {
        let coordinator = Coordinator::new(db.pool.clone(), config.clone());

        coordinator
            .join_group("survive-group", topic_id, "reader-1")
            .await
            .expect("Join should succeed");

        // Simulate inflight range
        sqlx::query(
            r#"
            INSERT INTO reader_inflight
            (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '45 seconds')
            "#,
        )
        .bind("survive-group")
        .bind(topic_id.0 as i32)
        .bind(0i64)
        .bind(42i64)
        .bind("reader-1")
        .execute(&db.pool)
        .await
        .expect("Insert inflight");

        sqlx::query(
            "UPDATE reader_group_state SET dispatch_cursor = 42 WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("survive-group")
        .bind(topic_id.0 as i32)
        .execute(&db.pool)
        .await
        .expect("Update dispatch cursor");

        // Commit range [0, 42)
        coordinator
            .commit_range("survive-group", topic_id, "reader-1", Offset(0), Offset(42))
            .await
            .expect("Commit should succeed");
    }
    // Coordinator dropped (simulates process restart)

    // Second coordinator session (new instance)
    {
        let coordinator = Coordinator::new(db.pool.clone(), config);

        // Send heartbeat -- reader should still be known
        let hb = coordinator
            .heartbeat("survive-group", topic_id, "reader-1")
            .await
            .expect("Heartbeat should succeed");

        assert_eq!(
            hb,
            flux_broker::HeartbeatStatus::Ok,
            "Reader should survive restart"
        );

        // Verify group state is still in DB
        let state_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM reader_group_state WHERE group_id = $1 AND topic_id = $2)",
        )
        .bind("survive-group")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("Query should succeed");

        assert!(state_exists, "Group state should survive restart");
    }
}

/// Test that reads see consistent state during writes (no partial visibility).
#[tokio::test]
async fn test_no_partial_write_visibility() {
    let db = TestDb::new().await;
    let broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("no-partial-test").await as u32);

    // Append a batch of records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(broker.state(), topic_id, records)
        .await
        .expect("Append should succeed");

    // Query batches - should see either 0 or 10 records, never partial
    let batch_info: Vec<(i64, i64, i32)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset, record_count
        FROM topic_batches
        WHERE topic_id = $1
        "#,
    )
    .bind(topic_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .expect("Query should succeed");

    // Each batch should have consistent start/end/count
    for (start, end, count) in &batch_info {
        let expected_count = end - start;
        assert_eq!(
            *count as i64, expected_count,
            "Batch record_count {} should equal end-start {}",
            count, expected_count
        );
    }

    // Watermark should match end of last batch
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    if let Some((_, end, _)) = batch_info.last() {
        assert_eq!(
            watermark, *end,
            "Watermark {} should equal end of last batch {}",
            watermark, end
        );
    }
}

/// Test that S3 latency does not cause false acks (slow != broken).
/// Writes with 500ms S3 latency should still succeed with correct offsets.
#[tokio::test]
async fn test_s3_latency_does_not_cause_false_ack() {
    use std::sync::Arc;
    use tempfile::TempDir;

    use flux_broker::{FlReader, LocalFsStore, ObjectStore};
    use flux_common::types::Record;

    use common::TestBrokerConfig;

    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "s3-latency-test".to_string(),
    };

    // 500ms latency on every put.
    store.set_put_delay_ms(500);

    let state = Arc::new(common::TestBrokerState::new(
        db.pool.clone(),
        store.clone(),
        config,
    ));

    let topic_id = TopicId(db.create_topic("s3-latency-test").await as u32);

    // Write 5 records -- each will be slow but should succeed.
    let mut acked_offsets = vec![];
    for i in 0..5 {
        let records = vec![Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("slow-value-{}", i)),
        }];
        let (start, end) = produce_records(&state, topic_id, records)
            .await
            .expect("Write should succeed despite S3 latency");
        acked_offsets.push((start, end));
    }

    // All offsets should be contiguous and non-overlapping.
    for i in 1..acked_offsets.len() {
        assert_eq!(
            acked_offsets[i].0.0,
            acked_offsets[i - 1].1.0,
            "Offsets should be contiguous: batch {} ends at {}, batch {} starts at {}",
            i - 1,
            acked_offsets[i - 1].1.0,
            i,
            acked_offsets[i].0.0
        );
    }

    // Final watermark should equal the end of the last batch.
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets \
         WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    assert_eq!(
        watermark as u64,
        acked_offsets.last().unwrap().1.0,
        "Watermark should match the last acked offset"
    );

    // Read back records and verify all 5 are present.
    let batches: Vec<(String,)> =
        sqlx::query_as("SELECT s3_key FROM topic_batches WHERE topic_id = $1")
            .bind(topic_id.0 as i32)
            .fetch_all(&db.pool)
            .await
            .unwrap();

    let mut all_records = vec![];
    for (s3_key,) in batches {
        let data = store.get(&s3_key).await.unwrap();
        let metas = FlReader::read_footer(&data).unwrap();
        for meta in &metas {
            if meta.topic_id == topic_id {
                let recs = FlReader::read_segment(&data, meta, true).unwrap();
                all_records.extend(recs);
            }
        }
    }

    assert_eq!(
        all_records.len(),
        5,
        "All 5 records should be readable despite S3 latency"
    );
}
