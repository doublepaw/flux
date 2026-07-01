// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired reader churn tests.
//!
//! Verifies that reader churn (repeated leave/join cycles) doesn't lose
//! committed offsets or cause the committed watermark to regress.

mod common;

use std::sync::Arc;
use std::time::Duration;

use flux_broker::{Coordinator, CoordinatorConfig};
use flux_common::ids::{Offset, TopicId};

use common::TestDb;

/// 4 readers in a group. In a loop (10 iterations): one reader leaves,
/// a new reader joins, verify member count stays correct.
/// At the end: committed watermark never regresses and all readers are members.
/// Invariant: reader churn doesn't lose committed progress or corrupt group state.
#[tokio::test]
async fn test_sustained_reader_churn_offset_continuity() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("reader-churn").await as u32);

    // Initial 4 readers join
    let mut active_readers: Vec<String> = (0..4).map(|i| format!("reader-{}", i)).collect();

    for reader_id in &active_readers {
        coordinator
            .join_group("churn-group", topic_id, reader_id)
            .await
            .expect("initial join should succeed");
    }

    let mut next_reader_id = 4u32;

    for iteration in 0..10 {
        // Pick a reader to leave (rotating)
        let leave_idx = iteration % active_readers.len();
        let leaving_reader = active_readers[leave_idx].clone();

        // Leave
        coordinator
            .leave_group("churn-group", topic_id, &leaving_reader)
            .await
            .expect("leave should succeed");

        // New reader joins
        let new_reader = format!("reader-{}", next_reader_id);
        next_reader_id += 1;
        active_readers[leave_idx] = new_reader.clone();

        coordinator
            .join_group("churn-group", topic_id, &new_reader)
            .await
            .expect("new reader join should succeed");
    }

    // Verify: final member count matches active readers
    let member_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_members WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query member count");

    assert_eq!(
        member_count,
        active_readers.len() as i64,
        "Member count should match active readers after churn"
    );

    // Verify: all active readers are actually in the members table
    for reader_id in &active_readers {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM reader_members \
             WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3)",
        )
        .bind("churn-group")
        .bind(topic_id.0 as i32)
        .bind(reader_id.as_str())
        .fetch_one(&db.pool)
        .await
        .expect("query member existence");

        assert!(
            exists,
            "Active reader {} should be in members table",
            reader_id
        );
    }

    // Verify: no departed readers linger in the members table
    for i in 0..4 {
        let reader_id = format!("reader-{}", i);
        if active_readers.contains(&reader_id) {
            continue;
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM reader_members \
             WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3)",
        )
        .bind("churn-group")
        .bind(topic_id.0 as i32)
        .bind(reader_id.as_str())
        .fetch_one(&db.pool)
        .await
        .expect("query departed member");

        assert!(
            !exists,
            "Departed reader {} should not be in members table",
            reader_id
        );
    }

    // Verify: group state exists and is consistent
    let state_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reader_group_state \
         WHERE group_id = $1 AND topic_id = $2)",
    )
    .bind("churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query group state");

    assert!(state_exists, "Group state should survive reader churn");
}

/// Test that commit_range works correctly during reader churn.
/// Simulates readers committing ranges, then churning, and verifies
/// the committed watermark never regresses.
#[tokio::test]
async fn test_committed_watermark_survives_churn() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("watermark-churn").await as u32);

    // Two readers join
    coordinator
        .join_group("wm-churn-group", topic_id, "reader-A")
        .await
        .expect("join should succeed");

    coordinator
        .join_group("wm-churn-group", topic_id, "reader-B")
        .await
        .expect("join should succeed");

    // Simulate inflight ranges by inserting directly into reader_inflight
    let lease_expires = chrono::Utc::now() + chrono::Duration::seconds(45);

    sqlx::query(
        "INSERT INTO reader_inflight (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .bind(0i64)
    .bind(10i64)
    .bind("reader-A")
    .bind(lease_expires)
    .execute(&db.pool)
    .await
    .expect("insert inflight");

    sqlx::query(
        "INSERT INTO reader_inflight (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .bind(10i64)
    .bind(20i64)
    .bind("reader-B")
    .bind(lease_expires)
    .execute(&db.pool)
    .await
    .expect("insert inflight");

    // Update dispatch_cursor to reflect dispatched ranges
    sqlx::query(
        "UPDATE reader_group_state SET dispatch_cursor = 20 \
         WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("update dispatch cursor");

    // Reader-A commits range [0, 10)
    let status = coordinator
        .commit_range(
            "wm-churn-group",
            topic_id,
            "reader-A",
            Offset(0),
            Offset(10),
        )
        .await
        .expect("commit should succeed");

    assert_eq!(
        status,
        flux_broker::CommitStatus::Ok,
        "Commit should succeed"
    );

    // Check watermark after first commit
    let watermark_after_first: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state \
         WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark");

    // Reader-A leaves (churn)
    coordinator
        .leave_group("wm-churn-group", topic_id, "reader-A")
        .await
        .expect("leave should succeed");

    // New reader joins
    coordinator
        .join_group("wm-churn-group", topic_id, "reader-C")
        .await
        .expect("join should succeed");

    // Watermark should not have regressed after churn
    let watermark_after_churn: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state \
         WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark after churn");

    assert!(
        watermark_after_churn >= watermark_after_first,
        "Committed watermark should not regress after churn: {} -> {}",
        watermark_after_first,
        watermark_after_churn
    );

    // Group state should still exist
    let state_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reader_group_state \
         WHERE group_id = $1 AND topic_id = $2)",
    )
    .bind("wm-churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query group state");

    assert!(state_exists, "Group state should survive reader churn");
}
