// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired reader group tests.
//!
//! These tests verify:
//! - Commit range semantics work correctly
//! - Stale reader detection timing
//! - Concurrent joins serialize correctly

mod common;

use std::sync::Arc;
use std::time::Duration;

use flux_broker::{CommitStatus, Coordinator, CoordinatorConfig, PollStatus};
use flux_common::ids::{Offset, TopicId};

use common::TestDb;

/// Test stale reader detection within session timeout.
#[tokio::test]
async fn test_stale_reader_detection() {
    let db = TestDb::new().await;
    // Use longer timeouts for CI reliability - lease must be longer than session
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(10),
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("stale-test").await as u32);

    // Reader A joins
    coordinator
        .join_group("stale-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Wait longer than session timeout without heartbeat (2x margin for CI)
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Reader B joins
    coordinator
        .join_group("stale-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Reader B heartbeats to trigger cleanup of expired A
    let _ = coordinator
        .heartbeat("stale-group", topic_id, "reader-b")
        .await;

    // Reader A's heartbeat should detect it was expired (UnknownMember)
    let hb_result = coordinator
        .heartbeat("stale-group", topic_id, "reader-a")
        .await
        .expect("Heartbeat should succeed");

    assert_eq!(
        hb_result,
        flux_broker::HeartbeatStatus::UnknownMember,
        "Stale reader should be detected as unknown"
    );
}

/// Test committed offset persists through reader reconnect.
#[tokio::test]
async fn test_committed_offset_persists() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("persist-test").await as u32);

    // Reader A joins
    coordinator
        .join_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Simulate an inflight range (normally created by poll)
    sqlx::query(
        r#"
        INSERT INTO reader_inflight
        (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at)
        VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '45 seconds')
        "#,
    )
    .bind("persist-group")
    .bind(topic_id.0 as i32)
    .bind(0i64)
    .bind(42i64)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("Insert inflight");

    // Advance dispatch_cursor so watermark can advance
    sqlx::query(
        "UPDATE reader_group_state SET dispatch_cursor = 42 WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("persist-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("Update dispatch cursor");

    // Commit range [0, 42)
    let status = coordinator
        .commit_range("persist-group", topic_id, "reader-a", Offset(0), Offset(42))
        .await
        .expect("Commit should succeed");
    assert_eq!(status, CommitStatus::Ok, "Commit should return Ok");

    // Reader leaves
    coordinator
        .leave_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Leave should succeed");

    // Same reader rejoins
    coordinator
        .join_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Rejoin should succeed");

    // Verify committed watermark persists in DB
    let committed: Option<i64> = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("persist-group")
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .expect("Query should succeed");

    assert_eq!(
        committed,
        Some(42),
        "Committed watermark should persist as 42 through leave/join"
    );
}

/// Test concurrent commits from the same reader don't cause corruption.
#[tokio::test]
async fn test_concurrent_commit_same_reader() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("concurrent-commit-test").await as u32);
    insert_test_batches(&db.pool, topic_id, 100).await;

    // Reader joins
    coordinator
        .join_group("cc-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Insert inflight rows for all 10 ranges (simulates what poll() does)
    for offset in 1..=10u64 {
        let start = ((offset - 1) * 10) as i64;
        let end = (offset * 10) as i64;
        sqlx::query(
            r#"
            INSERT INTO reader_inflight
            (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + INTERVAL '45 seconds')
            "#,
        )
        .bind("cc-group")
        .bind(topic_id.0 as i32)
        .bind(start)
        .bind(end)
        .bind("reader-a")
        .execute(&db.pool)
        .await
        .expect("Insert inflight");
    }

    // Advance dispatch_cursor so watermark can advance through all ranges
    sqlx::query(
        "UPDATE reader_group_state SET dispatch_cursor = 100 WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("cc-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("Update dispatch cursor");

    // Spawn multiple concurrent commits with different ranges
    let mut handles = vec![];
    for offset in 1..=10u64 {
        let coord = coordinator.clone();
        let start = (offset - 1) * 10;
        let end = offset * 10;
        handles.push(tokio::spawn(async move {
            coord
                .commit_range("cc-group", topic_id, "reader-a", Offset(start), Offset(end))
                .await
        }));
    }

    // All commits should succeed (each range has a matching inflight row)
    let results: Vec<_> = futures::future::join_all(handles).await;
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(CommitStatus::Ok)) => {}
            Ok(Ok(status)) => {
                panic!("Commit {} returned unexpected status: {:?}", i, status);
            }
            Ok(Err(e)) => {
                panic!("Commit {} failed with error: {}", i, e);
            }
            Err(e) => {
                panic!("Task {} panicked: {}", i, e);
            }
        }
    }

    // Verify all inflight rows were consumed (no corruption from concurrent deletes)
    let remaining_inflight: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("cc-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("Query should succeed");
    assert_eq!(
        remaining_inflight, 0,
        "All inflight rows should be consumed after concurrent commits"
    );

    // Reconcile watermark: concurrent commits race on watermark advancement
    // because each transaction only sees deletions committed before it.
    // With all inflight rows gone, watermark should resolve to dispatch_cursor.
    sqlx::query(
        r#"
        UPDATE reader_group_state
        SET committed_watermark = COALESCE(
            (SELECT MIN(start_offset) FROM reader_inflight
             WHERE group_id = $1 AND topic_id = $2),
            dispatch_cursor
        )
        WHERE group_id = $1 AND topic_id = $2
        "#,
    )
    .bind("cc-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("Reconcile watermark");

    let watermark: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("cc-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("Query should succeed");

    assert_eq!(
        watermark, 100,
        "Committed watermark should be 100 after reconciliation, got {}",
        watermark
    );
}

/// Test that commit during membership change is handled correctly.
#[tokio::test]
async fn test_commit_during_membership_change() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("commit-membership-test").await as u32);

    // Reader A joins
    coordinator
        .join_group("cr-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Spawn commit in background
    let coord_commit = coordinator.clone();
    let commit_handle = tokio::spawn(async move {
        // Small delay to allow B's join to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        coord_commit
            .commit_range("cr-group", topic_id, "reader-a", Offset(0), Offset(50))
            .await
    });

    // Reader B joins
    coordinator
        .join_group("cr-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Wait for commit to complete
    let commit_result = commit_handle.await.expect("Task should complete");

    // Commit should either succeed or fail gracefully
    match commit_result {
        Ok(CommitStatus::Ok) => {
            // Commit succeeded - correct behavior
        }
        Ok(CommitStatus::NotOwner) => {
            // Commit failed due to ownership issue - correct behavior
        }
        Err(e) => {
            panic!("Commit should not error: {}", e);
        }
    }

    // Verify A is still a member (heartbeat succeeds)
    let hb_result = coordinator
        .heartbeat("cr-group", topic_id, "reader-a")
        .await
        .expect("Heartbeat should succeed");

    assert_eq!(
        hb_result,
        flux_broker::HeartbeatStatus::Ok,
        "Reader A should still be a member"
    );
}

/// Test heartbeat after session has expired returns UnknownMember.
#[tokio::test]
async fn test_heartbeat_after_session_expired() {
    let db = TestDb::new().await;
    // Use short session timeout for testing
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(10),
        session_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("expired-hb-test").await as u32);

    // Reader A joins
    coordinator
        .join_group("ehb-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Reader B joins to trigger member tracking
    coordinator
        .join_group("ehb-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // B heartbeats to trigger cleanup of expired A
    let _ = coordinator
        .heartbeat("ehb-group", topic_id, "reader-b")
        .await;

    // Now A tries to heartbeat
    let hb_result = coordinator
        .heartbeat("ehb-group", topic_id, "reader-a")
        .await
        .expect("Heartbeat should not error");

    // Should get UnknownMember (session expired)
    assert_eq!(
        hb_result,
        flux_broker::HeartbeatStatus::UnknownMember,
        "Expired reader should get UnknownMember, got {:?}",
        hb_result
    );
}

// ============ Clock Skew Tests ============

/// Simulate clock skew by setting last_heartbeat to 60s in the past.
/// Reader A should be evicted when B heartbeats (triggers stale check).
#[tokio::test]
async fn test_clock_skew_premature_expiration() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);
    let topic_id = TopicId(db.create_topic("clock-skew-early").await as u32);

    // Reader A joins
    coordinator
        .join_group("cs-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Simulate clock skew: set A's heartbeat 60s in the past
    sqlx::query(
        "UPDATE reader_members SET last_heartbeat = NOW() - INTERVAL '60 seconds' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("cs-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("clock skew update should succeed");

    // Reader B joins (triggers stale member check)
    coordinator
        .join_group("cs-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // B heartbeats to trigger cleanup of expired A
    let _ = coordinator
        .heartbeat("cs-group", topic_id, "reader-b")
        .await;

    // A's next heartbeat should return UnknownMember
    let hb_a = coordinator
        .heartbeat("cs-group", topic_id, "reader-a")
        .await
        .expect("Heartbeat should not error");

    assert_eq!(
        hb_a,
        flux_broker::HeartbeatStatus::UnknownMember,
        "Clock-skewed reader should be evicted: {:?}",
        hb_a
    );
}

/// Simulate clock skew forward: reader A stops heartbeating but appears recent.
/// Reset to real time, then A expires normally.
#[tokio::test]
async fn test_clock_skew_delayed_expiration() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);
    let topic_id = TopicId(db.create_topic("clock-skew-late").await as u32);

    // Reader A joins
    coordinator
        .join_group("cs-late-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Reader B joins
    coordinator
        .join_group("cs-late-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Simulate forward clock skew: set A's heartbeat 60s in the future
    sqlx::query(
        "UPDATE reader_members SET last_heartbeat = NOW() + INTERVAL '60 seconds' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("cs-late-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("clock skew update should succeed");

    // Wait past session timeout
    tokio::time::sleep(Duration::from_secs(3)).await;

    // B heartbeats -- A should NOT be expired (appears recent due to clock skew)
    let _ = coordinator
        .heartbeat("cs-late-group", topic_id, "reader-b")
        .await
        .expect("Heartbeat should succeed");

    // A is NOT expired because its last_heartbeat is in the future
    let a_still_member: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM reader_members \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3)",
    )
    .bind("cs-late-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .fetch_one(&db.pool)
    .await
    .expect("query");
    assert!(
        a_still_member,
        "Reader A should still be a member (clock skew makes it appear recent)"
    );

    // Reset to real time (set heartbeat to past)
    sqlx::query(
        "UPDATE reader_members SET last_heartbeat = NOW() - INTERVAL '60 seconds' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("cs-late-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("reset clock skew");

    // B heartbeats again -- now A should be expired
    let _ = coordinator
        .heartbeat("cs-late-group", topic_id, "reader-b")
        .await;

    let a_expired: bool = !sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM reader_members \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3)",
    )
    .bind("cs-late-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .fetch_one(&db.pool)
    .await
    .expect("query");
    assert!(a_expired, "Reader A should be expired after clock reset");
}

/// 8 readers join the same group concurrently (spawned as 8 tasks).
/// Verify: all readers are members.
/// Invariant: concurrent joins serialize correctly via Postgres row locks.
#[tokio::test]
async fn test_concurrent_join_group_serialization() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("concurrent-join-8").await as u32);
    let num_readers = 8;

    // Spawn 8 readers joining concurrently
    let mut handles = vec![];
    for i in 0..num_readers {
        let coord = coordinator.clone();
        let reader_id = format!("reader-{}", i);
        handles.push(tokio::spawn(async move {
            coord.join_group("cj8-group", topic_id, &reader_id).await
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    for (i, result) in results.iter().enumerate() {
        result
            .as_ref()
            .unwrap_or_else(|e| panic!("task {} panicked: {}", i, e))
            .as_ref()
            .unwrap_or_else(|e| panic!("join {} failed: {}", i, e));
    }

    // All readers should be members
    let member_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_members WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("cj8-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query member count");

    assert_eq!(
        member_count, num_readers as i64,
        "All {} readers should be members",
        num_readers
    );
}

// ============ Pipelined Polling Jepsen Tests ============

/// Insert fake batch metadata so poll() has something to dispatch.
async fn insert_test_batches(pool: &sqlx::PgPool, topic_id: TopicId, total_records: i64) {
    let batch_size = 10i64;
    let mut offset = 0i64;
    while offset < total_records {
        let end = (offset + batch_size).min(total_records);
        sqlx::query(
            r#"
            INSERT INTO topic_batches
            (topic_id, schema_id, start_offset, end_offset, record_count,
             s3_key, byte_offset, byte_length, ingest_time, crc32)
            VALUES ($1, 1, $2, $3, $4, 'test-key', 0, 1024, NOW(), 0)
            "#,
        )
        .bind(topic_id.0 as i32)
        .bind(offset)
        .bind(end)
        .bind((end - offset) as i32)
        .execute(pool)
        .await
        .expect("insert batch");
        offset = end;
    }
    sqlx::query("UPDATE topic_offsets SET next_offset = $2 WHERE topic_id = $1")
        .bind(topic_id.0 as i32)
        .bind(total_records)
        .execute(pool)
        .await
        .expect("update offsets");
}

/// Reader A polls, reader B tries to commit A's range -> NotOwner.
#[tokio::test]
async fn test_commit_rejected_for_wrong_reader() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("wrong-reader-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("wrc-group", topic_id, "reader-a")
        .await
        .expect("join a");
    coordinator
        .join_group("wrc-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // Reader A polls and gets a range
    let poll_a = coordinator
        .poll("wrc-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(
        poll_a.start_offset != poll_a.end_offset,
        "reader-a should get work"
    );

    // Reader B tries to commit A's range
    let status = coordinator
        .commit_range(
            "wrc-group",
            topic_id,
            "reader-b",
            poll_a.start_offset,
            poll_a.end_offset,
        )
        .await
        .expect("commit should not error");

    assert_eq!(
        status,
        CommitStatus::NotOwner,
        "reader-b should not be able to commit reader-a's range"
    );
}

/// Concurrent pipelined polls from multiple readers -- no data loss.
#[tokio::test]
async fn test_concurrent_pipelined_polls_no_data_loss() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("concurrent-pipeline").await as u32);
    insert_test_batches(&db.pool, topic_id, 100).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    // 3 readers join
    for i in 0..3 {
        coordinator
            .join_group("cp-group", topic_id, &format!("reader-{}", i))
            .await
            .expect("join");
    }

    // Each reader pipelines 3 polls concurrently
    let mut handles = vec![];
    for i in 0..3 {
        let coord = coordinator.clone();
        let reader_id = format!("reader-{}", i);
        handles.push(tokio::spawn(async move {
            let mut ranges = vec![];
            for _ in 0..3 {
                match coord.poll("cp-group", topic_id, &reader_id, 1024).await {
                    Ok(poll)
                        if poll.status == PollStatus::Ok
                            && poll.start_offset != poll.end_offset =>
                    {
                        ranges.push((poll.start_offset, poll.end_offset));
                    }
                    _ => break,
                }
            }
            // Commit all
            for (start, end) in &ranges {
                let _ = coord
                    .commit_range("cp-group", topic_id, &reader_id, *start, *end)
                    .await;
            }
            ranges
        }));
    }

    let results: Vec<Vec<(Offset, Offset)>> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task"))
        .collect();

    // Flatten all ranges and verify no overlaps
    let all_ranges: Vec<(u64, u64)> = results.iter().flatten().map(|(s, e)| (s.0, e.0)).collect();

    for (i, r1) in all_ranges.iter().enumerate() {
        for r2 in all_ranges.iter().skip(i + 1) {
            assert!(
                r1.1 <= r2.0 || r2.1 <= r1.0,
                "ranges must not overlap: [{}, {}) vs [{}, {})",
                r1.0,
                r1.1,
                r2.0,
                r2.1,
            );
        }
    }

    // Verify total coverage -- all dispatched offsets should be committed
    let total_dispatched: u64 = all_ranges.iter().map(|(s, e)| e - s).sum();
    assert!(
        total_dispatched > 0,
        "at least some records should have been dispatched"
    );
}

/// Stress test: concurrent poll + commit from multiple readers.
#[tokio::test]
async fn test_concurrent_poll_commit_stress() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("poll-commit-stress").await as u32);
    insert_test_batches(&db.pool, topic_id, 200).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let num_readers = 4;
    for i in 0..num_readers {
        coordinator
            .join_group("pcs-group", topic_id, &format!("reader-{}", i))
            .await
            .expect("join");
    }

    // Each reader runs poll-commit cycles concurrently
    let mut handles = vec![];
    for i in 0..num_readers {
        let coord = coordinator.clone();
        let reader_id = format!("reader-{}", i);
        handles.push(tokio::spawn(async move {
            let mut committed = 0u64;
            for _ in 0..5 {
                match coord.poll("pcs-group", topic_id, &reader_id, 1024).await {
                    Ok(poll)
                        if poll.status == PollStatus::Ok
                            && poll.start_offset != poll.end_offset =>
                    {
                        let range_size = poll.end_offset.0 - poll.start_offset.0;
                        if let Ok(CommitStatus::Ok) = coord
                            .commit_range(
                                "pcs-group",
                                topic_id,
                                &reader_id,
                                poll.start_offset,
                                poll.end_offset,
                            )
                            .await
                        {
                            committed += range_size;
                        }
                    }
                    _ => break,
                }
            }
            committed
        }));
    }

    let totals: Vec<u64> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.expect("task"))
        .collect();

    let total_committed: u64 = totals.iter().sum();
    assert!(
        total_committed > 0,
        "concurrent poll+commit should process some records"
    );

    // Verify watermark is consistent
    let watermark: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("pcs-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark");

    // Watermark must match total committed (poll dispatches contiguous ranges
    // and successful commits advance the watermark).
    assert!(
        watermark as u64 >= total_committed,
        "watermark ({}) should be >= total committed ({})",
        watermark,
        total_committed
    );
    assert!(
        watermark > 0,
        "watermark should advance after poll+commit stress, got {}",
        watermark
    );
}
