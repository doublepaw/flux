// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Integration tests for the reader group coordinator.
//!
//! These tests require a running PostgreSQL instance.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test coordinator_integration
//! ```

mod common;

use flux_broker::{CommitStatus, Coordinator, CoordinatorConfig, HeartbeatStatus, PollStatus};
use flux_common::ids::{Offset, TopicId};
use std::sync::Arc;
use std::time::Duration;

use common::TestDb;

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

/// Test single reader joins.
#[tokio::test]
async fn test_single_consumer_join() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-1").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");
}

/// Test two consumers join.
#[tokio::test]
async fn test_two_consumers_join() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-2").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Reader A joins first
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");

    // Reader B joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-b")
        .await
        .expect("join_group failed");

    // A sends heartbeat - should be Ok (still a member)
    let hb_result = coordinator
        .heartbeat("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("heartbeat failed");

    assert_eq!(hb_result, HeartbeatStatus::Ok);
}

/// Test leave group.
#[tokio::test]
async fn test_leave_group() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-4").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Reader A joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");

    // Reader B joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-b")
        .await
        .expect("join_group failed");

    // A leaves
    coordinator
        .leave_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("leave_group failed");

    // B's heartbeat should succeed (still a member)
    let hb_result = coordinator
        .heartbeat("test-group", TopicId(topic_id as u32), "reader-b")
        .await
        .expect("heartbeat failed");

    assert_eq!(hb_result, HeartbeatStatus::Ok);
}

/// Test commit_range fails when reader doesn't own the range.
#[tokio::test]
async fn test_commit_range_not_owner() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-6").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Reader A joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");

    // Reader B tries to commit (never joined, no inflight range)
    let status = coordinator
        .commit_range(
            "test-group",
            TopicId(topic_id as u32),
            "reader-b",
            Offset(0),
            Offset(100),
        )
        .await
        .expect("commit_range failed");

    assert_eq!(status, CommitStatus::NotOwner);
}

/// Test heartbeat for unknown member.
#[tokio::test]
async fn test_heartbeat_unknown_member() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-7").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Reader A joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");

    // Unknown reader sends heartbeat
    let result = coordinator
        .heartbeat("test-group", TopicId(topic_id as u32), "unknown-reader")
        .await
        .expect("heartbeat failed");

    assert_eq!(result, HeartbeatStatus::UnknownMember);
}

/// Test heartbeat detects and removes expired members.
#[tokio::test]
async fn test_heartbeat_removes_expired_members() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-9").await;

    // Use very short session timeout for testing
    let config = CoordinatorConfig {
        session_timeout: Duration::from_millis(100),
        lease_duration: Duration::from_secs(45),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Reader A joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-a")
        .await
        .expect("join_group failed");

    // Reader B joins
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "reader-b")
        .await
        .expect("join_group failed");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B heartbeats - should detect A expired
    let result = coordinator
        .heartbeat("test-group", TopicId(topic_id as u32), "reader-b")
        .await
        .expect("heartbeat failed");

    // B's heartbeat should still succeed (B is still a member)
    assert_eq!(result, HeartbeatStatus::Ok);
}

/// Test multiple consumers join.
#[tokio::test]
async fn test_multiple_consumers_join() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-10").await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Three consumers join
    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "a")
        .await
        .expect("join failed");

    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "b")
        .await
        .expect("join failed");

    coordinator
        .join_group("test-group", TopicId(topic_id as u32), "c")
        .await
        .expect("join failed");

    // All members can heartbeat
    for reader_id in &["a", "b", "c"] {
        let hb = coordinator
            .heartbeat("test-group", TopicId(topic_id as u32), reader_id)
            .await
            .expect("heartbeat failed");
        assert_eq!(hb, HeartbeatStatus::Ok);
    }
}

// ============ New-Model Tests ============

/// Two concurrent polls on the same group must get non-overlapping ranges.
#[tokio::test]
async fn test_poll_dispatch_exclusivity() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("poll-excl").await as u32);
    insert_test_batches(&db.pool, topic_id, 30).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    // Two readers join
    coordinator
        .join_group("pe-group", topic_id, "reader-a")
        .await
        .expect("join a");
    coordinator
        .join_group("pe-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // Both poll concurrently
    let coord_a = coordinator.clone();
    let coord_b = coordinator.clone();
    let (poll_a, poll_b) = tokio::join!(
        coord_a.poll("pe-group", topic_id, "reader-a", 1024 * 1024),
        coord_b.poll("pe-group", topic_id, "reader-b", 1024 * 1024),
    );

    let a = poll_a.expect("poll a");
    let b = poll_b.expect("poll b");

    // At least one should get work
    let a_has_work = a.start_offset != a.end_offset;
    let b_has_work = b.start_offset != b.end_offset;
    assert!(
        a_has_work || b_has_work,
        "at least one reader should get work"
    );

    // If both got work, ranges must NOT overlap
    if a_has_work && b_has_work {
        assert!(
            a.end_offset.0 <= b.start_offset.0 || b.end_offset.0 <= a.start_offset.0,
            "ranges must not overlap: A=[{}, {}), B=[{}, {})",
            a.start_offset.0,
            a.end_offset.0,
            b.start_offset.0,
            b.end_offset.0,
        );
    }
}

/// Expired inflight lease allows re-dispatch to another reader.
#[tokio::test]
async fn test_inflight_lease_expiry_allows_redispatch() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("lease-expiry").await as u32);
    insert_test_batches(&db.pool, topic_id, 10).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Reader-A joins and polls
    coordinator
        .join_group("le-group", topic_id, "reader-a")
        .await
        .expect("join");
    let poll_a = coordinator
        .poll("le-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(
        poll_a.start_offset != poll_a.end_offset,
        "reader-a should get work"
    );

    // Manually expire the lease
    sqlx::query(
        "UPDATE reader_inflight SET lease_expires_at = NOW() - INTERVAL '1 second' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("le-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("expire lease");

    // Delete the expired inflight (simulates coordinator cleanup)
    sqlx::query(
        "DELETE FROM reader_inflight WHERE group_id = $1 AND topic_id = $2 AND lease_expires_at < NOW()",
    )
    .bind("le-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("cleanup expired");

    // Reset dispatch cursor so the range is re-dispatchable
    sqlx::query(
        "UPDATE reader_group_state SET dispatch_cursor = $3 WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("le-group")
    .bind(topic_id.0 as i32)
    .bind(poll_a.start_offset.0 as i64)
    .execute(&db.pool)
    .await
    .expect("reset cursor");

    // Reader-B joins and polls — should get the re-dispatched range
    coordinator
        .join_group("le-group", topic_id, "reader-b")
        .await
        .expect("join b");
    let poll_b = coordinator
        .poll("le-group", topic_id, "reader-b", 1024 * 1024)
        .await
        .expect("poll b");

    assert!(
        poll_b.start_offset.0 <= poll_a.start_offset.0
            && poll_b.end_offset.0 >= poll_a.start_offset.0,
        "reader-b should get re-dispatched range covering [{}, {}), got [{}, {})",
        poll_a.start_offset.0,
        poll_a.end_offset.0,
        poll_b.start_offset.0,
        poll_b.end_offset.0,
    );
}

/// Watermark advances only when all contiguous inflight ranges are committed.
#[tokio::test]
async fn test_watermark_advancement_ordering() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("wm-order").await as u32);
    insert_test_batches(&db.pool, topic_id, 30).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Single reader polls 3 times with small max_bytes to get 3 non-overlapping ranges.
    // Each batch is 1024 bytes, so max_bytes=1024 dispatches exactly one batch per poll.
    coordinator
        .join_group("wm-group", topic_id, "reader-a")
        .await
        .expect("join");

    let poll1 = coordinator
        .poll("wm-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 1");
    let poll2 = coordinator
        .poll("wm-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 2");
    let poll3 = coordinator
        .poll("wm-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 3");

    // All should have gotten work
    assert!(
        poll1.start_offset != poll1.end_offset,
        "poll1 should get work"
    );
    assert!(
        poll2.start_offset != poll2.end_offset,
        "poll2 should get work"
    );
    assert!(
        poll3.start_offset != poll3.end_offset,
        "poll3 should get work"
    );

    let get_watermark = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("wm-group")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("query watermark")
    };

    // Commit middle range first — watermark should NOT advance
    coordinator
        .commit_range(
            "wm-group",
            topic_id,
            "reader-a",
            poll2.start_offset,
            poll2.end_offset,
        )
        .await
        .expect("commit 2");
    assert_eq!(
        get_watermark().await,
        poll1.start_offset.0 as i64,
        "watermark should stay at poll1.start after committing middle range"
    );

    // Commit last range — watermark still blocked by first
    coordinator
        .commit_range(
            "wm-group",
            topic_id,
            "reader-a",
            poll3.start_offset,
            poll3.end_offset,
        )
        .await
        .expect("commit 3");
    assert_eq!(
        get_watermark().await,
        poll1.start_offset.0 as i64,
        "watermark should stay at poll1.start after committing last range"
    );

    // Commit first range — watermark should jump to end of all committed
    coordinator
        .commit_range(
            "wm-group",
            topic_id,
            "reader-a",
            poll1.start_offset,
            poll1.end_offset,
        )
        .await
        .expect("commit 1");
    assert_eq!(
        get_watermark().await,
        poll3.end_offset.0 as i64,
        "watermark should advance to end after all ranges committed"
    );
}

/// Happy path: poll -> commit -> verify DB state is clean.
#[tokio::test]
async fn test_commit_range_happy_path() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("commit-happy").await as u32);
    insert_test_batches(&db.pool, topic_id, 10).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("ch-group", topic_id, "reader-a")
        .await
        .expect("join");

    let poll = coordinator
        .poll("ch-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");

    assert!(poll.start_offset != poll.end_offset, "should get work");

    // Commit
    let status = coordinator
        .commit_range(
            "ch-group",
            topic_id,
            "reader-a",
            poll.start_offset,
            poll.end_offset,
        )
        .await
        .expect("commit");
    assert_eq!(status, CommitStatus::Ok);

    // Verify: inflight row deleted
    let inflight_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ch-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query inflight");
    assert_eq!(inflight_count, 0, "inflight row should be deleted");

    // Verify: committed_watermark advanced
    let watermark: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ch-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark");
    assert_eq!(
        watermark, poll.end_offset.0 as i64,
        "committed_watermark should equal end_offset"
    );

    // Verify: dispatch_cursor advanced
    let cursor: i64 = sqlx::query_scalar(
        "SELECT dispatch_cursor FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ch-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query cursor");
    assert_eq!(
        cursor, poll.end_offset.0 as i64,
        "dispatch_cursor should equal end_offset"
    );
}

// ============ Pipelined Polling Tests ============

/// Max inflight enforcement: polling max_inflight+1 times returns MaxInflight.
#[tokio::test]
async fn test_max_inflight_enforcement() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("max-inflight").await as u32);
    insert_test_batches(&db.pool, topic_id, 200).await;

    let max_inflight = 3u32;
    let config = CoordinatorConfig {
        max_inflight_per_reader: max_inflight,
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("mi-group", topic_id, "reader-a")
        .await
        .expect("join");

    // Poll max_inflight times — all should succeed
    for i in 0..max_inflight {
        let poll = coordinator
            .poll("mi-group", topic_id, "reader-a", 1024)
            .await
            .unwrap_or_else(|e| panic!("poll {} should succeed: {}", i, e));
        assert_eq!(poll.status, PollStatus::Ok, "poll {} should be Ok", i);
        assert!(
            poll.start_offset != poll.end_offset,
            "poll {} should get work",
            i
        );
    }

    // One more poll should return MaxInflight
    let poll = coordinator
        .poll("mi-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll should not error");
    assert_eq!(
        poll.status,
        PollStatus::MaxInflight,
        "poll beyond max_inflight should be rejected"
    );
}

/// Expired lease range is stolen by another reader via poll.
#[tokio::test]
async fn test_expired_range_stolen_by_another_reader() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("steal-expired").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Reader A joins and polls
    coordinator
        .join_group("se-group", topic_id, "reader-a")
        .await
        .expect("join a");
    coordinator
        .join_group("se-group", topic_id, "reader-b")
        .await
        .expect("join b");

    let poll_a = coordinator
        .poll("se-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(
        poll_a.start_offset != poll_a.end_offset,
        "reader-a should get work"
    );

    // Manually expire A's lease
    sqlx::query(
        "UPDATE reader_inflight SET lease_expires_at = NOW() - INTERVAL '1 second' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("se-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("expire lease");

    // Reader B polls — should steal A's expired range
    let poll_b = coordinator
        .poll("se-group", topic_id, "reader-b", 1024 * 1024)
        .await
        .expect("poll b");

    assert_eq!(poll_b.status, PollStatus::Ok);
    assert_eq!(
        poll_b.start_offset, poll_a.start_offset,
        "reader-b should steal A's expired range start"
    );
    assert_eq!(
        poll_b.end_offset, poll_a.end_offset,
        "reader-b should steal A's expired range end"
    );
}

/// Two readers each pipeline 2 polls; all 4 ranges are non-overlapping.
#[tokio::test]
async fn test_pipelined_poll_different_readers() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pipeline-2x2").await as u32);
    insert_test_batches(&db.pool, topic_id, 100).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("pp-group", topic_id, "reader-a")
        .await
        .expect("join a");
    coordinator
        .join_group("pp-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // Each reader polls twice (pipeline)
    let a1 = coordinator
        .poll("pp-group", topic_id, "reader-a", 1024)
        .await
        .expect("a1");
    let a2 = coordinator
        .poll("pp-group", topic_id, "reader-a", 1024)
        .await
        .expect("a2");
    let b1 = coordinator
        .poll("pp-group", topic_id, "reader-b", 1024)
        .await
        .expect("b1");
    let b2 = coordinator
        .poll("pp-group", topic_id, "reader-b", 1024)
        .await
        .expect("b2");

    let ranges: Vec<(u64, u64)> = [&a1, &a2, &b1, &b2]
        .iter()
        .filter(|p| p.start_offset != p.end_offset)
        .map(|p| (p.start_offset.0, p.end_offset.0))
        .collect();

    // All non-empty ranges must be non-overlapping
    for (i, r1) in ranges.iter().enumerate() {
        for r2 in ranges.iter().skip(i + 1) {
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
}

/// Empty poll (no data) should not insert inflight rows.
#[tokio::test]
async fn test_empty_poll_no_inflight_row() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("empty-poll").await as u32);
    // No data inserted — topic is empty

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("ep-group", topic_id, "reader-a")
        .await
        .expect("join");

    // Poll with no data available
    let poll = coordinator
        .poll("ep-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");

    // Should return empty range
    assert_eq!(
        poll.start_offset, poll.end_offset,
        "empty topic should return start == end"
    );

    // Verify no inflight rows
    let inflight_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ep-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query inflight");
    assert_eq!(
        inflight_count, 0,
        "empty poll should not create inflight rows"
    );

    // Poll again — still no inflight rows
    let poll2 = coordinator
        .poll("ep-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll 2");
    assert_eq!(poll2.start_offset, poll2.end_offset);

    let inflight_count2: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ep-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query inflight 2");
    assert_eq!(
        inflight_count2, 0,
        "second empty poll should still have no inflight rows"
    );
}

/// Verify lease_deadline_ms is non-zero and approximately correct.
#[tokio::test]
async fn test_lease_deadline_ms_populated() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("lease-ms").await as u32);
    insert_test_batches(&db.pool, topic_id, 10).await;

    let lease_secs = 30u64;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(lease_secs),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    coordinator
        .join_group("lm-group", topic_id, "reader-a")
        .await
        .expect("join");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let poll = coordinator
        .poll("lm-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");

    assert!(poll.start_offset != poll.end_offset, "should get work");
    assert!(
        poll.lease_deadline_ms > 0,
        "lease_deadline_ms should be non-zero"
    );

    // Should be roughly now + lease_duration (within 5 second tolerance)
    let expected_min = now_ms + (lease_secs - 5) * 1000;
    let expected_max = now_ms + (lease_secs + 5) * 1000;
    assert!(
        poll.lease_deadline_ms >= expected_min && poll.lease_deadline_ms <= expected_max,
        "lease_deadline_ms {} should be near now + {}s (expected [{}, {}])",
        poll.lease_deadline_ms,
        lease_secs,
        expected_min,
        expected_max,
    );
}

// ============ Boundary-Condition Tests ============

/// Partial range commit is rejected: poll [0, N), try commit [0, N/2).
#[tokio::test]
async fn test_partial_range_commit_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("partial-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("pc-group", topic_id, "reader-a")
        .await
        .expect("join");

    let poll = coordinator
        .poll("pc-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");
    assert!(poll.start_offset != poll.end_offset);

    let mid = Offset(poll.start_offset.0 + (poll.end_offset.0 - poll.start_offset.0) / 2);
    let status = coordinator
        .commit_range("pc-group", topic_id, "reader-a", poll.start_offset, mid)
        .await
        .expect("commit partial");
    assert_eq!(status, CommitStatus::NotOwner);
}

/// Superset range commit is rejected: poll [0, N), try commit [0, N+50).
#[tokio::test]
async fn test_superset_range_commit_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("superset-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("sc-group", topic_id, "reader-a")
        .await
        .expect("join");

    let poll = coordinator
        .poll("sc-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");
    assert!(poll.start_offset != poll.end_offset);

    let status = coordinator
        .commit_range(
            "sc-group",
            topic_id,
            "reader-a",
            poll.start_offset,
            Offset(poll.end_offset.0 + 50),
        )
        .await
        .expect("commit superset");
    assert_eq!(status, CommitStatus::NotOwner);
}

/// Wrong range commit is rejected: poll [0, N), try commit [200, 300).
#[tokio::test]
async fn test_wrong_range_commit_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("wrong-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("wc-group", topic_id, "reader-a")
        .await
        .expect("join");

    let poll = coordinator
        .poll("wc-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll");
    assert!(poll.start_offset != poll.end_offset);

    let status = coordinator
        .commit_range("wc-group", topic_id, "reader-a", Offset(200), Offset(300))
        .await
        .expect("commit wrong");
    assert_eq!(status, CommitStatus::NotOwner);
}

/// Commit after lease steal: A polls, lease expires, B steals, A commits → NotOwner.
#[tokio::test]
async fn test_commit_after_lease_steal_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("steal-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("ls-group", topic_id, "reader-a")
        .await
        .expect("join a");
    coordinator
        .join_group("ls-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // A polls
    let poll_a = coordinator
        .poll("ls-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(poll_a.start_offset != poll_a.end_offset);

    // Expire A's lease
    sqlx::query(
        "UPDATE reader_inflight SET lease_expires_at = NOW() - INTERVAL '1 second' \
         WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3",
    )
    .bind("ls-group")
    .bind(topic_id.0 as i32)
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("expire lease");

    // B polls — steals A's range
    let poll_b = coordinator
        .poll("ls-group", topic_id, "reader-b", 1024 * 1024)
        .await
        .expect("poll b");
    assert_eq!(poll_b.start_offset, poll_a.start_offset);

    // A tries to commit the stolen range — should be NotOwner
    let status_a = coordinator
        .commit_range(
            "ls-group",
            topic_id,
            "reader-a",
            poll_a.start_offset,
            poll_a.end_offset,
        )
        .await
        .expect("commit a");
    assert_eq!(status_a, CommitStatus::NotOwner);

    // B can still commit
    let status_b = coordinator
        .commit_range(
            "ls-group",
            topic_id,
            "reader-b",
            poll_b.start_offset,
            poll_b.end_offset,
        )
        .await
        .expect("commit b");
    assert_eq!(status_b, CommitStatus::Ok);
}

/// Poll with all data dispatched returns empty (start == end), inflight unchanged.
#[tokio::test]
async fn test_poll_empty_when_all_dispatched() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("all-dispatched").await as u32);
    insert_test_batches(&db.pool, topic_id, 10).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("ad-group", topic_id, "reader-a")
        .await
        .expect("join");

    // Poll all data
    let poll1 = coordinator
        .poll("ad-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll 1");
    assert!(poll1.start_offset != poll1.end_offset);

    let inflight_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ad-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("count inflight");

    // Poll again — no more data
    let poll2 = coordinator
        .poll("ad-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll 2");
    assert_eq!(poll2.start_offset, poll2.end_offset, "should return empty");

    let inflight_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_inflight WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("ad-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("count inflight after");

    assert_eq!(
        inflight_before, inflight_after,
        "inflight count should be unchanged"
    );
}

/// Watermark advances to dispatch_cursor when a reader leaves with inflight ranges.
#[tokio::test]
async fn test_watermark_advances_on_leave() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("wm-leave").await as u32);
    insert_test_batches(&db.pool, topic_id, 30).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());
    coordinator
        .join_group("wl-group", topic_id, "reader-a")
        .await
        .expect("join");

    // Poll twice to get 2 inflight ranges
    let poll1 = coordinator
        .poll("wl-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 1");
    let poll2 = coordinator
        .poll("wl-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 2");
    assert!(poll1.start_offset != poll1.end_offset);
    assert!(poll2.start_offset != poll2.end_offset);

    // Commit the first range
    coordinator
        .commit_range(
            "wl-group",
            topic_id,
            "reader-a",
            poll1.start_offset,
            poll1.end_offset,
        )
        .await
        .expect("commit 1");

    // Leave — should clear remaining inflight and advance watermark
    coordinator
        .leave_group("wl-group", topic_id, "reader-a")
        .await
        .expect("leave");

    let watermark: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("wl-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark");

    let dispatch_cursor: i64 = sqlx::query_scalar(
        "SELECT dispatch_cursor FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("wl-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query cursor");

    assert_eq!(
        watermark, dispatch_cursor,
        "watermark should advance to dispatch_cursor after leave"
    );
}

/// 200 records, 5 concurrent readers poll — ranges non-overlapping, full coverage.
#[tokio::test]
async fn test_dispatch_serialization_under_load() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("dispatch-load").await as u32);
    insert_test_batches(&db.pool, topic_id, 200).await;

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    // 5 readers join
    for i in 0..5 {
        coordinator
            .join_group("dl-group", topic_id, &format!("reader-{}", i))
            .await
            .expect("join");
    }

    // Each reader polls repeatedly until empty
    let mut handles = vec![];
    for i in 0..5 {
        let coord = coordinator.clone();
        let reader_id = format!("reader-{}", i);
        handles.push(tokio::spawn(async move {
            let mut ranges = vec![];
            loop {
                let poll = coord
                    .poll("dl-group", topic_id, &reader_id, 1024)
                    .await
                    .expect("poll");
                if poll.start_offset == poll.end_offset {
                    break;
                }
                ranges.push((poll.start_offset.0, poll.end_offset.0));
                if poll.status == PollStatus::MaxInflight {
                    break;
                }
            }
            ranges
        }));
    }

    let mut all_ranges: Vec<(u64, u64)> = vec![];
    for handle in handles {
        all_ranges.extend(handle.await.expect("join handle"));
    }

    // Sort by start offset
    all_ranges.sort_by_key(|r| r.0);

    // Verify non-overlapping
    for window in all_ranges.windows(2) {
        assert!(
            window[0].1 <= window[1].0,
            "ranges overlap: [{}, {}) and [{}, {})",
            window[0].0,
            window[0].1,
            window[1].0,
            window[1].1,
        );
    }

    // Verify full coverage: first range starts at 0, last range ends at 200
    if !all_ranges.is_empty() {
        assert_eq!(
            all_ranges.first().unwrap().0,
            0,
            "first range should start at 0"
        );
        assert_eq!(
            all_ranges.last().unwrap().1,
            200,
            "last range should end at 200"
        );
    }
}

/// Evicted reader's commit is rejected (inflight rows deleted on heartbeat eviction).
#[tokio::test]
async fn test_evicted_reader_commit_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("evicted-commit").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let config = CoordinatorConfig {
        session_timeout: Duration::from_millis(100),
        lease_duration: Duration::from_secs(45),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // A joins and polls
    coordinator
        .join_group("ec-group", topic_id, "reader-a")
        .await
        .expect("join a");
    let poll = coordinator
        .poll("ec-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(poll.start_offset != poll.end_offset);

    // B joins
    coordinator
        .join_group("ec-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B's heartbeat evicts A (cleans up A's inflight rows)
    let hb = coordinator
        .heartbeat("ec-group", topic_id, "reader-b")
        .await
        .expect("heartbeat b");
    assert_eq!(hb, HeartbeatStatus::Ok);

    // A tries to commit — should be NotOwner (inflight rows deleted)
    let status = coordinator
        .commit_range(
            "ec-group",
            topic_id,
            "reader-a",
            poll.start_offset,
            poll.end_offset,
        )
        .await
        .expect("commit a");
    assert_eq!(status, CommitStatus::NotOwner);
}

/// Heartbeat eviction must roll back dispatch_cursor so expired ranges are re-dispatched.
///
/// Bug: heartbeat deleted expired members' inflight rows without rolling back
/// dispatch_cursor. The watermark then jumped to dispatch_cursor via
/// `COALESCE(MIN(remaining), dispatch_cursor)`, silently skipping uncommitted data.
#[tokio::test]
async fn test_heartbeat_eviction_redispatches_expired_ranges() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("evict-redispatch").await as u32);
    insert_test_batches(&db.pool, topic_id, 20).await;

    let config = CoordinatorConfig {
        session_timeout: Duration::from_millis(100),
        lease_duration: Duration::from_secs(45),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // A joins and polls — gets a range
    coordinator
        .join_group("er-group", topic_id, "reader-a")
        .await
        .expect("join a");
    let poll_a = coordinator
        .poll("er-group", topic_id, "reader-a", 1024 * 1024)
        .await
        .expect("poll a");
    assert!(
        poll_a.start_offset != poll_a.end_offset,
        "reader-a should get work"
    );

    // B joins
    coordinator
        .join_group("er-group", topic_id, "reader-b")
        .await
        .expect("join b");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B's heartbeat evicts A (deletes A's inflight rows)
    let hb = coordinator
        .heartbeat("er-group", topic_id, "reader-b")
        .await
        .expect("heartbeat b");
    assert_eq!(hb, HeartbeatStatus::Ok);

    // dispatch_cursor must be rolled back so evicted range is re-dispatchable
    let cursor: i64 = sqlx::query_scalar(
        "SELECT dispatch_cursor FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("er-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query cursor");

    assert!(
        cursor <= poll_a.start_offset.0 as i64,
        "dispatch_cursor ({}) should be rolled back to at most {} after eviction",
        cursor,
        poll_a.start_offset.0,
    );

    // B polls — should get the re-dispatched range
    let poll_b = coordinator
        .poll("er-group", topic_id, "reader-b", 1024 * 1024)
        .await
        .expect("poll b");

    assert!(
        poll_b.start_offset.0 <= poll_a.start_offset.0,
        "reader-b should get re-dispatched range starting at or before A's range ({} vs {})",
        poll_b.start_offset.0,
        poll_a.start_offset.0,
    );
    assert!(
        poll_b.end_offset.0 > poll_a.start_offset.0,
        "reader-b should cover A's evicted data (end {} > start {})",
        poll_b.end_offset.0,
        poll_a.start_offset.0,
    );
}

/// force_reset must not skip uncommitted data.
///
/// Bug: force_reset set `committed_watermark = dispatch_cursor` after deleting
/// all inflight rows, silently acknowledging uncommitted ranges. The cursor should
/// be rolled back to committed_watermark so those ranges get re-dispatched.
#[tokio::test]
async fn test_force_reset_does_not_skip_uncommitted_data() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("force-rebal").await as u32);
    insert_test_batches(&db.pool, topic_id, 30).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    coordinator
        .join_group("fr-group", topic_id, "reader-a")
        .await
        .expect("join a");

    // Poll twice to get two ranges
    let poll1 = coordinator
        .poll("fr-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 1");
    let poll2 = coordinator
        .poll("fr-group", topic_id, "reader-a", 1024)
        .await
        .expect("poll 2");
    assert!(
        poll1.start_offset != poll1.end_offset,
        "poll1 should get work"
    );
    assert!(
        poll2.start_offset != poll2.end_offset,
        "poll2 should get work"
    );

    // Commit only the first range
    let status = coordinator
        .commit_range(
            "fr-group",
            topic_id,
            "reader-a",
            poll1.start_offset,
            poll1.end_offset,
        )
        .await
        .expect("commit 1");
    assert_eq!(status, CommitStatus::Ok);

    let watermark_before: i64 = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("fr-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark before");

    // force_reset — evicts everyone, clears inflight
    coordinator
        .force_reset("fr-group", topic_id)
        .await
        .expect("force_reset");

    let (cursor_after, watermark_after): (i64, i64) = sqlx::query_as(
        "SELECT dispatch_cursor, committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("fr-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query state after");

    // Watermark must NOT advance past what was actually committed
    assert_eq!(
        watermark_after, watermark_before,
        "committed_watermark ({}) should stay at {} (not jump to dispatch_cursor)",
        watermark_after, watermark_before,
    );

    // dispatch_cursor must be rolled back to committed_watermark
    assert_eq!(
        cursor_after, watermark_before,
        "dispatch_cursor ({}) should be rolled back to committed_watermark ({})",
        cursor_after, watermark_before,
    );

    // New reader joins and polls — should get the uncommitted range
    coordinator
        .join_group("fr-group", topic_id, "reader-b")
        .await
        .expect("join b");
    let poll_b = coordinator
        .poll("fr-group", topic_id, "reader-b", 1024 * 1024)
        .await
        .expect("poll b");

    assert!(
        poll_b.start_offset.0 <= poll2.start_offset.0,
        "reader-b should get data at or before poll2's start ({} <= {})",
        poll_b.start_offset.0,
        poll2.start_offset.0,
    );
    assert!(
        poll_b.end_offset.0 > poll2.start_offset.0,
        "reader-b should cover poll2's uncommitted data (end {} > start {})",
        poll_b.end_offset.0,
        poll2.start_offset.0,
    );
}
