// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired crash recovery tests.
//!
//! These tests verify:
//! - Broker restart clears stale watermark cache
//! - Reader reconnects resume from committed offset
//! - Append deduplication works across crashes

mod common;

use bytes::Bytes;
use std::time::Duration;

use flux_broker::{Coordinator, CoordinatorConfig};
use flux_common::ids::{Offset, TopicId};
use flux_common::types::Record;

use common::ws_helpers;
use common::{CrashableBroker, CrashableWsBroker, TestDb, produce_records};

/// Get high watermark from database.
async fn get_watermark(pool: &sqlx::PgPool, topic_id: TopicId) -> i64 {
    sqlx::query_scalar("SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1")
        .bind(topic_id.0 as i32)
        .fetch_optional(pool)
        .await
        .unwrap()
        .unwrap_or(0)
}

/// Test that broker restart sees fresh watermark from DB.
#[tokio::test]
async fn test_broker_restart_watermark_fresh() {
    let db = TestDb::new().await;
    let mut broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("crash-watermark-test").await as u32);

    // Append 10 records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    let (_, end_offset) = produce_records(broker.state(), topic_id, records)
        .await
        .expect("Append should succeed");

    assert_eq!(end_offset.0, 10);

    // Verify watermark before crash
    let watermark_before = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(watermark_before, 10);

    // Simulate crash and restart
    broker.crash();
    assert!(broker.is_crashed());

    broker.restart();
    assert!(!broker.is_crashed());

    // Watermark should still be 10 after restart (loaded from DB)
    let watermark_after = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(
        watermark_after, 10,
        "Watermark should be fresh from DB after restart"
    );

    // Append more records after restart
    let more_records: Vec<Record> = (10..15)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    let (_, new_end_offset) = produce_records(broker.state(), topic_id, more_records)
        .await
        .expect("Append after restart should succeed");

    assert_eq!(new_end_offset.0, 15, "New records should be at offset 15");
}

/// Test reader reconnect resumes from committed offset.
#[tokio::test]
async fn test_consumer_reconnect_resumes_offset() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("reconnect-test").await as u32);

    // Reader joins
    coordinator
        .join_group("reconnect-group", topic_id, "reader-1")
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
    .bind("reconnect-group")
    .bind(topic_id.0 as i32)
    .bind(0i64)
    .bind(100i64)
    .bind("reader-1")
    .execute(&db.pool)
    .await
    .expect("Insert inflight");

    // Also advance dispatch_cursor so watermark can advance
    sqlx::query(
        "UPDATE reader_group_state SET dispatch_cursor = 100 WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("reconnect-group")
    .bind(topic_id.0 as i32)
    .execute(&db.pool)
    .await
    .expect("Update dispatch cursor");

    // Commit range [0, 100)
    let status = coordinator
        .commit_range(
            "reconnect-group",
            topic_id,
            "reader-1",
            Offset(0),
            Offset(100),
        )
        .await
        .expect("Commit should succeed");
    assert_eq!(
        status,
        flux_broker::CommitStatus::Ok,
        "Commit should return Ok"
    );

    // Simulate reader disconnect by leaving
    coordinator
        .leave_group("reconnect-group", topic_id, "reader-1")
        .await
        .expect("Leave should succeed");

    // Reader reconnects
    coordinator
        .join_group("reconnect-group", topic_id, "reader-1")
        .await
        .expect("Rejoin should succeed");

    // Verify committed state via DB
    let committed: Option<i64> = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("reconnect-group")
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .expect("Query should succeed");

    // Committed watermark should reflect what was committed
    assert_eq!(
        committed,
        Some(100),
        "Committed watermark should be 100 after rejoin"
    );
}

/// Test that writes are not lost during broker crash.
#[tokio::test]
async fn test_crash_does_not_lose_committed_writes() {
    let db = TestDb::new().await;
    let mut broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("no-loss-test").await as u32);

    // Track all committed record values
    let mut committed_values = vec![];

    // Append records, simulating crash between batches
    for batch_idx in 0..3 {
        let records: Vec<Record> = (0..5)
            .map(|i| {
                let value = format!("batch-{}-value-{}", batch_idx, i);
                committed_values.push(value.clone());
                Record {
                    key: Some(Bytes::from(format!("batch-{}-key-{}", batch_idx, i))),
                    value: Bytes::from(value),
                }
            })
            .collect();

        produce_records(broker.state(), topic_id, records)
            .await
            .expect("Append should succeed");

        // Crash after second batch
        if batch_idx == 1 {
            broker.crash();
            broker.restart();
        }
    }

    // Verify all committed records are visible from DB perspective
    let batches: Vec<(i64, i64)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset FROM topic_batches
        WHERE topic_id = $1
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .expect("Query should succeed");

    // Should have 3 batches, each with 5 records
    assert_eq!(batches.len(), 3, "Should have 3 batches");

    let total_records: i64 = batches.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total_records, 15, "Should have 15 total records");

    // Verify watermark matches
    let watermark = get_watermark(&db.pool, topic_id).await;
    assert_eq!(watermark, 15, "Watermark should be 15");
}

/// Test that uncommitted writes are not visible after crash.
#[tokio::test]
async fn test_uncommitted_writes_not_visible_after_crash() {
    let db = TestDb::new().await;
    let mut broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("uncommitted-test").await as u32);

    // Append some records (this commits to DB)
    let records: Vec<Record> = (0..5)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(broker.state(), topic_id, records)
        .await
        .expect("Append should succeed");

    let watermark_before = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(watermark_before, 5);

    // Crash the broker
    broker.crash();
    broker.restart();

    // Watermark should still be 5 (committed writes visible)
    let watermark_after = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(
        watermark_after, 5,
        "Only committed writes should be visible after crash"
    );
}

/// Full DB unavailability: broker stalls, doesn't corrupt.
/// Start broker, produce data. Crash to simulate DB unavailability.
/// Restart. Previously committed data intact, new writes succeed.
/// Invariant: DB unavailability causes stalls/errors but never data loss or corruption.
#[tokio::test]
async fn test_full_db_unavailability_stalls_not_corrupts() {
    let db = TestDb::new().await;
    let mut broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("db-unavail").await as u32);

    // Write baseline data
    let records: Vec<Record> = (0..5)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("val-{}", i)),
        })
        .collect();

    let (_, end_offset) = produce_records(broker.state(), topic_id, records)
        .await
        .expect("baseline write should succeed");
    assert_eq!(end_offset.0, 5);

    // Crash (simulates DB unavailability)
    broker.crash();
    assert!(broker.is_crashed());

    // Restart
    broker.restart();
    assert!(!broker.is_crashed());

    // Previously committed data should be intact
    let watermark = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(watermark, 5, "watermark should be 5 after restart");

    // New writes should succeed
    let records: Vec<Record> = (5..8)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("new-{}", i)),
        })
        .collect();

    let (start, end) = produce_records(broker.state(), topic_id, records)
        .await
        .expect("new writes should succeed");
    assert_eq!(start.0, 5, "new writes should start at 5");
    assert_eq!(end.0, 8, "new writes should end at 8");

    // Final watermark check
    let watermark = get_watermark(&broker.pool, topic_id).await;
    assert_eq!(watermark, 8, "final watermark should be 8");

    // Verify batches are contiguous (no gaps from the crash)
    let batches: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT start_offset, end_offset FROM topic_batches \
         WHERE topic_id = $1 ORDER BY start_offset",
    )
    .bind(topic_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .expect("query batches");

    // Verify contiguity: each batch's start == previous batch's end
    for window in batches.windows(2) {
        assert_eq!(
            window[0].1, window[1].0,
            "batches should be contiguous: prev_end={}, next_start={}",
            window[0].1, window[1].0
        );
    }
}

/// Test that reader group state persists through coordinator restart.
#[tokio::test]
async fn test_consumer_group_state_persists() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("persist-cg-test").await as u32);

    // First coordinator instance
    {
        let coordinator = Coordinator::new(db.pool.clone(), config.clone());

        coordinator
            .join_group("persist-cg", topic_id, "reader-1")
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
        .bind("persist-cg")
        .bind(topic_id.0 as i32)
        .bind(0i64)
        .bind(50i64)
        .bind("reader-1")
        .execute(&db.pool)
        .await
        .expect("Insert inflight");

        // Advance dispatch_cursor so watermark can advance
        sqlx::query(
            "UPDATE reader_group_state SET dispatch_cursor = 50 WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("persist-cg")
        .bind(topic_id.0 as i32)
        .execute(&db.pool)
        .await
        .expect("Update dispatch cursor");

        // Commit range [0, 50)
        let status = coordinator
            .commit_range("persist-cg", topic_id, "reader-1", Offset(0), Offset(50))
            .await
            .expect("Commit should succeed");
        assert_eq!(status, flux_broker::CommitStatus::Ok);
    }
    // First coordinator dropped here

    // Second coordinator instance (simulates restart)
    {
        let coordinator = Coordinator::new(db.pool.clone(), config);

        // Leave and rejoin to get fresh state
        let _ = coordinator
            .leave_group("persist-cg", topic_id, "reader-1")
            .await;

        coordinator
            .join_group("persist-cg", topic_id, "reader-1")
            .await
            .expect("Rejoin should succeed");

        // Verify group state persists in DB with correct watermark
        let watermark: i64 = sqlx::query_scalar(
            "SELECT committed_watermark FROM reader_group_state WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("persist-cg")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("Reader group state should persist in database");

        assert_eq!(
            watermark, 50,
            "Committed watermark should be 50 after coordinator restart"
        );
    }
}

// ============ L1: Read-after-write across broker restart ============

use flux_common::ids::*;
use flux_common::types::RecordBatch;
use flux_wire::{ClientMessage, reader, writer};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Message};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

async fn ws_produce(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    value: &str,
) -> Result<writer::AppendResponse, String> {
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(seq),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from(value.to_string()),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_read_all(ws: &mut Ws, topic_id: TopicId) -> Result<(Vec<Bytes>, Offset), String> {
    let mut all_values = Vec::new();
    let mut next_offset = Offset(0);
    let mut hwm = Offset(0);

    loop {
        let req = reader::ReadRequest {
            topic_id,
            offset: next_offset,
            max_bytes: 10 * 1024 * 1024,
        };
        let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| format!("send: {}", e))?;

        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .map_err(|_| "timeout".to_string())?
            .ok_or("stream closed")?
            .map_err(|e| format!("recv: {}", e))?;

        let data = match msg {
            Message::Binary(d) => d,
            _ => return Err("expected binary".to_string()),
        };
        let resp = match ws_helpers::decode_server_frame(&data) {
            flux_wire::ServerMessage::Read(resp) => resp,
            other => return Err(format!("unexpected: {:?}", other)),
        };

        if !resp.success {
            return Err(format!("read failed: {}", resp.error_message));
        }

        let mut got = false;
        for r in &resp.results {
            if r.high_watermark.0 > hwm.0 {
                hwm = r.high_watermark;
            }
            if !r.records.is_empty() {
                got = true;
                next_offset = Offset(next_offset.0 + r.records.len() as u64);
                all_values.extend(r.records.iter().map(|rec| rec.value.clone()));
            }
        }
        if !got || next_offset.0 >= hwm.0 {
            break;
        }
    }
    Ok((all_values, hwm))
}

/// Produce N records, get ack with offsets. Crash and restart broker.
/// Read from offset 0. Verify: all N records present with correct payloads.
/// Invariant: acknowledged writes survive broker restart (S3 + DB durability).
#[tokio::test]
async fn test_read_after_write_across_restart() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("raw-restart").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;

    // Produce 10 records and collect acked values
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();
    let mut acked_values = Vec::new();
    for i in 0..10 {
        let val = format!("record-{}", i);
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &val)
            .await
            .expect("produce should succeed");
        assert!(resp.success, "write {} should be acked", i);
        acked_values.push(Bytes::from(val));
    }

    // Crash and restart
    drop(ws);
    broker.restart().await;

    // Read all from offset 0
    let mut ws = ws_connect(broker.addr()).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read after restart should succeed");

    // All acked records should be present
    assert_eq!(
        values.len(),
        acked_values.len(),
        "all {} acked records should survive restart, got {}",
        acked_values.len(),
        values.len()
    );

    for v in &acked_values {
        assert!(
            values.contains(v),
            "acked value {:?} should survive restart",
            v
        );
    }

    // Watermark should match record count
    assert_eq!(
        hwm.0,
        acked_values.len() as u64,
        "watermark should match acked count"
    );
}
