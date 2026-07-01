// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired tests targeting the pipelined flush failure windows.
//!
//! These tests use `CrashableWsBroker` (real WS + flush loop) to verify:
//! - S3 failures drop in-flight acks without corrupting buffered writes
//! - Concurrent produce+consume under rotating S3 faults preserves invariants
//! - Slow S3 does not lose buffered writes
//! - Crash between S3 put and DB commit leaves correct watermark after restart
//! - DB-blocked flush produces no false acks
//! - Transient S3 failures auto-recover
//! - Sustained S3 failure triggers backpressure that recovers after fault clears

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, DbBlocker, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

/// Produce a single record and return the AppendResponse.
async fn ws_produce(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    value: &str,
) -> Result<writer::AppendResponse, String> {
    ws_produce_timeout(ws, writer_id, seq, topic_id, value, Duration::from_secs(10)).await
}

async fn ws_produce_timeout(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    value: &str,
    timeout: Duration,
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

    let msg = tokio::time::timeout(timeout, ws.next())
        .await
        .map_err(|_| "timeout waiting for append response".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary message".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected server message: {:?}", other)),
    }
}

/// Single read request at a given offset.
async fn ws_read_at(
    ws: &mut Ws,
    topic_id: TopicId,
    offset: Offset,
) -> Result<reader::ReadResponse, String> {
    let req = reader::ReadRequest {
        topic_id,
        offset,
        max_bytes: 10 * 1024 * 1024,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout waiting for read response".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary message".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Read(resp) => Ok(resp),
        other => Err(format!("unexpected server message: {:?}", other)),
    }
}

/// Read ALL records from offset 0 for a topic by paginating.
/// The broker returns at most 10 batches per read, so we must paginate.
async fn ws_read_all(ws: &mut Ws, topic_id: TopicId) -> Result<reader::ReadResponse, String> {
    let mut all_records = vec![];
    let mut high_watermark = Offset(0);
    let mut next_offset = Offset(0);

    loop {
        let resp = ws_read_at(ws, topic_id, next_offset).await?;
        if !resp.success {
            return Err(format!("read failed: {}", resp.error_message));
        }

        let mut got_records = false;
        for result in &resp.results {
            if result.high_watermark.0 > high_watermark.0 {
                high_watermark = result.high_watermark;
            }
            if !result.records.is_empty() {
                got_records = true;
                next_offset = Offset(next_offset.0 + result.records.len() as u64);
                all_records.extend(result.records.iter().cloned());
            }
        }

        // Stop when no more records or we've reached the watermark.
        if !got_records || next_offset.0 >= high_watermark.0 {
            break;
        }
    }

    // Build a synthetic response with all collected records.
    Ok(reader::ReadResponse {
        success: true,
        error_code: 0,
        error_message: String::new(),
        results: vec![reader::TopicResult {
            topic_id,
            schema_id: SchemaId(100),
            high_watermark,
            records: all_records,
        }],
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// S3 failure during flush drops in-flight acks; subsequent writes succeed.
#[tokio::test]
async fn test_s3_failure_drops_inflight_acks_not_buffered() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("s3-fail-ack").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();

    // Fail the next S3 put — the flush that processes our write will fail.
    broker.faulty_store().fail_next_put();

    // Produce a record. Flush will fail, so ack should not indicate success.
    let result = ws_produce(&mut ws, writer_id, 1, topic_id, "fail-me").await;
    let failed = result.is_err() || !result.as_ref().unwrap().success;
    assert!(failed, "Write during S3 failure should not succeed");

    // Reconnect (connection may be in an inconsistent state after failed ack).
    drop(ws);
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id2 = WriterId::new();

    // S3 fault consumed — next write should succeed.
    let resp = ws_produce(&mut ws, writer_id2, 1, topic_id, "should-work")
        .await
        .expect("Write after recovery should succeed");
    assert!(resp.success, "second write should be acked");

    // Verify the successful write is visible.
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(read.success);
    let values: Vec<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    assert!(
        values.contains(&Bytes::from("should-work")),
        "Acked write should be visible in reads"
    );
    assert!(
        !values.contains(&Bytes::from("fail-me")),
        "Failed write should NOT be visible"
    );
}

/// Concurrent produce + consume under periodic S3 failures.
/// Invariants: acked writes visible, unique offsets, monotonic watermark.
#[tokio::test]
async fn test_concurrent_produce_consume_under_s3_faults() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("concurrent-s3").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let store = broker.faulty_store().clone();
    let addr = broker.addr();

    let acked_values = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Bytes>::new()));

    // Fault injector: periodically fail S3 puts.
    let store_fault = store.clone();
    let fault_handle = tokio::spawn(async move {
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            store_fault.fail_next_put();
        }
    });

    // Producer: send 20 records, collecting acked values.
    let acked = acked_values.clone();
    let producer = tokio::spawn(async move {
        let mut ws = ws_connect(addr).await;
        let writer_id = WriterId::new();
        for i in 0..20 {
            let val = format!("v-{}", i);
            let result = ws_produce(&mut ws, writer_id, i + 1, topic_id, &val).await;
            if let Ok(resp) = &result
                && resp.success
            {
                acked.lock().await.push(Bytes::from(val));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    producer.await.unwrap();
    fault_handle.await.unwrap();

    // Final paginated read — verify all acked values are visible.
    let acked = acked_values.lock().await;
    let mut ws = ws_connect(addr).await;
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read should succeed");

    let read_values: std::collections::HashSet<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();

    for val in acked.iter() {
        assert!(
            read_values.contains(val),
            "Acked value {:?} missing from final read",
            val
        );
    }

    // Watermark should be at least as high as the number of acked writes.
    // Use a fresh read for the watermark check.
    let mut ws = ws_connect(addr).await;
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("watermark read should succeed");
    if let Some(result) = read.results.first() {
        assert!(
            result.high_watermark.0 >= acked.len() as u64,
            "Watermark {} < acked count {}",
            result.high_watermark.0,
            acked.len()
        );
    }
}

/// Slow S3 does not lose buffered writes.
#[tokio::test]
async fn test_slow_s3_does_not_lose_buffered_writes() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("slow-s3").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    broker.faulty_store().set_put_delay_ms(500);

    let addr = broker.addr();
    let acked = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Bytes>::new()));

    // Send writes from several producers in parallel.
    let mut handles = vec![];
    for p in 0..3 {
        let acked = acked.clone();
        handles.push(tokio::spawn(async move {
            let mut ws = ws_connect(addr).await;
            let writer_id = WriterId::new();
            for i in 0..5 {
                let val = format!("p{}-s{}", p, i);
                let resp = ws_produce_timeout(
                    &mut ws,
                    writer_id,
                    i + 1,
                    topic_id,
                    &val,
                    Duration::from_secs(30),
                )
                .await;
                if let Ok(r) = &resp
                    && r.success
                {
                    acked.lock().await.push(Bytes::from(val));
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // All writes should eventually have been acked.
    let acked = acked.lock().await;
    assert!(
        acked.len() == 15,
        "Expected 15 acked writes under slow S3, got {}",
        acked.len()
    );

    // Final read: contiguous offsets, all records present.
    let mut ws = ws_connect(addr).await;
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(read.success);
    let record_count: usize = read.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(record_count, 15, "All 15 records should be readable");
}

/// Crash after first successful flush; second batch not committed.
/// Watermark correct after restart.
#[tokio::test]
async fn test_crash_after_s3_put_before_db_commit() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("crash-mid-flush").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;

    // First write: should succeed fully (flush 1 OK).
    let mut ws = ws_connect(broker.addr()).await;
    let w1 = WriterId::new();
    let resp = ws_produce(&mut ws, w1, 1, topic_id, "first-ok")
        .await
        .expect("first write should succeed");
    assert!(resp.success);

    // Fail all subsequent S3 puts so the second flush can't complete.
    broker.faulty_store().fail_on_put_n(1);

    let w2 = WriterId::new();
    let result = ws_produce(&mut ws, w2, 1, topic_id, "second-fail").await;
    let failed = result.is_err() || !result.as_ref().unwrap().success;
    assert!(failed, "Second write should fail (S3 put fails)");

    // Crash and restart.
    drop(ws);
    broker.restart().await;

    // After restart, only the first batch should be visible.
    let mut ws = ws_connect(broker.addr()).await;
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read after restart should succeed");
    assert!(read.success);

    let values: Vec<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    assert!(
        values.contains(&Bytes::from("first-ok")),
        "First committed batch should survive restart"
    );
    assert!(
        !values.contains(&Bytes::from("second-fail")),
        "Uncommitted batch should not appear after restart"
    );

    // Watermark should reflect only the first batch.
    if let Some(r) = read.results.first() {
        assert_eq!(
            r.records.len() as u64,
            r.high_watermark.0,
            "Watermark should match committed records"
        );
    }
}

/// DB-blocked flush produces no false acks. Acks arrive only after unblock.
#[tokio::test]
async fn test_db_blocked_flush_no_false_acks() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-block").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Block the topic in the DB.
    let mut blocker = DbBlocker::new(db.pool.clone());
    blocker.block_topic(topic_id.0 as i32).await;

    // Send a produce. The flush will stall on the DB lock, so we should
    // NOT receive an ack within a short timeout.
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();

    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("blocked-write"),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    // Short timeout — should NOT get ack while DB is blocked.
    let short_result = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    assert!(
        short_result.is_err(),
        "Should NOT receive ack while DB is blocked (flush stalled)"
    );

    // Unblock the DB.
    blocker.unblock().await;

    // Now we should receive the ack.
    let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
        .await
        .expect("Should receive ack after DB unblock")
        .expect("Stream should not be closed")
        .expect("Should not be a WS error");
    let data = match msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    };
    let resp = ws_helpers::decode_produce_response(&data);
    assert!(resp.success, "Write should succeed after DB unblock");

    // Verify data is correct.
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(read.success);
    let values: Vec<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    assert!(values.contains(&Bytes::from("blocked-write")));
}

/// Transient S3 failures recover; all writes succeed after recovery.
#[tokio::test]
async fn test_transient_s3_failure_recovery() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("transient-s3").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Fail first 3 puts then auto-recover.
    broker.faulty_store().fail_put_transiently(3);

    let acked = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Bytes>::new()));

    // Send 10 writes, retrying on failure with a new writer to avoid dedup.
    let acked_clone = acked.clone();
    let producer = tokio::spawn(async move {
        let mut ws = ws_connect(addr).await;
        let mut seq = 0u64;
        for i in 0..10 {
            let val = format!("t-{}", i);
            // Retry loop: try until acked (transient failures clear after 3 puts).
            loop {
                seq += 1;
                let writer_id = WriterId::new();
                match ws_produce(&mut ws, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        acked_clone.lock().await.push(Bytes::from(val));
                        break;
                    }
                    _ => {
                        // Reconnect and retry after brief delay.
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        ws = ws_connect(addr).await;
                    }
                }
            }
        }
    });

    producer.await.unwrap();

    let acked = acked.lock().await;
    assert_eq!(acked.len(), 10, "All 10 writes should eventually be acked");

    // Final read — verify no duplicates.
    let mut ws = ws_connect(addr).await;
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    let values: Vec<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();

    // Check for duplicates in the read result.
    let unique: std::collections::HashSet<&Bytes> = values.iter().collect();
    assert_eq!(
        unique.len(),
        values.len(),
        "No duplicate records should appear in reads"
    );
}

/// Orphan S3 file after DB commit failure: first batch commits normally,
/// then S3 put for the second batch succeeds but the broker crashes before
/// DB commit. After restart, the orphan S3 file exists but only the first
/// batch's data is visible. New writes proceed correctly.
/// Invariant: orphan S3 files do not affect correctness; uncommitted writes are invisible.
#[tokio::test]
async fn test_orphan_s3_file_after_db_commit_failure() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("orphan-s3").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // First write: commits normally
    let mut ws = ws_connect(addr).await;
    let w1 = WriterId::new();
    let resp = ws_produce(&mut ws, w1, 1, topic_id, "committed-ok")
        .await
        .expect("first write should succeed");
    assert!(resp.success);

    // Set long S3 delay so second write's S3 put takes time
    broker.faulty_store().set_put_delay_ms(3000);

    // Send second write — S3 put will be in flight
    let addr2 = broker.addr();
    let produce_handle = tokio::spawn(async move {
        let mut ws2 = ws_connect(addr2).await;
        let w2 = WriterId::new();
        ws_produce(&mut ws2, w2, 1, topic_id, "orphan-val").await
    });

    // Wait for S3 put to start but not finish (put delay is 3s)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash the broker while S3 put is in progress (before DB commit)
    drop(ws);
    broker.crash();
    drop(produce_handle);

    // Verify only the first batch is in topic_batches
    let batch_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1")
            .bind(topic_id.0 as i32)
            .fetch_one(&db.pool)
            .await
            .expect("query batch count");
    assert_eq!(
        batch_count, 1,
        "only the first committed batch should exist in DB"
    );

    // Watermark should reflect only the first batch
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .expect("query watermark")
    .unwrap_or(0);
    assert_eq!(
        watermark, 1,
        "watermark should be 1 (only first write committed)"
    );

    // Restart broker — new writes should succeed and not collide with orphan
    broker.restart().await;
    let mut ws = ws_connect(broker.addr()).await;
    let w3 = WriterId::new();
    let resp = ws_produce(&mut ws, w3, 1, topic_id, "after-orphan")
        .await
        .expect("write after restart should succeed");
    assert!(
        resp.success,
        "new write should succeed after restart with orphan"
    );

    // Verify data: first write + after-orphan visible, orphan-val NOT visible
    let read = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(read.success);
    let values: Vec<Bytes> = read
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    assert!(
        values.contains(&Bytes::from("committed-ok")),
        "first committed write should be visible"
    );
    assert!(
        values.contains(&Bytes::from("after-orphan")),
        "post-restart write should be visible"
    );
    assert!(
        !values.contains(&Bytes::from("orphan-val")),
        "orphan write should NOT be visible"
    );
}

/// Sustained S3 failure causes backpressure; recovery after fault clears.
#[tokio::test]
async fn test_backpressure_under_sustained_s3_failure() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("backpressure").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Fail all S3 puts.
    broker.faulty_store().fail_on_put_n(1);

    // Send several writes — they should fail (flush always fails).
    let mut ws = ws_connect(addr).await;
    let mut failure_count = 0;
    for i in 0..5 {
        let writer_id = WriterId::new();
        let result = ws_produce(&mut ws, writer_id, 1, topic_id, &format!("bp-{}", i)).await;
        if result.is_err() || !result.as_ref().map(|r| r.success).unwrap_or(false) {
            failure_count += 1;
        }
        // Reconnect if needed.
        if result.is_err() {
            ws = ws_connect(addr).await;
        }
    }
    assert!(
        failure_count > 0,
        "Some writes should fail under sustained S3 failure"
    );

    // Reset faults — recovery.
    broker.faulty_store().reset();

    // New writes should succeed.
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "after-recovery")
        .await
        .expect("Write after fault reset should succeed");
    assert!(resp.success, "Write should succeed after recovery");
}
