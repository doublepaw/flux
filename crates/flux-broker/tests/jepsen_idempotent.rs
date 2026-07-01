// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired idempotent producer tests.
//!
//! Tests the dedup path: LRU cache + `writer_state` DB table.
//! Verifies that retried produces with the same (writer_id, seq) never create duplicates.

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
use common::{CrashableWsBroker, TestDb};

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

async fn ws_read_all(ws: &mut Ws, topic_id: TopicId) -> Result<Vec<Bytes>, String> {
    let mut all_values = Vec::new();
    let mut next_offset = Offset(0);

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
        let mut hwm = Offset(0);
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
    Ok(all_values)
}

/// Send same (writer_id, seq=1) twice. Second returns cached ack, record appears once.
#[tokio::test]
async fn test_retry_same_seq_returns_cached_ack() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-retry").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();

    // First send
    let resp1 = ws_produce(&mut ws, writer_id, 1, topic_id, "dedup-val")
        .await
        .expect("first produce should succeed");
    assert!(resp1.success);
    let ack1 = resp1.append_acks.clone();

    // Retry same (writer_id, seq=1)
    let resp2 = ws_produce(&mut ws, writer_id, 1, topic_id, "dedup-val")
        .await
        .expect("retry produce should succeed");
    assert!(resp2.success, "retry should return cached ack");
    assert_eq!(resp2.append_acks, ack1, "acks should match");

    // Verify exactly 1 record
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(values.len(), 1, "exactly 1 record should exist");
    assert_eq!(values[0], Bytes::from("dedup-val"));
}

/// Retry during slow flush. Both connections get same ack via InFlightAppendDecision::Wait.
#[tokio::test]
async fn test_retry_during_slow_flush_no_duplicate() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-slow-flush").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    broker.faulty_store().set_put_delay_ms(2000);

    let writer_id = WriterId::new();
    let addr = broker.addr();

    // Send from first connection
    let mut ws1 = ws_connect(addr).await;
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("slow-dedup"),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws1.send(Message::Binary(buf)).await.unwrap();

    // Brief delay, then send duplicate from second connection
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut ws2 = ws_connect(addr).await;
    let req2 = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("slow-dedup"),
            }],
        }],
    };
    let buf2 = ws_helpers::encode_client_frame(ClientMessage::Append(req2), 8192);
    ws2.send(Message::Binary(buf2)).await.unwrap();

    // Both should receive acks (wait long enough for the slow flush)
    let msg1 = tokio::time::timeout(Duration::from_secs(15), ws1.next())
        .await
        .expect("ws1 should get response")
        .expect("ws1 not closed")
        .expect("ws1 no error");
    let msg2 = tokio::time::timeout(Duration::from_secs(15), ws2.next())
        .await
        .expect("ws2 should get response")
        .expect("ws2 not closed")
        .expect("ws2 no error");

    let resp1 = ws_helpers::decode_produce_response(match &msg1 {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    let resp2 = ws_helpers::decode_produce_response(match &msg2 {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });

    assert!(resp1.success, "first should succeed");
    assert!(resp2.success, "second should succeed (cached or waited)");

    // Reset delay and verify exactly 1 record
    broker.faulty_store().reset();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut ws = ws_connect(addr).await;
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(
        values.len(),
        1,
        "exactly 1 record, no duplicate from slow flush retry"
    );
}

/// Crash + restart clears LRU cache. Retry hits DB slow-path → Duplicate.
#[tokio::test]
async fn test_retry_after_crash_with_db_dedup() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-crash").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let writer_id = WriterId::new();

    // Write and get ack
    let mut ws = ws_connect(broker.addr()).await;
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "crash-dedup")
        .await
        .expect("produce should succeed");
    assert!(resp.success);

    // Crash + restart (LRU cache lost)
    drop(ws);
    broker.restart().await;

    // Retry same (writer_id, seq=1) — should hit DB dedup
    let mut ws = ws_connect(broker.addr()).await;
    let resp2 = ws_produce(&mut ws, writer_id, 1, topic_id, "crash-dedup")
        .await
        .expect("retry should succeed");
    assert!(resp2.success, "DB dedup should return cached ack");

    // Verify no duplicate
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(values.len(), 1, "no duplicate after crash+restart");
}

/// Send seq=1, then seq=2. Retry seq=1 → ERR_STALE_SEQUENCE.
#[tokio::test]
async fn test_stale_seq_rejected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-stale").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();

    // Send seq=1
    let resp1 = ws_produce(&mut ws, writer_id, 1, topic_id, "val-1")
        .await
        .expect("seq=1 should succeed");
    assert!(resp1.success);

    // Send seq=2
    let resp2 = ws_produce(&mut ws, writer_id, 2, topic_id, "val-2")
        .await
        .expect("seq=2 should succeed");
    assert!(resp2.success);

    // Retry seq=1 (stale)
    let resp3 = ws_produce(&mut ws, writer_id, 1, topic_id, "val-1")
        .await
        .expect("stale retry should return response");

    assert!(!resp3.success, "stale seq retry should not succeed");
    assert_eq!(
        resp3.error_code,
        flux_wire::ERR_STALE_SEQUENCE,
        "stale seq retry should return ERR_STALE_SEQUENCE, got error_code={}",
        resp3.error_code
    );
}

/// Dedup cache eviction then DB fallback: writer sends seq=1, cache is flushed
/// (via broker restart which clears LRU), retry seq=1 hits DB dedup → no duplicate.
/// Invariant: dedup is correct even after in-memory cache eviction.
#[tokio::test]
async fn test_dedup_cache_eviction_db_fallback() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-evict").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let writer_id = WriterId::new();

    // Write seq=1 and get ack
    let mut ws = ws_connect(broker.addr()).await;
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "evict-val")
        .await
        .expect("first produce should succeed");
    assert!(resp.success);
    let ack1 = resp.append_acks.clone();

    // Restart broker to clear LRU cache (simulates cache eviction)
    drop(ws);
    broker.restart().await;

    // Retry seq=1 — cache miss, DB lookup should find Duplicate
    let mut ws = ws_connect(broker.addr()).await;
    let resp2 = ws_produce(&mut ws, writer_id, 1, topic_id, "evict-val")
        .await
        .expect("retry should succeed");
    assert!(
        resp2.success,
        "DB dedup should return cached ack after eviction"
    );
    assert_eq!(resp2.append_acks, ack1, "acks should match original");

    // Verify exactly 1 record — no duplicate
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(
        values.len(),
        1,
        "no duplicate after cache eviction + DB fallback"
    );
    assert_eq!(values[0], Bytes::from("evict-val"));
}

/// Writer state GC doesn't break active writers: writer A sends seq=1,
/// writer_state is deleted (simulating GC with zero retention), retry seq=1
/// is treated as a new write (no cached ack), but offsets remain valid.
/// Invariant: GC of writer state is safe; worst case is a new offset.
#[tokio::test]
async fn test_writer_state_gc_does_not_break_active_writers() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-gc").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let writer_id = WriterId::new();

    // Write seq=1 and get ack
    let mut ws = ws_connect(broker.addr()).await;
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "gc-val")
        .await
        .expect("first produce should succeed");
    assert!(resp.success);

    // Simulate GC: delete writer_state from DB and restart to clear cache
    sqlx::query("DELETE FROM writer_state WHERE writer_id = $1")
        .bind(writer_id.0)
        .execute(&db.pool)
        .await
        .expect("GC delete should succeed");

    drop(ws);
    broker.restart().await;

    // Retry seq=1 — no cached ack in cache or DB, treated as new write
    let mut ws = ws_connect(broker.addr()).await;
    let resp2 = ws_produce(&mut ws, writer_id, 1, topic_id, "gc-val")
        .await
        .expect("retry after GC should succeed");
    assert!(resp2.success, "retry after GC should succeed as new write");

    // May have 1 or 2 records (duplicate is acceptable per whitepaper)
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(
        !values.is_empty() && values.len() <= 2,
        "should have 1 or 2 records after GC, got {}",
        values.len()
    );

    // Offsets should be valid (no gaps in watermark)
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query watermark");
    assert_eq!(
        watermark as usize,
        values.len(),
        "watermark should match actual record count"
    );
}

/// Fail S3 put → flush fails → retry same seq → Accept (first never committed).
#[tokio::test]
async fn test_retry_after_failed_flush() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("idem-fail-flush").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let writer_id = WriterId::new();

    // Fail S3 put
    broker.faulty_store().fail_next_put();

    let mut ws = ws_connect(broker.addr()).await;
    let result = ws_produce(&mut ws, writer_id, 1, topic_id, "fail-then-retry").await;
    let failed = result.is_err() || !result.as_ref().unwrap().success;
    assert!(failed, "write should fail during S3 failure");

    // Reset store, reconnect
    broker.faulty_store().reset();
    drop(ws);
    let mut ws = ws_connect(broker.addr()).await;

    // Retry same (writer_id, seq=1) — first never committed to DB, so dedup returns Accept
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "fail-then-retry")
        .await
        .expect("retry should succeed");
    assert!(resp.success, "retry after failed flush should succeed");

    // Verify exactly 1 record
    let values = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(values.len(), 1, "exactly 1 record after retry");
    assert_eq!(values[0], Bytes::from("fail-then-retry"));
}
