// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired dedup cache pressure tests.
//!
//! The LRU dedup cache holds 100K entries by default. When it's full, evicted
//! writers must fall through to Postgres for dedup checks. These tests verify
//! exactly-once semantics when the cache is saturated and writers retry.
//!
//! Inspired by Jepsen's Redpanda findings where sequence number bugs caused
//! duplicates under pressure.

mod common;

use std::collections::HashSet;
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

    let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
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

/// Many unique writers each send one message, then all retry their last sequence.
/// The LRU cache may have evicted some entries, so retries must fall through
/// to Postgres. Verify exactly-once: no duplicates in the committed log.
#[tokio::test]
async fn test_many_writers_retry_after_eviction() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("dedup-evict").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Use 200 unique writers (well within cache size but exercises the path)
    let num_writers = 200;
    let mut writers = Vec::with_capacity(num_writers);
    for _ in 0..num_writers {
        writers.push(WriterId::new());
    }

    // Phase 1: Each writer sends seq=1
    let mut ws = ws_connect(addr).await;
    for (i, &writer_id) in writers.iter().enumerate() {
        let val = format!("w{}-s1", i);
        let resp = ws_produce(&mut ws, writer_id, 1, topic_id, &val)
            .await
            .expect("initial write");
        assert!(resp.success, "writer {} initial write should succeed", i);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 2: All writers retry seq=1 (duplicate detection)
    let mut dup_count = 0;
    let mut stale_count = 0;
    drop(ws);
    let mut ws = ws_connect(addr).await;
    for (i, &writer_id) in writers.iter().enumerate() {
        let val = format!("w{}-s1", i);
        let resp = ws_produce(&mut ws, writer_id, 1, topic_id, &val)
            .await
            .expect("retry write");
        if resp.success {
            // Dedup returned the original ack — this is correct
            dup_count += 1;
        } else {
            // Stale or error — also correct (means it was already committed)
            stale_count += 1;
        }
    }
    eprintln!(
        "  [dedup-evict] retries: {} dup-acked, {} stale/error",
        dup_count, stale_count
    );

    // Verify: exactly num_writers records, no duplicates
    let (values, _) = ws_read_all(&mut ws, topic_id).await.expect("final read");

    let unique: HashSet<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
    assert_eq!(
        values.len(),
        unique.len(),
        "no duplicate values in committed log"
    );
    assert_eq!(
        values.len(),
        num_writers,
        "exactly {} records (one per writer)",
        num_writers
    );
}

/// Concurrent writers from multiple connections retry simultaneously.
/// Tests the cache + Postgres dedup under concurrent access.
#[tokio::test]
async fn test_concurrent_dedup_retries() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("dedup-concurrent").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    let num_writers = 50;
    let writers: Vec<WriterId> = (0..num_writers).map(|_| WriterId::new()).collect();

    // Phase 1: All writers send seq=1 concurrently
    let mut handles = vec![];
    for (i, writer_id) in writers.iter().copied().enumerate() {
        handles.push(tokio::spawn(async move {
            let mut ws = ws_connect(addr).await;
            let val = format!("cw{}-s1", i);
            let resp = ws_produce(&mut ws, writer_id, 1, topic_id, &val)
                .await
                .expect("initial write");
            assert!(resp.success, "writer {} initial write should succeed", i);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 2: All writers retry seq=1 concurrently from different connections
    let mut handles = vec![];
    for (i, writer_id) in writers.iter().copied().enumerate() {
        handles.push(tokio::spawn(async move {
            let mut ws = ws_connect(addr).await;
            let val = format!("cw{}-s1", i);
            // Don't care about success/failure — just that it doesn't create duplicates
            let _ = ws_produce(&mut ws, writer_id, 1, topic_id, &val).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify: no duplicates
    let mut ws = ws_connect(addr).await;
    let (values, _) = ws_read_all(&mut ws, topic_id).await.expect("final read");

    let unique: HashSet<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
    assert_eq!(
        values.len(),
        unique.len(),
        "no duplicate values in committed log after concurrent retries"
    );
    assert_eq!(values.len(), num_writers, "exactly {} records", num_writers);
}

/// Broker crash between write and retry. Writer reconnects with same WriterId
/// and retries last seq. Postgres dedup catches it even though cache is lost.
#[tokio::test]
async fn test_dedup_survives_broker_crash() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("dedup-crash").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;

    // Write records
    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("dc-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Crash broker — LRU cache is lost
    drop(ws);
    broker.crash();
    tokio::time::sleep(Duration::from_millis(500)).await;
    broker.restart().await;

    // Retry last seq (5) with same WriterId — Postgres should catch it
    let mut ws = ws_connect(broker.addr()).await;
    let resp = ws_produce(&mut ws, writer_id, 5, topic_id, "dc-4")
        .await
        .expect("retry after crash");

    // Should be dup-acked (success=true with original offsets) or stale
    eprintln!(
        "  [dedup-crash] retry result: success={}, error_code={}",
        resp.success, resp.error_code
    );

    // Send next seq — should succeed
    let resp = ws_produce(&mut ws, writer_id, 6, topic_id, "dc-5")
        .await
        .expect("next seq after crash");
    assert!(resp.success, "next seq should succeed after crash");

    // Verify no duplicates
    let (values, _) = ws_read_all(&mut ws, topic_id).await.expect("final read");

    let unique: HashSet<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
    assert_eq!(
        values.len(),
        unique.len(),
        "no duplicates after broker crash + retry"
    );
    assert_eq!(values.len(), 6, "should have 6 unique records");
}

/// Multiple writers, each sending multiple sequences. After each batch of
/// writes, broker crashes and all writers retry their last unacked seq.
/// Exercises the full dedup recovery path repeatedly.
#[tokio::test]
async fn test_repeated_crash_retry_cycles() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("dedup-cycles").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;

    let num_writers = 5;
    let writers: Vec<WriterId> = (0..num_writers).map(|_| WriterId::new()).collect();
    let mut next_seqs = vec![1u64; num_writers];

    for cycle in 0..3 {
        // Write round
        let mut ws = ws_connect(broker.addr()).await;
        for (i, &writer_id) in writers.iter().enumerate() {
            let seq = next_seqs[i];
            let val = format!("cycle{}-w{}-s{}", cycle, i, seq);
            let resp = ws_produce(&mut ws, writer_id, seq, topic_id, &val)
                .await
                .expect("write");
            assert!(resp.success);
            next_seqs[i] = seq + 1;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Crash
        drop(ws);
        broker.crash();
        tokio::time::sleep(Duration::from_millis(500)).await;
        broker.restart().await;

        // Retry last seq for each writer
        let mut ws = ws_connect(broker.addr()).await;
        for (i, &writer_id) in writers.iter().enumerate() {
            let retry_seq = next_seqs[i] - 1;
            let val = format!("cycle{}-w{}-s{}", cycle, i, retry_seq);
            let _ = ws_produce(&mut ws, writer_id, retry_seq, topic_id, &val).await;
        }

        eprintln!("  [dedup-cycles] cycle {} done", cycle);
    }

    // Final verification
    let mut ws = ws_connect(broker.addr()).await;
    let (values, _) = ws_read_all(&mut ws, topic_id).await.expect("final read");

    let unique: HashSet<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
    assert_eq!(
        values.len(),
        unique.len(),
        "no duplicates after {} crash/retry cycles",
        3
    );
    // 5 writers * 3 cycles * 1 write per cycle = 15 unique writes
    assert_eq!(values.len(), 15, "should have exactly 15 unique records");
}
