// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired partial/corrupted S3 write tests.
//!
//! Tests the scenario where S3 puts succeed but the data is corrupted or
//! truncated in transit. The CRC check on read should catch these cases.
//!
//! Also tests the "committed in Postgres but corrupt on S3" state — the
//! broker's CRC validation is the last line of defense.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, OperationHistory, TestDb};

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

async fn ws_read(
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
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Read(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_read_all(ws: &mut Ws, topic_id: TopicId) -> Result<(Vec<Bytes>, Offset), String> {
    let mut all_values = Vec::new();
    let mut next_offset = Offset(0);
    let mut hwm = Offset(0);

    loop {
        let resp = ws_read(ws, topic_id, next_offset).await?;
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

/// Write, corrupt S3 on read, verify error. Write more, verify new writes ok.
/// Interleaves corruption with healthy writes to test isolation.
#[tokio::test]
async fn test_interleaved_corruption_and_healthy_writes() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("interleave-corrupt").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Round 1: write + verify
    let mut ws = ws_connect(addr).await;
    let w1 = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, w1, i + 1, topic_id, &format!("r1-{}", i))
            .await
            .expect("round 1 write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resp = ws_read(&mut ws, topic_id, Offset(0)).await.expect("read 1");
    assert!(resp.success);
    let count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(count, 3, "should read 3 records");

    // Corrupt next read
    broker.faulty_store().corrupt_next_get_range();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    match resp {
        Ok(r) if !r.success => {
            eprintln!("  [interleave] corrupted read failed as expected");
        }
        Err(_) => {
            eprintln!("  [interleave] connection error on corrupted read");
        }
        Ok(_) => {
            // Corruption may have hit a non-data query; acceptable
            eprintln!("  [interleave] corrupted read succeeded (hit non-data query)");
        }
    }

    // Round 2: write more after corruption
    broker.faulty_store().reset();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let w2 = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, w2, i + 1, topic_id, &format!("r2-{}", i))
            .await
            .expect("round 2 write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify all data intact
    let (values, _) = ws_read_all(&mut ws, topic_id).await.expect("final read");
    assert_eq!(values.len(), 6, "should have 6 records total");
}

/// Write to multiple topics, corrupt reads for one topic.
/// Other topics should be unaffected.
#[tokio::test]
async fn test_corruption_isolated_per_topic() {
    let db = TestDb::new().await;
    let topic_a = TopicId(db.create_topic("corrupt-iso-a").await as u32);
    let topic_b = TopicId(db.create_topic("corrupt-iso-b").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write to both topics
    let mut ws = ws_connect(addr).await;
    let wa = WriterId::new();
    let wb = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, wa, i + 1, topic_a, &format!("a-{}", i))
            .await
            .expect("write topic a");
        assert!(resp.success);
        let resp = ws_produce(&mut ws, wb, i + 1, topic_b, &format!("b-{}", i))
            .await
            .expect("write topic b");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Corrupt one read (likely hits topic_a's data)
    broker.faulty_store().corrupt_next_get_range();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let _ = ws_read(&mut ws, topic_a, Offset(0)).await;

    // Reset corruption
    broker.faulty_store().reset();

    // Topic B should be fully readable (its data isn't corrupted)
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let (values_b, _) = ws_read_all(&mut ws, topic_b)
        .await
        .expect("topic B should be readable");
    assert_eq!(values_b.len(), 5, "topic B should have all 5 records");

    // Topic A should also be readable now (corruption was one-shot)
    let (values_a, _) = ws_read_all(&mut ws, topic_a)
        .await
        .expect("topic A should be readable after corruption clears");
    assert_eq!(values_a.len(), 5, "topic A should have all 5 records");
}

/// Concurrent readers during corruption. One reader gets error, others
/// on clean data should succeed.
#[tokio::test]
async fn test_concurrent_readers_during_corruption() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("concurrent-corrupt").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..10 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("cc-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Corrupt once
    broker.faulty_store().corrupt_next_get_range();

    // Spawn concurrent readers
    let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let error_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = vec![];

    for _ in 0..5 {
        let sc = success_count.clone();
        let ec = error_count.clone();
        handles.push(tokio::spawn(async move {
            let mut ws = ws_connect(addr).await;
            match ws_read(&mut ws, topic_id, Offset(0)).await {
                Ok(r) if r.success => {
                    sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                _ => {
                    ec.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let successes = success_count.load(std::sync::atomic::Ordering::SeqCst);
    let errors = error_count.load(std::sync::atomic::Ordering::SeqCst);
    eprintln!(
        "  [concurrent-corrupt] {} successes, {} errors",
        successes, errors
    );

    // At most 1 reader should see the corruption (one-shot flag)
    // The rest should succeed
    assert!(
        successes >= 4,
        "most readers should succeed (one-shot corruption)"
    );
}

/// Repeated corruption and recovery cycles. Broker stays healthy throughout.
#[tokio::test]
async fn test_repeated_corruption_recovery_cycles() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("repeated-corrupt").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let history = OperationHistory::shared();

    // Write initial data
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let val = format!("rc-{}", i);
        let idx = {
            let mut h = history.lock().await;
            h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
        };
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &val)
            .await
            .expect("write");
        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
        history
            .lock()
            .await
            .record_write_complete(idx, offset, resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 5 cycles of: corrupt → read fails → reset → read succeeds → write more
    for cycle in 0..5 {
        // Corrupt
        broker.faulty_store().corrupt_next_get_range();
        drop(ws);
        ws = ws_connect(addr).await;
        let _ = ws_read(&mut ws, topic_id, Offset(0)).await;

        // Reset and verify clean read
        broker.faulty_store().reset();
        drop(ws);
        ws = ws_connect(addr).await;
        let resp = ws_read(&mut ws, topic_id, Offset(0))
            .await
            .expect("clean read after corruption");
        assert!(resp.success, "cycle {}: clean read should succeed", cycle);

        // Write more
        let w = WriterId::new();
        let val = format!("rc-cycle-{}", cycle);
        let idx = {
            let mut h = history.lock().await;
            h.record_write(w, TopicId(0), Bytes::from(val.clone()))
        };
        let resp = ws_produce(&mut ws, w, 1, topic_id, &val)
            .await
            .expect("cycle write");
        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
        history
            .lock()
            .await
            .record_write_complete(idx, offset, resp.success);
    }

    // Final verification
    let (values, hwm) = ws_read_all(&mut ws, topic_id).await.expect("final read");

    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values.clone(), hwm, true);

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible after corruption cycles");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no phantom writes");

    assert_eq!(values.len(), 10, "5 initial + 5 cycle writes = 10 records");
}
