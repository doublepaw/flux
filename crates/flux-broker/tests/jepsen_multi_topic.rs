// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired multi-topic concurrent chaos tests.
//!
//! The flush pipeline merges batches from multiple topics into a single FL file
//! and commits offsets atomically. This test exercises:
//! - Concurrent writes to many topics
//! - Broker crash mid-flush with mixed-topic buffer
//! - Per-topic invariant verification

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::sync::Mutex;
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

/// 5 topics, 3 producers per topic, concurrent writes, verify per-topic invariants.
#[tokio::test]
async fn test_multi_topic_concurrent_writes() {
    let db = TestDb::new().await;
    let num_topics = 5;
    let producers_per_topic = 3;

    let mut topic_ids = Vec::new();
    for t in 0..num_topics {
        let tid = db.create_topic(&format!("multi-topic-{}", t)).await;
        topic_ids.push(TopicId(tid as u32));
    }

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Per-topic history
    let histories: Vec<Arc<Mutex<OperationHistory>>> = (0..num_topics)
        .map(|_| OperationHistory::shared())
        .collect();

    // Spawn producers
    let mut handles = vec![];
    for t in 0..num_topics {
        for p in 0..producers_per_topic {
            let history = histories[t].clone();
            let topic_id = topic_ids[t];
            handles.push(tokio::spawn(async move {
                let writer_id = WriterId::new();
                if let Ok((mut ws, _)) = connect_async(format!("ws://{}", addr)).await {
                    for i in 0..10 {
                        let val = format!("t{}-p{}-s{}", t, p, i);
                        let idx = {
                            let mut h = history.lock().await;
                            h.record_write(writer_id, topic_id, Bytes::from(val.clone()))
                        };
                        match ws_produce(&mut ws, writer_id, (i + 1) as u64, topic_id, &val).await {
                            Ok(resp) if resp.success => {
                                let offset =
                                    resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                                history
                                    .lock()
                                    .await
                                    .record_write_complete(idx, offset, true);
                            }
                            _ => {
                                history.lock().await.record_write_complete(idx, None, false);
                            }
                        }
                    }
                }
            }));
        }
    }

    for h in handles {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify per-topic invariants
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let mut ws = ws_connect(addr).await;
        let (values, hwm) = ws_read_all(&mut ws, *topic_id)
            .await
            .unwrap_or_else(|e| panic!("read topic {} failed: {}", t, e));

        let mut h = histories[t].lock().await;
        let idx = h.record_read(*topic_id, "final".to_string(), Offset(0));
        h.record_read_complete(idx, values.clone(), hwm, true);

        h.verify_acknowledged_writes_visible()
            .unwrap_or_else(|e| panic!("topic {}: {}", t, e));
        h.verify_unique_offsets()
            .unwrap_or_else(|e| panic!("topic {}: {}", t, e));
        h.verify_no_duplicates()
            .unwrap_or_else(|e| panic!("topic {}: {}", t, e));
        h.verify_no_phantom_writes()
            .unwrap_or_else(|e| panic!("topic {}: {}", t, e));

        let successful = h.writes.iter().filter(|w| w.success).count();
        eprintln!(
            "  [multi-topic] topic {}: {}/{} writes succeeded, {} records",
            t,
            successful,
            h.writes.len(),
            values.len()
        );
    }
}

/// 5 topics, concurrent writes, broker crash mid-flush, verify per-topic invariants.
#[tokio::test]
async fn test_multi_topic_broker_crash_mid_flush() {
    let db = TestDb::new().await;
    let num_topics = 5;

    let mut topic_ids = Vec::new();
    for t in 0..num_topics {
        let tid = db.create_topic(&format!("mt-crash-{}", t)).await;
        topic_ids.push(TopicId(tid as u32));
    }

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = Arc::new(Mutex::new(broker.addr()));
    let histories: Vec<Arc<Mutex<OperationHistory>>> = (0..num_topics)
        .map(|_| OperationHistory::shared())
        .collect();

    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn producers (one per topic for simplicity)
    let mut handles = vec![];
    for t in 0..num_topics {
        let history = histories[t].clone();
        let topic_id = topic_ids[t];
        let addr = addr.clone();
        let mut stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("mc-t{}-s{}", t, seq);

                if ws.is_none() {
                    let a = *addr.lock().await;
                    match connect_async(format!("ws://{}", a)).await {
                        Ok((w, _)) => ws = Some(w),
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                    }
                }

                let w = ws.as_mut().unwrap();
                let idx = {
                    let mut h = history.lock().await;
                    h.record_write(writer_id, topic_id, Bytes::from(val.clone()))
                };

                match ws_produce(w, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                        history
                            .lock()
                            .await
                            .record_write_complete(idx, offset, true);
                    }
                    _ => {
                        history.lock().await.record_write_complete(idx, None, false);
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }));
    }

    // Let producers run
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Crash broker
    broker.crash();
    eprintln!("  [mt-crash] broker crashed");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart
    broker.restart().await;
    *addr.lock().await = broker.addr();
    eprintln!("  [mt-crash] broker restarted at {}", broker.addr());

    // Let producers run more
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop
    let _ = stop_tx.send(());
    for h in handles {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify per-topic
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let mut ws = ws_connect(broker.addr()).await;
        let (values, hwm) = ws_read_all(&mut ws, *topic_id)
            .await
            .unwrap_or_else(|e| panic!("read topic {} failed: {}", t, e));

        let mut h = histories[t].lock().await;
        let idx = h.record_read(*topic_id, "final".to_string(), Offset(0));
        h.record_read_complete(idx, values.clone(), hwm, true);

        h.verify_acknowledged_writes_visible()
            .unwrap_or_else(|e| panic!("topic {} after crash: {}", t, e));
        h.verify_unique_offsets()
            .unwrap_or_else(|e| panic!("topic {} after crash: {}", t, e));
        h.verify_no_duplicates()
            .unwrap_or_else(|e| panic!("topic {} after crash: {}", t, e));
        h.verify_no_phantom_writes()
            .unwrap_or_else(|e| panic!("topic {} after crash: {}", t, e));

        let successful = h.writes.iter().filter(|w| w.success).count();
        let failed = h.writes.len() - successful;
        eprintln!(
            "  [mt-crash] topic {}: {}/{} succeeded, {} failed, {} records",
            t,
            successful,
            h.writes.len(),
            failed,
            values.len()
        );
    }
}

/// S3 partition affects all topics equally. After heal, all topics recover.
#[tokio::test]
async fn test_multi_topic_s3_partition() {
    let db = TestDb::new().await;
    let num_topics = 3;

    let mut topic_ids = Vec::new();
    for t in 0..num_topics {
        let tid = db.create_topic(&format!("mt-s3-{}", t)).await;
        topic_ids.push(TopicId(tid as u32));
    }

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let store = broker.faulty_store().clone();

    // Write to all topics
    let mut ws = ws_connect(addr).await;
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let writer_id = WriterId::new();
        for i in 0..5 {
            let resp = ws_produce(
                &mut ws,
                writer_id,
                (i + 1) as u64,
                *topic_id,
                &format!("ms3-t{}-{}", t, i),
            )
            .await
            .expect("pre-partition write");
            assert!(resp.success);
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Partition S3
    store.partition_puts();
    eprintln!("  [mt-s3] S3 partitioned");

    // Reads should still work (data already flushed)
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let (values, _) = ws_read_all(&mut ws, *topic_id)
            .await
            .unwrap_or_else(|e| panic!("read topic {} during partition failed: {}", t, e));
        assert_eq!(values.len(), 5, "topic {} should have 5 records", t);
    }

    // Heal and write more
    store.heal_partition();
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let w = WriterId::new();
        let resp = ws_produce(&mut ws, w, 1, *topic_id, &format!("ms3-post-{}", t))
            .await
            .expect("post-partition write");
        assert!(
            resp.success,
            "topic {} post-partition write should succeed",
            t
        );
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify all topics have correct data
    for (t, topic_id) in topic_ids.iter().enumerate() {
        let (values, _) = ws_read_all(&mut ws, *topic_id)
            .await
            .unwrap_or_else(|e| panic!("final read topic {} failed: {}", t, e));
        assert_eq!(
            values.len(),
            6,
            "topic {} should have 6 records (5 pre + 1 post)",
            t
        );
    }
}

/// Crash a broker during continuous writes and verify that some writes fail.
/// Without the CancellationToken fix, crash() only aborts the accept loop —
/// the flush loop and connection handlers survive, so all writes succeed.
/// With the fix, cancel_token stops children and clients see failures.
#[tokio::test]
async fn test_crash_causes_inflight_write_failures() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("crash-failures").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = Arc::new(Mutex::new(broker.addr()));

    let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let failure_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn 3 concurrent writers that write continuously
    let mut handles = vec![];
    for w in 0..3 {
        let addr = addr.clone();
        let sc = success_count.clone();
        let fc = failure_count.clone();
        let mut stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("cf-w{}-s{}", w, seq);

                if ws.is_none() {
                    let a = *addr.lock().await;
                    match connect_async(format!("ws://{}", a)).await {
                        Ok((w, _)) => ws = Some(w),
                        Err(_) => {
                            fc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }

                let w_ws = ws.as_mut().unwrap();
                match ws_produce(w_ws, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    _ => {
                        fc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // Let writers run and accumulate some successful writes
    tokio::time::sleep(Duration::from_secs(2)).await;
    let pre_crash_successes = success_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        pre_crash_successes > 0,
        "should have some successful writes before crash"
    );

    // Reset failure counter, then crash
    failure_count.store(0, std::sync::atomic::Ordering::SeqCst);
    broker.crash();
    eprintln!(
        "  [crash-failures] broker crashed after {} successes",
        pre_crash_successes
    );

    // Let writers run against the dead broker for a bit
    tokio::time::sleep(Duration::from_secs(1)).await;

    let crash_failures = failure_count.load(std::sync::atomic::Ordering::SeqCst);
    eprintln!(
        "  [crash-failures] {} failures during crash",
        crash_failures
    );

    // Stop writers, restart broker
    let _ = stop_tx.send(());
    for h in handles {
        let _ = h.await;
    }

    broker.restart().await;

    // KEY ASSERTION: crash must cause at least some write failures.
    // Without the CancellationToken fix, crash() doesn't kill connection
    // handlers or flush loop, so all in-flight writes succeed → 0 failures.
    assert!(
        crash_failures > 0,
        "INVARIANT: broker crash must cause some inflight write failures \
         (got 0 failures — crash() is not stopping child tasks)"
    );

    // Verify data integrity: all committed data should be readable
    let mut ws = ws_connect(broker.addr()).await;
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read after crash");
    eprintln!(
        "  [crash-failures] {} records committed after crash+restart",
        values.len()
    );
    assert!(
        !values.is_empty(),
        "should have some committed records after restart"
    );
}
