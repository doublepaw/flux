// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired multi-broker tests.
//!
//! Tests multiple brokers sharing the same DB + S3 store.
//! Verifies offset uniqueness, crash tolerance, cross-broker dedup,
//! and consumer group behavior across brokers.

mod common;

use std::collections::HashSet;
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
use common::{MultiBrokerCluster, OperationHistory, TestDb};

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

/// 2 brokers, 2 producers each writing to topic.
/// All acked offsets globally unique (tests FOR UPDATE serialization).
#[tokio::test]
async fn test_concurrent_writes_unique_offsets() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("multi-unique").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let addrs = cluster.addrs();

    let all_offsets = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));

    let mut handles = vec![];
    for (broker_idx, &addr) in addrs.iter().enumerate() {
        for p in 0..2 {
            let offsets = all_offsets.clone();
            handles.push(tokio::spawn(async move {
                let mut ws = ws_connect(addr).await;
                let writer_id = WriterId::new();
                for i in 0..10 {
                    let val = format!("b{}-p{}-s{}", broker_idx, p, i);
                    let seq = i + 1;
                    match ws_produce(&mut ws, writer_id, seq, topic_id, &val).await {
                        Ok(resp) if resp.success => {
                            for ack in &resp.append_acks {
                                offsets
                                    .lock()
                                    .await
                                    .push((ack.start_offset.0, ack.end_offset.0));
                            }
                        }
                        _ => {
                            // Reconnect on failure
                            ws = ws_connect(addr).await;
                        }
                    }
                }
            }));
        }
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all individual offsets are unique
    let ranges = all_offsets.lock().await;
    let mut all_individual: Vec<u64> = Vec::new();
    for &(start, end) in ranges.iter() {
        for o in start..end {
            all_individual.push(o);
        }
    }
    let unique: HashSet<u64> = all_individual.iter().copied().collect();
    assert_eq!(
        all_individual.len(),
        unique.len(),
        "All acked offsets should be globally unique across brokers"
    );
}

/// 2 brokers. Write through both. Crash broker 0. Continue through broker 1.
/// Restart broker 0. All acked writes visible, contiguous offsets.
#[tokio::test]
async fn test_broker_crash_other_continues() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("multi-crash").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let acked_values = Arc::new(Mutex::new(Vec::<Bytes>::new()));

    // Write through broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    for i in 0..5 {
        let val = format!("b0-{}", i);
        let resp = ws_produce(&mut ws0, w0, i + 1, topic_id, &val)
            .await
            .expect("write through broker 0");
        if resp.success {
            acked_values.lock().await.push(Bytes::from(val));
        }
    }

    // Write through broker 1
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    for i in 0..5 {
        let val = format!("b1-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("write through broker 1");
        if resp.success {
            acked_values.lock().await.push(Bytes::from(val));
        }
    }

    // Crash broker 0
    drop(ws0);
    cluster.crash_broker(0).await;

    // Continue through broker 1
    for i in 5..10 {
        let val = format!("b1-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("write through broker 1 after crash");
        if resp.success {
            acked_values.lock().await.push(Bytes::from(val));
        }
    }

    // Restart broker 0
    cluster.restart_broker(0).await;

    // Verify all acked writes visible from either broker
    let mut ws = ws_connect(cluster.broker(1).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");

    let acked = acked_values.lock().await;

    // All acked values present
    let value_set: HashSet<Bytes> = values.iter().cloned().collect();
    for v in acked.iter() {
        assert!(
            value_set.contains(v),
            "Acked value {:?} missing after broker crash/restart",
            v
        );
    }

    // Offsets are contiguous: records count == watermark (no gaps)
    assert_eq!(
        values.len() as u64,
        hwm.0,
        "Offsets should be contiguous: {} records but watermark is {}",
        values.len(),
        hwm.0
    );
}

/// Consumer group across brokers: Reader A on broker 0, Reader B on broker 1.
/// Both join the same group. Verify group state is consistent in DB.
/// Crash broker 0, B continues. State persists across brokers.
#[tokio::test]
async fn test_consumer_group_across_brokers() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("multi-cg").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Reader A joins via broker 0
    let mut ws_a = ws_connect(cluster.broker(0).addr()).await;
    let join_req_a = reader::JoinGroupRequest {
        group_id: "multi-cg-group".to_string(),
        topic_ids: vec![topic_id],
        reader_id: "reader-a".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::JoinGroup(join_req_a), 8192);
    ws_a.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_a.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let resp_a = ws_helpers::decode_join_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(resp_a.success, "Reader A join should succeed");

    // Reader B joins via broker 1
    let mut ws_b = ws_connect(cluster.broker(1).addr()).await;
    let join_req_b = reader::JoinGroupRequest {
        group_id: "multi-cg-group".to_string(),
        topic_ids: vec![topic_id],
        reader_id: "reader-b".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::JoinGroup(join_req_b), 8192);
    ws_b.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_b.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let resp_b = ws_helpers::decode_join_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(resp_b.success, "Reader B join should succeed");

    // Verify both members exist in DB
    let member_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM reader_members WHERE group_id = $1")
            .bind("multi-cg-group")
            .fetch_one(cluster.pool())
            .await
            .expect("query members");
    assert_eq!(member_count, 2, "both readers should be members");

    // --- Crash broker 0, verify B continues via heartbeat ---
    drop(ws_a);
    // Simulate session timeout by setting reader-a's last_heartbeat to the past
    sqlx::query(
        "UPDATE reader_members SET last_heartbeat = NOW() - interval '60 seconds' \
         WHERE group_id = $1 AND reader_id = $2",
    )
    .bind("multi-cg-group")
    .bind("reader-a")
    .execute(cluster.pool())
    .await
    .expect("expire reader-a heartbeat");

    // Reader B sends heartbeat, triggering stale member detection
    let hb_req = reader::HeartbeatRequest {
        group_id: "multi-cg-group".to_string(),
        topic_id,
        reader_id: "reader-b".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Heartbeat(hb_req), 8192);
    ws_b.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_b.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let hb_resp = match ws_helpers::decode_server_frame(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    }) {
        flux_wire::ServerMessage::Heartbeat(r) => r,
        other => panic!("expected heartbeat response, got {:?}", other),
    };
    assert!(hb_resp.success, "heartbeat should succeed");

    // Verify reader-a was expired from members
    let remaining_members: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_members WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("multi-cg-group")
    .bind(topic_id.0 as i32)
    .fetch_one(cluster.pool())
    .await
    .expect("query remaining members");
    assert_eq!(
        remaining_members, 1,
        "only reader-b should remain after member expiry"
    );
}

/// Producer writes via broker 0, ack received. Retry same (writer_id, seq) via broker 1.
/// Broker 1 checks writer_state table → returns Duplicate. No duplicate record.
#[tokio::test]
async fn test_cross_broker_dedup() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("cross-dedup").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let writer_id = WriterId::new();

    // Write via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let resp = ws_produce(&mut ws0, writer_id, 1, topic_id, "cross-dedup-val")
        .await
        .expect("produce via broker 0");
    assert!(resp.success);

    // Retry same (writer_id, seq=1) via broker 1
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let resp2 = ws_produce(&mut ws1, writer_id, 1, topic_id, "cross-dedup-val")
        .await
        .expect("retry via broker 1");
    assert!(resp2.success, "cross-broker dedup should return cached ack");

    // Verify exactly 1 record via either broker
    let (values, _) = ws_read_all(&mut ws1, topic_id)
        .await
        .expect("read should succeed");
    assert_eq!(values.len(), 1, "no duplicate from cross-broker retry");
}

/// 3 brokers, rotating crashes. Producers/consumers connect to random surviving broker.
/// Full OperationHistory verification.
#[ignore]
#[tokio::test]
async fn test_multi_broker_chaos() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("multi-chaos").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 3).await;
    let history = OperationHistory::shared();

    let addrs = Arc::new(Mutex::new(cluster.addrs()));
    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Producers
    let mut producer_handles = vec![];
    for p in 0..4 {
        let history = history.clone();
        let addrs = addrs.clone();
        let mut stop_rx = stop_tx.subscribe();

        producer_handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("p{}-s{}", p, seq);

                if ws.is_none() {
                    let all_addrs = addrs.lock().await;
                    let addr = all_addrs[p % all_addrs.len()];
                    match connect_async(format!("ws://{}", addr)).await {
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
                    h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
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
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // Consumers
    let mut consumer_handles = vec![];
    for c in 0..2 {
        let history = history.clone();
        let addrs = addrs.clone();
        let mut stop_rx = stop_tx.subscribe();

        consumer_handles.push(tokio::spawn(async move {
            let reader_id = format!("reader-{}", c);
            let mut next_offset = Offset(0);
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                if ws.is_none() {
                    let all_addrs = addrs.lock().await;
                    let addr = all_addrs[c % all_addrs.len()];
                    match connect_async(format!("ws://{}", addr)).await {
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
                    h.record_read(TopicId(0), reader_id.clone(), next_offset)
                };

                match ws_read(w, topic_id, next_offset).await {
                    Ok(resp) if resp.success => {
                        let values: Vec<Bytes> = resp
                            .results
                            .iter()
                            .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
                            .collect();
                        let hwm = resp
                            .results
                            .first()
                            .map(|r| r.high_watermark)
                            .unwrap_or(Offset(0));
                        next_offset = Offset(next_offset.0 + values.len() as u64);
                        history
                            .lock()
                            .await
                            .record_read_complete(idx, values, hwm, true);
                    }
                    _ => {
                        history
                            .lock()
                            .await
                            .record_read_complete(idx, vec![], Offset(0), false);
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }));
    }

    // Rotating crashes
    for round in 0..6 {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let crash_idx = round % 3;
        eprintln!("  [multi-chaos] crashing broker {}", crash_idx);
        cluster.crash_broker(crash_idx).await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        eprintln!("  [multi-chaos] restarting broker {}", crash_idx);
        cluster.restart_broker(crash_idx).await;
        *addrs.lock().await = cluster.addrs();
    }

    // Stop workers
    let _ = stop_tx.send(());
    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    // Final read for verification
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(cluster.broker(0).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read should succeed");

    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values, hwm, true);

    // Invariant checks
    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_watermark_monotonic()
        .expect("INVARIANT: watermark monotonic");
    h.verify_no_duplicates()
        .expect("INVARIANT: no duplicate records");
    h.verify_monotonic_sends()
        .expect("INVARIANT: per-producer offsets monotonic");
    h.verify_write_write_causal()
        .expect("INVARIANT: write-write causal ordering");
    h.verify_poll_contiguity()
        .expect("INVARIANT: sequential reads don't skip offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no unacked values in reads");
}

/// 2 brokers. Partition broker 0 from S3. Writes via broker 0 hang.
/// Writes via broker 1 succeed. Heal. All acked writes visible.
#[tokio::test]
async fn test_asymmetric_s3_partition_one_broker() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("asym-s3").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Write some baseline data via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws0, w0, i + 1, topic_id, &format!("pre-{}", i))
            .await
            .expect("baseline write");
        assert!(resp.success);
    }

    // Partition broker 0 from S3 (puts only)
    cluster.broker_store(0).partition_puts();

    // Write via broker 0 should hang (timeout)
    let w0b = WriterId::new();
    let hung = tokio::time::timeout(
        Duration::from_secs(2),
        ws_produce(&mut ws0, w0b, 1, topic_id, "should-hang"),
    )
    .await;
    assert!(
        hung.is_err(),
        "broker 0 write should hang while S3 partitioned"
    );

    // Write via broker 1 should succeed
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    let mut acked_via_b1 = vec![];
    for i in 0..5 {
        let val = format!("b1-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("broker 1 write");
        assert!(resp.success, "broker 1 write should succeed");
        acked_via_b1.push(Bytes::from(val));
    }

    // Heal broker 0
    cluster.broker_store(0).heal_partition();

    // Wait for broker 0's pending write to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all broker 1 acked writes visible
    let (values, hwm) = ws_read_all(&mut ws1, topic_id).await.expect("final read");
    for v in &acked_via_b1 {
        assert!(values.contains(v), "broker 1 acked value {:?} missing", v);
    }

    // Offsets contiguous
    assert_eq!(values.len() as u64, hwm.0, "offsets should be contiguous");

    // No duplicate values
    let unique: HashSet<&Bytes> = values.iter().collect();
    assert_eq!(values.len(), unique.len(), "no duplicate values");
}

/// 2 brokers. Partition broker 0 from S3 (puts only, gets ok).
/// Both accept writes. Broker 0's acks time out (no false acks).
/// Broker 1's acks succeed. Heal. Verify no false acks from broker 0.
#[tokio::test]
async fn test_split_brain_writes() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("split-brain").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let history = OperationHistory::shared();

    // Partition broker 0 from S3 (puts only)
    cluster.broker_store(0).partition_puts();

    // Broker 0: writes should time out (no false acks)
    let history_b0 = history.clone();
    let addr0 = cluster.broker(0).addr();
    let b0_handle = tokio::spawn(async move {
        let mut ws = ws_connect(addr0).await;
        let writer_id = WriterId::new();
        for i in 0..3 {
            let val = format!("b0-{}", i);
            let idx = {
                let mut h = history_b0.lock().await;
                h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
            };
            match tokio::time::timeout(
                Duration::from_secs(3),
                ws_produce(&mut ws, writer_id, i + 1, topic_id, &val),
            )
            .await
            {
                Ok(Ok(resp)) if resp.success => {
                    // This should NOT happen — broker 0 can't flush to S3
                    let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                    history_b0
                        .lock()
                        .await
                        .record_write_complete(idx, offset, true);
                }
                _ => {
                    history_b0
                        .lock()
                        .await
                        .record_write_complete(idx, None, false);
                    // Reconnect for next attempt
                    if let Ok((w, _)) = connect_async(format!("ws://{}", addr0)).await {
                        ws = w;
                    }
                }
            }
        }
    });

    // Broker 1: writes should succeed
    let history_b1 = history.clone();
    let addr1 = cluster.broker(1).addr();
    let b1_handle = tokio::spawn(async move {
        let mut ws = ws_connect(addr1).await;
        let writer_id = WriterId::new();
        for i in 0..5 {
            let val = format!("b1-{}", i);
            let idx = {
                let mut h = history_b1.lock().await;
                h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
            };
            match ws_produce(&mut ws, writer_id, i + 1, topic_id, &val).await {
                Ok(resp) if resp.success => {
                    let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                    history_b1
                        .lock()
                        .await
                        .record_write_complete(idx, offset, true);
                }
                _ => {
                    history_b1
                        .lock()
                        .await
                        .record_write_complete(idx, None, false);
                }
            }
        }
    });

    b0_handle.await.unwrap();
    b1_handle.await.unwrap();

    // Heal broker 0
    cluster.broker_store(0).heal_partition();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Final read
    let mut ws = ws_connect(cluster.broker(1).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id).await.expect("final read");
    {
        let mut h = history.lock().await;
        let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
        h.record_read_complete(idx, values, hwm, true);
    }

    let h = history.lock().await;
    // Broker 0 should not have acked anything it couldn't commit
    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_no_duplicates()
        .expect("INVARIANT: no duplicate records");
    h.verify_write_write_causal()
        .expect("INVARIANT: write-write causal ordering");
    h.verify_poll_contiguity()
        .expect("INVARIANT: sequential reads don't skip offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no unacked values in reads");
}

/// 2 brokers. Close broker 0's DB pool. Broker 0's writes fail.
/// Broker 1 continues normally. Heal broker 0. All broker 1 acked writes visible.
#[tokio::test]
async fn test_db_partition_one_broker_continues() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-part").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Write baseline via both
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    let resp = ws_produce(&mut ws0, w0, 1, topic_id, "b0-pre")
        .await
        .expect("baseline b0");
    assert!(resp.success);

    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    let resp = ws_produce(&mut ws1, w1, 1, topic_id, "b1-pre")
        .await
        .expect("baseline b1");
    assert!(resp.success);

    // Partition broker 0 from DB
    drop(ws0);
    cluster.partition_broker_db(0).await;

    // Broker 1 should continue normally
    let mut acked_b1 = vec![Bytes::from("b1-pre")];
    for i in 1..6 {
        let val = format!("b1-{}", i);
        let resp = ws_produce(&mut ws1, w1, (i + 1) as u64, topic_id, &val)
            .await
            .expect("broker 1 write");
        assert!(
            resp.success,
            "broker 1 should continue after broker 0 DB partition"
        );
        acked_b1.push(Bytes::from(val));
    }

    // Heal broker 0
    cluster.heal_broker_db(0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all broker 1 acked writes visible
    let (values, _) = ws_read_all(&mut ws1, topic_id).await.expect("final read");
    for v in &acked_b1 {
        assert!(values.contains(v), "broker 1 acked value {:?} missing", v);
    }

    // No duplicate offsets
    let unique: HashSet<&Bytes> = values.iter().collect();
    assert_eq!(values.len(), unique.len(), "no duplicate values");
}

/// Write + delay S3 put so flush is mid-S3-write. Close broker 0's pool.
/// S3 put succeeds but DB commit fails. Broker reports failure. After heal, retry succeeds.
#[tokio::test]
async fn test_db_partition_during_flush() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-flush").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Add S3 delay so flush takes time
    cluster.broker_store(0).set_put_delay_ms(1000);

    // Start a write that will be in-flight when DB dies
    let addr0 = cluster.broker(0).addr();
    let write_handle = tokio::spawn(async move {
        let mut ws = ws_connect(addr0).await;
        let writer_id = WriterId::new();
        ws_produce(&mut ws, writer_id, 1, topic_id, "inflight-val").await
    });

    // Wait for S3 put to start, then kill DB
    tokio::time::sleep(Duration::from_millis(200)).await;
    cluster.partition_broker_db(0).await;

    // The write should fail (DB commit fails after S3 put)
    let result = tokio::time::timeout(Duration::from_secs(10), write_handle)
        .await
        .expect("write task should complete");
    match result {
        Ok(Ok(resp)) => {
            // Write may succeed or fail depending on timing
            // If it failed, that's the expected path
            if resp.success {
                eprintln!("  [db-flush] write succeeded before DB partition took effect");
            }
        }
        Ok(Err(_)) | Err(_) => {
            // Expected: write failed
        }
    }

    // Heal broker 0
    cluster.broker_store(0).set_put_delay_ms(0);
    cluster.heal_broker_db(0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Retry via healed broker 0
    let mut ws = ws_connect(cluster.broker(0).addr()).await;
    let w = WriterId::new();
    let resp = ws_produce(&mut ws, w, 1, topic_id, "after-heal")
        .await
        .expect("write after heal");
    assert!(resp.success, "write should succeed after healing");

    // Verify data is consistent
    let (values, hwm) = ws_read_all(&mut ws, topic_id).await.expect("final read");
    assert!(
        values.contains(&Bytes::from("after-heal")),
        "post-heal value should be visible"
    );
    assert_eq!(
        values.len() as u64,
        hwm.0,
        "offsets should be contiguous (no gaps)"
    );
}

/// Write via broker 0, immediately read via broker 1. Cross-broker read-after-write
/// consistency: since both share the same DB + S3, reads from broker 1 must see
/// committed writes from broker 0.
#[tokio::test]
async fn test_write_broker_a_read_broker_b_immediately() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("cross-read").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Write 5 records via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let writer_id = WriterId::new();
    let mut expected_values = Vec::new();
    for i in 0..5 {
        let val = format!("cross-{}", i);
        let resp = ws_produce(&mut ws0, writer_id, i + 1, topic_id, &val)
            .await
            .expect("write via broker 0");
        assert!(resp.success, "write {} should succeed", i);
        expected_values.push(Bytes::from(val));
    }

    // Immediately read via broker 1 from offset 0
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws1, topic_id)
        .await
        .expect("read via broker 1 should succeed");

    // All records written via broker 0 must be visible via broker 1
    for v in &expected_values {
        assert!(
            values.contains(v),
            "Value {:?} written via broker 0 must be readable via broker 1",
            v
        );
    }

    // Watermark must reflect all committed writes
    assert!(hwm.0 >= 5, "high watermark should be >= 5, got {}", hwm.0);

    // Offsets contiguous
    assert_eq!(
        values.len() as u64,
        hwm.0,
        "offsets should be contiguous: {} records but watermark is {}",
        values.len(),
        hwm.0
    );
}

/// L2: Cross-broker reader group reconnect.
/// 2-broker cluster. Reader connects to broker A. Crash broker A.
/// Reader reconnects to broker B, calls join_group. Verify: committed
/// offsets are preserved (state is in Postgres).
/// Invariant: reader group state is in Postgres, not broker memory.
#[tokio::test]
async fn test_cross_broker_reader_group_reconnect() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("cross-reconnect").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Reader A joins via broker 0
    let mut ws_a = ws_connect(cluster.broker(0).addr()).await;
    let join_req = reader::JoinGroupRequest {
        group_id: "xr-group".to_string(),
        topic_ids: vec![topic_id],
        reader_id: "reader-a".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::JoinGroup(join_req), 8192);
    ws_a.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_a.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let join_resp = ws_helpers::decode_join_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(join_resp.success, "join via broker 0 should succeed");

    // Produce some records via broker 0 so there's work to dispatch
    let writer_id = WriterId::new();
    let produce_resp = ws_produce(&mut ws_a, writer_id, 1, topic_id, "test-value")
        .await
        .expect("produce");
    assert!(produce_resp.success, "produce via broker 0 should succeed");

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Poll for work and commit the dispatched range via broker 0
    let poll_req = reader::PollRequest {
        group_id: "xr-group".to_string(),
        topic_id,
        reader_id: "reader-a".to_string(),
        max_bytes: 1024 * 1024,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Poll(poll_req), 8192);
    ws_a.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_a.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let poll_resp = ws_helpers::decode_poll_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(poll_resp.success, "poll via broker 0 should succeed");

    let commit_req = reader::CommitRequest {
        group_id: "xr-group".to_string(),
        reader_id: "reader-a".to_string(),
        topic_id,
        start_offset: poll_resp.start_offset,
        end_offset: poll_resp.end_offset,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Commit(commit_req), 8192);
    ws_a.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_a.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let commit_resp = ws_helpers::decode_commit_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(commit_resp.success, "commit via broker 0 should succeed");

    // Crash broker 0
    drop(ws_a);
    cluster.crash_broker(0).await;

    // Reader reconnects to broker 1
    let mut ws_b = ws_connect(cluster.broker(1).addr()).await;

    // Leave and rejoin via broker 1 to simulate reconnect
    let leave_req = reader::LeaveGroupRequest {
        group_id: "xr-group".to_string(),
        topic_id,
        reader_id: "reader-a".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::LeaveGroup(leave_req), 8192);
    ws_b.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_b.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    match ws_helpers::decode_server_frame(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    }) {
        flux_wire::ServerMessage::LeaveGroup(r) => {
            assert!(r.success, "leave via broker 1 should succeed");
        }
        other => panic!("expected leave response, got {:?}", other),
    }

    // Rejoin via broker 1
    let join_req2 = reader::JoinGroupRequest {
        group_id: "xr-group".to_string(),
        topic_ids: vec![topic_id],
        reader_id: "reader-a".to_string(),
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::JoinGroup(join_req2), 8192);
    ws_b.send(Message::Binary(buf)).await.unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(5), ws_b.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let join_resp2 = ws_helpers::decode_join_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(join_resp2.success, "rejoin via broker 1 should succeed");

    // Verify: committed watermark should be preserved in DB
    let db_watermark: Option<i64> = sqlx::query_scalar(
        "SELECT committed_watermark FROM reader_group_state \
         WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("xr-group")
    .bind(topic_id.0 as i32)
    .fetch_optional(cluster.pool())
    .await
    .expect("query");
    assert!(
        db_watermark.is_some() && db_watermark.unwrap() > 0,
        "committed watermark should persist in DB across broker crash, got {:?}",
        db_watermark
    );
}

/// L3: Sustained S3 impairment with multi-broker writes.
/// 2-broker cluster. Partition S3 puts on broker A only. Broker B continues.
/// After 5s, heal broker A. No duplicate offsets, all acked writes readable.
/// Invariant: asymmetric S3 faults don't cause cross-broker offset conflicts.
#[tokio::test]
async fn test_sustained_s3_impairment_multi_broker() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("sustained-s3").await as u32);

    let cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Write baseline data via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws0, w0, i + 1, topic_id, &format!("baseline-{}", i))
            .await
            .expect("baseline write");
        assert!(resp.success);
    }

    // Partition broker 0 from S3 (puts only)
    cluster.broker_store(0).partition_puts();

    // Broker B continues writing for ~3 seconds
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    let mut acked_b1 = Vec::new();
    for i in 0..10 {
        let val = format!("b1-sustained-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("broker 1 write should succeed");
        assert!(resp.success, "broker 1 write {} should succeed", i);
        acked_b1.push(Bytes::from(val));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Broker A writes should hang during this period
    let w0b = WriterId::new();
    let hung = tokio::time::timeout(
        Duration::from_secs(1),
        ws_produce(&mut ws0, w0b, 1, topic_id, "should-hang"),
    )
    .await;
    assert!(
        hung.is_err(),
        "broker 0 write should hang while S3 partitioned"
    );

    // Heal broker A
    cluster.broker_store(0).heal_partition();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read all data via broker 1
    let (values, hwm) = ws_read_all(&mut ws1, topic_id)
        .await
        .expect("final read should succeed");

    // All broker 1 acked writes should be visible
    for v in &acked_b1 {
        assert!(
            values.contains(v),
            "broker 1 acked value {:?} should be visible",
            v
        );
    }

    // No duplicate offsets: values count == watermark (contiguous)
    assert_eq!(
        values.len() as u64,
        hwm.0,
        "offsets should be contiguous: {} values but hwm {}",
        values.len(),
        hwm.0
    );

    // No duplicate values
    let unique: HashSet<&Bytes> = values.iter().collect();
    assert_eq!(
        values.len(),
        unique.len(),
        "no duplicate values from both brokers"
    );
}
