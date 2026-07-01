// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired Postgres failure tests.
//!
//! Verifies broker behavior when Postgres is unavailable, slow, or crashes.
//! Postgres is the SPOF for offset allocation, dedup, segment indexing, and
//! reader group coordination. A Postgres hiccup affects ALL brokers.
//!
//! Inspired by Jepsen's Bufstream findings where coordinator pauses caused
//! metastable failures (stuck consumers that never recovered).

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
use common::{CrashableWsBroker, DbBlocker, MultiBrokerCluster, OperationHistory, TestDb};

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

/// DbBlocker stalls flush (locks topic_offsets row). Writes timeout.
/// After unlock, writes succeed and all data is intact.
#[tokio::test]
async fn test_db_lock_stalls_flush_and_recovers() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-lock-stall").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write some records first
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(
            &mut ws,
            writer_id,
            i + 1,
            topic_id,
            &format!("pre-lock-{}", i),
        )
        .await
        .expect("pre-lock write");
        assert!(resp.success);
    }

    // Lock the topic_offsets row — stalls flush
    let mut blocker = DbBlocker::new(db.pool.clone());
    blocker.block_topic(topic_id.0 as i32).await;

    // Writes should timeout (flush can't commit)
    let w2 = WriterId::new();
    let result = ws_produce_timeout(
        &mut ws,
        w2,
        1,
        topic_id,
        "during-lock",
        Duration::from_secs(3),
    )
    .await;
    // The write may timeout or the broker may queue it
    eprintln!("  [db-lock] write during lock: {:?}", result.is_ok());

    // Unlock
    blocker.unblock().await;

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New writes should succeed
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let w3 = WriterId::new();
    let resp = ws_produce(&mut ws, w3, 1, topic_id, "after-unlock")
        .await
        .expect("post-unlock write");
    assert!(resp.success, "writes should succeed after DB lock released");

    // Verify all pre-lock records are intact
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    for i in 0..3 {
        let expected = Bytes::from(format!("pre-lock-{}", i));
        assert!(values.contains(&expected), "pre-lock record {} missing", i);
    }
}

/// Close DB pool → broker gets errors on flush → heal → broker recovers.
/// Uses MultiBrokerCluster for partition_broker_db/heal_broker_db.
#[tokio::test]
async fn test_db_partition_and_recovery() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-partition").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 1).await;
    let addr = cluster.broker(0).addr();

    // Write records before partition
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(
            &mut ws,
            writer_id,
            i + 1,
            topic_id,
            &format!("pre-part-{}", i),
        )
        .await
        .expect("pre-partition write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Partition broker from DB
    cluster.partition_broker_db(0).await;
    eprintln!("  [db-partition] DB partitioned");

    // Writes should fail or timeout
    drop(ws);
    // The broker is still running but can't reach DB
    // Give it a moment to notice the broken pool
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Heal: fresh pool + restart
    cluster.heal_broker_db(0).await;
    let addr = cluster.broker(0).addr();
    eprintln!("  [db-partition] DB healed, broker restarted at {}", addr);

    // Writes should work again
    let mut ws = ws_connect(addr).await;
    let w2 = WriterId::new();
    let resp = ws_produce(&mut ws, w2, 1, topic_id, "post-heal")
        .await
        .expect("post-heal write");
    assert!(resp.success, "writes should succeed after DB heal");

    // Verify all pre-partition records survived
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed after recovery");
    for i in 0..5 {
        let expected = Bytes::from(format!("pre-part-{}", i));
        assert!(
            values.contains(&expected),
            "pre-partition record {} missing",
            i
        );
    }
    assert!(
        values.contains(&Bytes::from("post-heal")),
        "post-heal record should be visible"
    );
}

/// Concurrent writers during DB lock → all either succeed after unlock or fail gracefully.
/// No duplicates, no data loss for acked writes.
#[tokio::test]
async fn test_concurrent_writers_during_db_lock() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("db-concurrent-lock").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let history = OperationHistory::shared();

    // Lock DB
    let mut blocker = DbBlocker::new(db.pool.clone());
    blocker.block_topic(topic_id.0 as i32).await;

    // Spawn concurrent writers
    let mut handles = vec![];
    for p in 0..3 {
        let history = history.clone();
        handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            if let Ok((mut ws, _)) = connect_async(format!("ws://{}", addr)).await {
                for i in 0..5 {
                    let val = format!("locked-p{}-s{}", p, i);
                    let idx = {
                        let mut h = history.lock().await;
                        h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
                    };
                    match ws_produce_timeout(
                        &mut ws,
                        writer_id,
                        (i + 1) as u64,
                        topic_id,
                        &val,
                        Duration::from_secs(8),
                    )
                    .await
                    {
                        Ok(resp) if resp.success => {
                            let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
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

    // Let writers attempt for a bit
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Unlock
    blocker.unblock().await;
    eprintln!("  [concurrent-lock] DB unlocked");

    // Wait for writers to finish
    for h in handles {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Final read
    let mut ws = ws_connect(addr).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read should succeed");

    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values, hwm, true);

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible after DB lock");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_no_duplicates()
        .expect("INVARIANT: no duplicate records");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no phantom writes");
}

/// DB partition on multi-broker cluster: one broker loses DB, other keeps working.
/// After heal, partitioned broker recovers and all data is consistent.
#[tokio::test]
async fn test_multi_broker_db_partition() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("multi-db-part").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let addr0 = cluster.broker(0).addr();
    let addr1 = cluster.broker(1).addr();
    let history = OperationHistory::shared();

    // Write to both brokers
    let mut ws0 = ws_connect(addr0).await;
    let mut ws1 = ws_connect(addr1).await;
    let w0 = WriterId::new();
    let w1 = WriterId::new();

    for i in 0..3 {
        let val = format!("b0-pre-{}", i);
        let idx = {
            let mut h = history.lock().await;
            h.record_write(w0, TopicId(0), Bytes::from(val.clone()))
        };
        let resp = ws_produce(&mut ws0, w0, (i + 1) as u64, topic_id, &val)
            .await
            .expect("broker0 pre-write");
        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
        history
            .lock()
            .await
            .record_write_complete(idx, offset, resp.success);
    }
    for i in 0..3 {
        let val = format!("b1-pre-{}", i);
        let idx = {
            let mut h = history.lock().await;
            h.record_write(w1, TopicId(0), Bytes::from(val.clone()))
        };
        let resp = ws_produce(&mut ws1, w1, (i + 1) as u64, topic_id, &val)
            .await
            .expect("broker1 pre-write");
        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
        history
            .lock()
            .await
            .record_write_complete(idx, offset, resp.success);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Partition broker 0 from DB
    cluster.partition_broker_db(0).await;
    eprintln!("  [multi-db-part] broker 0 partitioned from DB");

    // Broker 1 should still work
    let w1b = WriterId::new();
    let val = "b1-during-partition";
    let idx = {
        let mut h = history.lock().await;
        h.record_write(w1b, TopicId(0), Bytes::from(val))
    };
    let resp = ws_produce(&mut ws1, w1b, 1, topic_id, val)
        .await
        .expect("broker1 during partition");
    let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
    history
        .lock()
        .await
        .record_write_complete(idx, offset, resp.success);

    // Heal broker 0
    cluster.heal_broker_db(0).await;
    let addr0 = cluster.broker(0).addr();
    eprintln!("  [multi-db-part] broker 0 healed at {}", addr0);

    // Write to healed broker
    drop(ws0);
    let mut ws0 = ws_connect(addr0).await;
    let w0b = WriterId::new();
    let val = "b0-post-heal";
    let idx = {
        let mut h = history.lock().await;
        h.record_write(w0b, TopicId(0), Bytes::from(val))
    };
    let resp = ws_produce(&mut ws0, w0b, 1, topic_id, val)
        .await
        .expect("broker0 post-heal");
    let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
    history
        .lock()
        .await
        .record_write_complete(idx, offset, resp.success);

    // Final read from broker 1 (should see all data)
    drop(ws1);
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws1, topic_id)
        .await
        .expect("final read should succeed");

    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values, hwm, true);

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible after DB partition");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no phantom writes");
}

/// Slow S3 + DB contention simultaneously. Double pressure on flush pipeline.
#[tokio::test]
async fn test_slow_s3_plus_db_contention() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("slow-s3-db").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let history = OperationHistory::shared();

    // Slow S3 puts
    broker.faulty_store().set_put_delay_ms(300);

    // Lock DB for 2 seconds
    let pool = db.pool.clone();
    let tid = topic_id.0 as i32;
    let db_lock_handle = tokio::spawn(async move {
        let mut blocker = DbBlocker::new(pool);
        blocker.block_topic(tid).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        blocker.unblock().await;
    });

    // Concurrent writers during double pressure
    let mut handles = vec![];
    for p in 0..2 {
        let history = history.clone();
        handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            if let Ok((mut ws, _)) = connect_async(format!("ws://{}", addr)).await {
                for i in 0..5 {
                    let val = format!("double-p{}-s{}", p, i);
                    let idx = {
                        let mut h = history.lock().await;
                        h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
                    };
                    match ws_produce_timeout(
                        &mut ws,
                        writer_id,
                        (i + 1) as u64,
                        topic_id,
                        &val,
                        Duration::from_secs(15),
                    )
                    .await
                    {
                        Ok(resp) if resp.success => {
                            let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
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

    // Wait for everything
    db_lock_handle.await.unwrap();
    for h in handles {
        let _ = h.await;
    }

    // Reset S3 delays
    broker.faulty_store().reset();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Final read
    let mut ws = ws_connect(addr).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read should succeed");

    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values, hwm, true);

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible after double pressure");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no phantom writes");
}
