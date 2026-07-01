// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired network partition tests.
//!
//! Tests true network partitions (hanging I/O via black-hole mode) and
//! process pause equivalents. Verifies no false acks, no data loss,
//! and correct recovery after partition heals.

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

/// Partition S3 puts. Produce doesn't ack within 2s. Heal -> ack arrives.
#[tokio::test]
async fn test_s3_partition_hangs_put_no_ack() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("partition-hang").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Partition S3 puts
    broker.faulty_store().partition_puts();

    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();

    // Send produce request
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("partition-test"),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    // Ack should NOT arrive within 2s (put is hanging)
    let short = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    assert!(short.is_err(), "Should NOT get ack while S3 is partitioned");

    // Heal partition
    broker.faulty_store().heal_partition();

    // Now ack should arrive
    let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
        .await
        .expect("Should get ack after healing")
        .expect("stream not closed")
        .expect("no ws error");
    let resp = ws_helpers::decode_produce_response(match &msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    });
    assert!(resp.success, "Write should succeed after partition heals");

    // Verify record visible
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(values.contains(&Bytes::from("partition-test")));
}

/// Multiple producers writing. Partition S3 midway. No false acks.
/// All acked writes visible after healing.
#[tokio::test]
async fn test_s3_partition_concurrent_writes() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("partition-concurrent").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let store = broker.faulty_store().clone();
    let history = OperationHistory::shared();

    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Producers
    let mut handles = vec![];
    for p in 0..3 {
        let history = history.clone();
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
                let val = format!("p{}-s{}", p, seq);

                if ws.is_none() {
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

                match ws_produce_timeout(w, writer_id, seq, topic_id, &val, Duration::from_secs(5))
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
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // Let producers run for 2s
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Partition S3
    eprintln!("  [partition] partitioning S3 puts");
    store.partition_puts();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Heal partition
    eprintln!("  [partition] healing partition");
    store.heal_partition();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop workers
    let _ = stop_tx.send(());
    for h in handles {
        let _ = h.await;
    }

    // Final read
    store.reset();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(addr).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("final read should succeed");
    let mut h = history.lock().await;
    let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
    h.record_read_complete(idx, values, hwm, true);

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible after partition");
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

/// Partition puts only (gets still work). Existing data readable.
/// New writes hang. Consumers unaffected by producer-side partition.
#[tokio::test]
async fn test_asymmetric_partition_reads_ok() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("asymmetric").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write some data first
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("pre-{}", i))
            .await
            .expect("pre-partition write");
        assert!(resp.success);
    }

    // Partition puts only (gets still work)
    broker.faulty_store().partition_puts();

    // Reads should still work
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("reads should work during put partition");
    assert_eq!(values.len(), 5, "all pre-partition data should be readable");

    // Verify reads work on a fresh connection too (consumers truly unaffected)
    let mut ws_reader = ws_connect(addr).await;
    let (reader_values, _) = ws_read_all(&mut ws_reader, topic_id)
        .await
        .expect("consumer reads should work during put partition");
    assert_eq!(
        reader_values.len(),
        5,
        "consumer on separate connection should read all pre-partition data"
    );

    // New writes should hang (timeout)
    let w2 = WriterId::new();
    let result = ws_produce_timeout(
        &mut ws,
        w2,
        1,
        topic_id,
        "during-partition",
        Duration::from_secs(2),
    )
    .await;
    assert!(result.is_err(), "write should timeout during partition");

    // Heal and verify
    broker.faulty_store().heal_partition();
}

/// Write data -> partition S3 -> attempt writes (timeout) -> heal -> retry -> verify.
#[tokio::test]
async fn test_partition_heal_and_recovery() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("heal-recover").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write initial data
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "before-partition")
        .await
        .expect("initial write");
    assert!(resp.success);

    // Partition S3
    broker.faulty_store().partition_puts();

    // Attempt writes — they should time out
    let w2 = WriterId::new();
    let result = ws_produce_timeout(
        &mut ws,
        w2,
        1,
        topic_id,
        "during-partition",
        Duration::from_secs(2),
    )
    .await;
    assert!(result.is_err(), "write should timeout during partition");

    // Heal partition
    broker.faulty_store().heal_partition();

    // Reconnect and retry
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let w3 = WriterId::new();
    let resp = ws_produce(&mut ws, w3, 1, topic_id, "after-heal")
        .await
        .expect("write after heal");
    assert!(resp.success, "write should succeed after healing");

    // Verify all acked data visible and watermark monotonicity
    // Read in stages to track watermark progression
    let mut watermarks = Vec::new();
    let mut next_offset = Offset(0);
    loop {
        let resp = ws_read(&mut ws, topic_id, next_offset).await.expect("read");
        assert!(resp.success, "read should succeed");
        let mut got = false;
        for r in &resp.results {
            watermarks.push(r.high_watermark);
            if !r.records.is_empty() {
                got = true;
                next_offset = Offset(next_offset.0 + r.records.len() as u64);
            }
        }
        if !got {
            break;
        }
    }

    // Verify watermarks are monotonically non-decreasing
    for w in watermarks.windows(2) {
        assert!(
            w[1].0 >= w[0].0,
            "watermark should be monotonically non-decreasing: {} -> {}",
            w[0].0,
            w[1].0
        );
    }

    // Re-read all values to verify content
    let (values, hwm) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");
    assert!(
        values.contains(&Bytes::from("before-partition")),
        "pre-partition data visible"
    );
    assert!(
        values.contains(&Bytes::from("after-heal")),
        "post-heal data visible"
    );
    assert!(hwm.0 >= 2, "watermark should reflect at least 2 records");
    assert_eq!(
        values.len() as u64,
        hwm.0,
        "records count should match watermark (contiguous offsets)"
    );
}

/// Write 10 records. Partition puts (simulates pause). Sleep 3s. Heal. More writes.
/// All data visible, no gaps.
#[tokio::test]
async fn test_pause_resume_correct_state() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pause-resume").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let acked_values = Arc::new(Mutex::new(Vec::<Bytes>::new()));

    // Write 10 records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..10 {
        let val = format!("pre-pause-{}", i);
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &val)
            .await
            .expect("pre-pause write");
        assert!(resp.success);
        acked_values.lock().await.push(Bytes::from(val));
    }

    // Partition puts (simulates process pause)
    broker.faulty_store().partition_puts();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Heal
    broker.faulty_store().heal_partition();
    drop(ws);

    // More writes
    let mut ws = ws_connect(addr).await;
    let w2 = WriterId::new();
    for i in 0..5 {
        let val = format!("post-pause-{}", i);
        let resp = ws_produce(&mut ws, w2, i + 1, topic_id, &val)
            .await
            .expect("post-pause write");
        assert!(resp.success);
        acked_values.lock().await.push(Bytes::from(val));
    }

    // Verify all data visible
    let (values, _) = ws_read_all(&mut ws, topic_id)
        .await
        .expect("read should succeed");

    let acked = acked_values.lock().await;
    assert_eq!(values.len(), 15, "all 15 records should be visible");
    for v in acked.iter() {
        assert!(values.contains(v), "acked value {:?} missing", v);
    }
}

/// Start writes, pause mid-flush via black-hole. Resume. Flush completes.
/// No duplicate offsets (validates single-in-flight-flush invariant).
#[tokio::test]
async fn test_pause_during_flush_no_duplicate_offsets() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pause-flush").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let store = broker.faulty_store().clone();

    // Add slow S3 so we can partition mid-flush
    store.set_put_delay_ms(500);

    let acked_offsets = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));

    // Start writes
    let offsets = acked_offsets.clone();
    let writer_handle = tokio::spawn(async move {
        let mut ws = ws_connect(addr).await;
        let writer_id = WriterId::new();
        for i in 0..10 {
            let val = format!("flush-pause-{}", i);
            match ws_produce_timeout(
                &mut ws,
                writer_id,
                i + 1,
                topic_id,
                &val,
                Duration::from_secs(30),
            )
            .await
            {
                Ok(resp) if resp.success => {
                    for ack in &resp.append_acks {
                        offsets
                            .lock()
                            .await
                            .push((ack.start_offset.0, ack.end_offset.0));
                    }
                }
                _ => {
                    // Reconnect
                    ws = ws_connect(addr).await;
                }
            }
        }
    });

    // Briefly pause mid-flush
    tokio::time::sleep(Duration::from_millis(200)).await;
    store.partition_puts();
    tokio::time::sleep(Duration::from_secs(1)).await;
    store.heal_partition();

    writer_handle.await.unwrap();

    // Verify no duplicate offsets
    let ranges = acked_offsets.lock().await;
    let mut all_offsets: Vec<u64> = Vec::new();
    for &(start, end) in ranges.iter() {
        for o in start..end {
            all_offsets.push(o);
        }
    }
    let unique: std::collections::HashSet<u64> = all_offsets.iter().copied().collect();
    assert_eq!(
        all_offsets.len(),
        unique.len(),
        "No duplicate offsets after pause-during-flush"
    );
}
