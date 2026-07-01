// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired partial commit tests.
//!
//! Verifies that partial failures across topics don't create cross-topic
//! inconsistency. When a flush fails for one topic, other topics that
//! already succeeded remain consistent.

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

/// Write to topics 0, 1, 2 in quick succession. Inject fail_on_put_n(2) so
/// the second flush fails. Verify: topic 0 data is readable if acked,
/// topics 1/2 writes are not acked, offsets are consistent.
/// Invariant: partial failures across topics don't create cross-topic inconsistency.
#[tokio::test]
async fn test_multi_topic_partial_commit() {
    let db = TestDb::new().await;
    let topic_id_0 = TopicId(db.create_topic("partial-commit-0").await as u32);
    let topic_id_1 = TopicId(db.create_topic("partial-commit-1").await as u32);
    let topic_id_2 = TopicId(db.create_topic("partial-commit-2").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Fail from the 2nd S3 put onward
    broker.faulty_store().fail_on_put_n(2);

    // Write to topic 0 — first put succeeds
    let mut ws0 = ws_connect(addr).await;
    let w0 = WriterId::new();
    let resp0 = ws_produce(&mut ws0, w0, 1, topic_id_0, "t0-val").await;

    // Write to topic 1 — second put fails
    let mut ws1 = ws_connect(addr).await;
    let w1 = WriterId::new();
    let resp1 = ws_produce(&mut ws1, w1, 1, topic_id_1, "t1-val").await;

    // Write to topic 2 — also fails
    let mut ws2 = ws_connect(addr).await;
    let w2 = WriterId::new();
    let resp2 = ws_produce(&mut ws2, w2, 1, topic_id_2, "t2-val").await;

    // Reset faults for reads
    broker.faulty_store().reset();

    // Topic 0 should have succeeded
    let t0_acked = resp0.as_ref().map(|r| r.success).unwrap_or(false);

    // Topics 1 and 2 should have failed
    let t1_failed = resp1.is_err() || !resp1.as_ref().unwrap().success;
    let t2_failed = resp2.is_err() || !resp2.as_ref().unwrap().success;
    assert!(t1_failed, "topic 1 write should fail (2nd S3 put fails)");
    assert!(t2_failed, "topic 2 write should fail (3rd S3 put fails)");

    // If topic 0 was acked, verify it's readable
    if t0_acked {
        let mut ws = ws_connect(addr).await;
        let (values, hwm) = ws_read_all(&mut ws, topic_id_0)
            .await
            .expect("topic 0 read should succeed");
        assert!(
            values.contains(&Bytes::from("t0-val")),
            "topic 0 acked value should be readable"
        );
        assert_eq!(
            hwm.0,
            values.len() as u64,
            "topic 0 offsets should be consistent"
        );
    }

    // Verify topics 1 and 2 have no committed data (or empty)
    for (idx, tid) in [(1, topic_id_1), (2, topic_id_2)] {
        let watermark: i64 = sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
        )
        .bind(tid.0 as i32)
        .fetch_optional(&db.pool)
        .await
        .expect("query watermark")
        .unwrap_or(0);
        assert_eq!(
            watermark, 0,
            "topic {} watermark should be 0 (write failed)",
            idx
        );
    }

    // New writes to all topics should succeed after fault reset
    let mut ws = ws_connect(addr).await;
    for (idx, tid) in [(0, topic_id_0), (1, topic_id_1), (2, topic_id_2)] {
        let w = WriterId::new();
        let resp = ws_produce(&mut ws, w, 1, tid, &format!("recover-t{}", idx))
            .await
            .expect("recovery write should succeed");
        assert!(
            resp.success,
            "topic {} write should succeed after reset",
            idx
        );
    }
}

/// Write to two topics. One topic's flush fails.
/// Verify the successful topic's offsets are contiguous and consistent.
#[tokio::test]
async fn test_partial_commit_offset_consistency() {
    let db = TestDb::new().await;
    let topic_id_0 = TopicId(db.create_topic("partial-offset-0").await as u32);
    let topic_id_1 = TopicId(db.create_topic("partial-offset-1").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write 5 records to topic 0 (all should succeed)
    let mut ws = ws_connect(addr).await;
    let w0 = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, w0, i + 1, topic_id_0, &format!("t0-{}", i))
            .await
            .expect("t0 write should succeed");
        assert!(resp.success);
    }

    // Now fail S3 and write to topic 1
    broker.faulty_store().fail_on_put_n(1);
    let w1 = WriterId::new();
    let resp = ws_produce(&mut ws, w1, 1, topic_id_1, "t1-fail").await;
    let t1_failed = resp.is_err() || !resp.as_ref().unwrap().success;
    assert!(t1_failed, "topic 1 should fail with S3 fault");

    // Reset and verify topic 0 is fully consistent
    broker.faulty_store().reset();
    let mut ws = ws_connect(addr).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id_0)
        .await
        .expect("t0 read should succeed");
    assert_eq!(values.len(), 5, "topic 0 should have all 5 records");
    assert_eq!(hwm.0, 5, "topic 0 watermark should be 5");

    // Topic 1 failure should not have affected topic 0's consistency
    let unique: std::collections::HashSet<&Bytes> = values.iter().collect();
    assert_eq!(values.len(), unique.len(), "no duplicates in topic 0");
}
