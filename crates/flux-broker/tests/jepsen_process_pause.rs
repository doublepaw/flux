// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired process pause simulation tests.
//!
//! Simulates a full process pause by combining per-broker S3 black-hole +
//! DB pool close. When both S3 and DB are cut, the broker is alive but
//! can't do anything -- flush hangs, heartbeats fail, coordinator ops fail.

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
use common::{MultiBrokerCluster, TestDb};

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

/// Pause broker 0 (S3 partition + DB close). Reader A's heartbeats fail.
/// Expire A via SQL. B detects A expired, stale member removed.
/// Resume broker 0.
#[tokio::test]
async fn test_pause_broker_members_expire() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pause-expire").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Reader A joins via broker 0
    let mut ws_a = ws_connect(cluster.broker(0).addr()).await;
    let join_req_a = reader::JoinGroupRequest {
        group_id: "pause-group".to_string(),
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
        group_id: "pause-group".to_string(),
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

    // "Pause" broker 0: S3 partition + DB close
    drop(ws_a);
    cluster.broker_store(0).partition_puts();
    cluster.broker_store(0).partition_gets();
    cluster.partition_broker_db(0).await;

    // Simulate session timeout: expire reader-a's heartbeat
    sqlx::query(
        "UPDATE reader_members SET last_heartbeat = NOW() - interval '60 seconds' \
         WHERE group_id = $1 AND reader_id = $2",
    )
    .bind("pause-group")
    .bind("reader-a")
    .execute(&db.pool)
    .await
    .expect("expire reader-a");

    // Reader B heartbeat triggers stale member detection
    let hb_req = reader::HeartbeatRequest {
        group_id: "pause-group".to_string(),
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
    assert!(hb_resp.success, "B heartbeat should succeed");

    // Verify reader-a was expired from members
    let member_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reader_members WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("pause-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query members");
    assert_eq!(member_count, 1, "only reader-b should remain after expiry");

    // Resume broker 0
    cluster.broker_store(0).heal_partition();
    cluster.heal_broker_db(0).await;
}

/// 2 brokers. "Pause" broker 0. Writes via broker 0 hang.
/// Writes via broker 1 succeed. Resume. All acked data consistent.
#[tokio::test]
async fn test_pause_broker_writes_stall() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pause-stall").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;

    // Write baseline
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    let resp = ws_produce(&mut ws0, w0, 1, topic_id, "pre-pause")
        .await
        .expect("baseline write");
    assert!(resp.success);

    // "Pause" broker 0
    cluster.broker_store(0).partition_puts();
    cluster.broker_store(0).partition_gets();
    cluster.partition_broker_db(0).await;

    // Write via broker 0 should hang
    let hung = tokio::time::timeout(
        Duration::from_secs(2),
        ws_produce(&mut ws0, WriterId::new(), 1, topic_id, "hung"),
    )
    .await;
    assert!(hung.is_err(), "broker 0 write should hang while paused");

    // Write via broker 1 should succeed
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    let mut acked = vec![Bytes::from("pre-pause")];
    for i in 0..5 {
        let val = format!("b1-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("broker 1 write");
        assert!(resp.success);
        acked.push(Bytes::from(val));
    }

    // Resume broker 0
    cluster.broker_store(0).heal_partition();
    cluster.heal_broker_db(0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all acked writes visible
    let mut ws = ws_connect(cluster.broker(1).addr()).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id).await.expect("final read");
    for v in &acked {
        assert!(values.contains(v), "acked value {:?} missing", v);
    }
    assert_eq!(values.len() as u64, hwm.0, "contiguous offsets");
}

/// Write 10 via broker 0. Pause broker 0 for 5s. Write 10 via broker 1.
/// Resume broker 0. Write 5 more via broker 0. All 25 acked records visible.
#[tokio::test]
async fn test_pause_resume_no_data_loss() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("pause-nodataloss").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let acked = Arc::new(Mutex::new(Vec::<Bytes>::new()));

    // Write 10 via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0 = WriterId::new();
    for i in 0..10 {
        let val = format!("b0-pre-{}", i);
        let resp = ws_produce(&mut ws0, w0, i + 1, topic_id, &val)
            .await
            .expect("pre-pause write");
        assert!(resp.success);
        acked.lock().await.push(Bytes::from(val));
    }

    // Pause broker 0
    drop(ws0);
    cluster.broker_store(0).partition_puts();
    cluster.broker_store(0).partition_gets();
    cluster.partition_broker_db(0).await;

    // Write 10 via broker 1 during pause
    let mut ws1 = ws_connect(cluster.broker(1).addr()).await;
    let w1 = WriterId::new();
    for i in 0..10 {
        let val = format!("b1-during-{}", i);
        let resp = ws_produce(&mut ws1, w1, i + 1, topic_id, &val)
            .await
            .expect("during-pause write");
        assert!(resp.success);
        acked.lock().await.push(Bytes::from(val));
    }

    // Resume broker 0
    cluster.broker_store(0).heal_partition();
    cluster.heal_broker_db(0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write 5 more via broker 0
    let mut ws0 = ws_connect(cluster.broker(0).addr()).await;
    let w0b = WriterId::new();
    for i in 0..5 {
        let val = format!("b0-post-{}", i);
        let resp = ws_produce(&mut ws0, w0b, i + 1, topic_id, &val)
            .await
            .expect("post-pause write");
        assert!(resp.success);
        acked.lock().await.push(Bytes::from(val));
    }

    // Verify all 25 acked records visible
    let (values, hwm) = ws_read_all(&mut ws0, topic_id).await.expect("final read");

    let acked = acked.lock().await;
    assert_eq!(
        values.len(),
        acked.len(),
        "all {} acked records should be visible, got {}",
        acked.len(),
        values.len()
    );
    for v in acked.iter() {
        assert!(values.contains(v), "acked value {:?} missing", v);
    }
    assert_eq!(values.len() as u64, hwm.0, "no gaps in offsets");
}
