// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Backpressure behavior tests.
//!
//! Verifies that the broker returns ERR_BACKPRESSURE when the buffer
//! exceeds high water mark, and that backpressure clears after flush drains.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Message};

use flux_broker::buffer::BufferConfig;
use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, ERR_BACKPRESSURE, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

fn make_append(writer_id: WriterId, seq: u64, topic_id: TopicId, payload_bytes: usize) -> Vec<u8> {
    let value = Bytes::from(vec![b'x'; payload_bytes]);
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(seq),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record { key: None, value }],
        }],
    };
    ws_helpers::encode_client_frame(ClientMessage::Append(req), payload_bytes + 4096)
}

async fn recv_append_response(ws: &mut Ws) -> writer::AppendResponse {
    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("ws error");
    let data = match msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    };
    ws_helpers::decode_produce_response(&data)
}

/// When buffer exceeds high water mark, subsequent appends get ERR_BACKPRESSURE.
#[tokio::test]
async fn test_backpressure_returns_err_backpressure_code() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("bp-test-1").await as u32);

    let buffer_config = BufferConfig {
        max_size_bytes: 1024,
        max_wait: Duration::from_millis(200),
        high_water_bytes: 512,
        low_water_bytes: 256,
        ..Default::default()
    };

    let broker = CrashableWsBroker::start_with_buffer_config(db.pool.clone(), buffer_config).await;

    // Partition S3 puts so flush can't drain the buffer
    broker.faulty_store().partition_puts();

    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();

    // Send a large append that will exceed the high water mark (512 bytes)
    let buf = make_append(writer_id, 1, topic_id, 600);
    ws.send(Message::Binary(buf)).await.unwrap();

    // Wait for flush loop to process and set backpressure
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second append should get backpressure error
    let w2 = WriterId::new();
    let buf = make_append(w2, 1, topic_id, 100);
    ws.send(Message::Binary(buf)).await.unwrap();

    let resp = recv_append_response(&mut ws).await;
    assert!(!resp.success, "should be rejected with backpressure");
    assert_eq!(
        resp.error_code, ERR_BACKPRESSURE,
        "error code should be ERR_BACKPRESSURE"
    );
}

/// Backpressure clears after flush successfully drains buffer below low water mark.
#[tokio::test]
async fn test_backpressure_clears_after_flush_drains() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("bp-test-2").await as u32);

    let buffer_config = BufferConfig {
        max_size_bytes: 1024,
        max_wait: Duration::from_millis(200),
        high_water_bytes: 512,
        low_water_bytes: 256,
        ..Default::default()
    };

    let broker = CrashableWsBroker::start_with_buffer_config(db.pool.clone(), buffer_config).await;

    // Partition S3 puts so flush can't drain
    broker.faulty_store().partition_puts();

    let mut ws = ws_connect(broker.addr()).await;
    let writer_id = WriterId::new();

    // Trigger backpressure: send large payload
    let buf = make_append(writer_id, 1, topic_id, 600);
    ws.send(Message::Binary(buf)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify backpressure is active
    let w2 = WriterId::new();
    let buf = make_append(w2, 1, topic_id, 100);
    ws.send(Message::Binary(buf)).await.unwrap();
    let resp = recv_append_response(&mut ws).await;
    assert!(!resp.success, "should be under backpressure");
    assert_eq!(resp.error_code, ERR_BACKPRESSURE);

    // Heal S3 partition — flush can now drain
    broker.faulty_store().heal_partition();

    // Wait for flush to drain and release backpressure
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Third append should succeed now
    let w3 = WriterId::new();
    let buf = make_append(w3, 1, topic_id, 100);
    ws.send(Message::Binary(buf)).await.unwrap();

    let resp = recv_append_response(&mut ws).await;
    assert!(
        resp.success,
        "append should succeed after backpressure clears: error_code={}, msg={}",
        resp.error_code, resp.error_message
    );
}
