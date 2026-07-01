// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Read-path fault tolerance tests.
//!
//! Verifies that S3 failures and data corruption during reads produce
//! clean errors rather than panics, hangs, or silent data loss.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Message};

use flux_broker::ObjectStore;
use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, ERR_INTERNAL_ERROR, reader, writer};

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
) -> writer::AppendResponse {
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
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for append response")
        .expect("stream closed")
        .expect("ws error");
    let data = match msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    };
    ws_helpers::decode_produce_response(&data)
}

async fn ws_read(ws: &mut Ws, topic_id: TopicId, offset: Offset) -> reader::ReadResponse {
    let req = reader::ReadRequest {
        topic_id,
        offset,
        max_bytes: 10 * 1024 * 1024,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for read response")
        .expect("stream closed")
        .expect("ws error");
    let data = match msg {
        Message::Binary(d) => d,
        _ => panic!("expected binary"),
    };
    ws_helpers::decode_read_response(&data)
}

/// S3 get_range failure during read must return a clean error, not panic or hang.
#[tokio::test]
async fn test_read_returns_error_on_s3_get_range_failure() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("read-fault-1").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;

    // Write 5 records and wait for acks
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("v{}", i)).await;
        assert!(resp.success, "write {} should succeed", i);
    }

    // Inject get_range fault
    broker.faulty_store().fail_next_get_range();

    // Read should return an error, not hang
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    assert!(
        !resp.success,
        "read should fail with injected get_range fault"
    );
    assert_eq!(resp.error_code, ERR_INTERNAL_ERROR);
    assert_eq!(resp.error_message, "read failed");
}

/// Broker recovers from transient S3 read failure — post-heal reads succeed.
#[tokio::test]
async fn test_read_succeeds_after_s3_partition_heals() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("read-fault-2").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;

    // Write records
    let writer_id = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("v{}", i)).await;
        assert!(resp.success);
    }

    // Partition gets — reads will hang
    broker.faulty_store().partition_gets();

    // Spawn a read that should hang
    let addr = broker.addr();
    let hung_read = tokio::spawn(async move {
        let mut ws2 = ws_connect(addr).await;
        ws_read(&mut ws2, topic_id, Offset(0)).await
    });

    // Wait a bit, then heal
    tokio::time::sleep(Duration::from_millis(500)).await;
    broker.faulty_store().heal_partition();

    // Send a fresh read on the original connection — should succeed
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    assert!(resp.success, "post-heal read should succeed");
    let record_count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(record_count, 3, "should see all 3 records after heal");

    // The hung read should also eventually complete (success or error, but not hang)
    let hung_result = tokio::time::timeout(Duration::from_secs(5), hung_read)
        .await
        .expect("hung read should complete after heal")
        .expect("task should not panic");
    // After heal, the hung read might succeed or fail depending on timing — either is acceptable
    // The key invariant is that it doesn't hang forever
    let _ = hung_result;
}

/// Corrupted FL data returns a clean error, not a panic. Broker stays alive.
#[tokio::test]
async fn test_read_corrupt_fl_returns_error_not_panic() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("read-fault-corrupt").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;

    // Write a record to get a FL file on disk
    let writer_id = WriterId::new();
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "corrupt-me").await;
    assert!(resp.success);

    // Find and completely corrupt every FL file on disk
    let store = broker.faulty_store().inner();
    let keys = store.list("data/").await.expect("list should succeed");
    assert!(!keys.is_empty(), "should have at least one FL file");

    for key in &keys {
        let data = store.get(key).await.expect("get should succeed");
        // Overwrite every byte — ensures any get_range hits garbage
        let corrupted = vec![0xDE; data.len()];
        store
            .put(key, Bytes::from(corrupted))
            .await
            .expect("put should succeed");
    }

    // Read from corrupted topic — should get clean error
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    assert!(!resp.success, "read of corrupted data should fail cleanly");
    assert_eq!(resp.error_code, ERR_INTERNAL_ERROR);

    // Broker should still be alive — can we still write new records?
    let w2 = WriterId::new();
    let mut ws2 = ws_connect(broker.addr()).await;
    let resp = ws_produce(&mut ws2, w2, 1, topic_id, "after-corrupt").await;
    assert!(
        resp.success,
        "broker should still accept writes after corrupt read"
    );
}

/// Write batch, confirm ack. Inject get_range fault → read fails.
/// Heal → retry read returns the committed data.
/// Invariant: read failure never returns stale/corrupt data; retry after heal succeeds.
#[tokio::test]
async fn test_stale_s3_read_after_batch_index_update() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("read-fault-stale").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let mut ws = ws_connect(broker.addr()).await;

    // Write 3 records and confirm acks
    let writer_id = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("v{}", i)).await;
        assert!(resp.success, "write {} should succeed", i);
    }

    // Inject get_range fault — next read will fail
    broker.faulty_store().fail_next_get_range();

    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    assert!(
        !resp.success,
        "read should fail with injected get_range fault"
    );
    assert_eq!(resp.error_code, ERR_INTERNAL_ERROR);

    // Fault consumed — heal is implicit. Retry should succeed with correct data.
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;
    assert!(resp.success, "retry after heal should succeed");
    let record_count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(
        record_count, 3,
        "retry should return all 3 committed records"
    );

    // Verify values are correct (not stale/corrupt)
    let values: Vec<Bytes> = resp
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    for i in 0..3 {
        assert!(
            values.contains(&Bytes::from(format!("v{}", i))),
            "value v{} should be present after retry",
            i
        );
    }
}
