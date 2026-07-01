// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! End-to-end WebSocket write tests.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_write
//! ```

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, writer};

use common::TestDb;
use common::ws_helpers::*;

/// Test basic append and read via WebSocket.
#[tokio::test]
async fn test_produce_fetch_roundtrip() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("ws-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let writer_id = WriterId::new();
    let records = vec![
        Record {
            key: Some(Bytes::from("key-1")),
            value: Bytes::from(r#"{"msg":"hello"}"#),
        },
        Record {
            key: Some(Bytes::from("key-2")),
            value: Bytes::from(r#"{"msg":"world"}"#),
        },
    ];

    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: records.clone(),
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    let resp = decode_produce_response(&data);
    assert_eq!(resp.append_seq.0, 1);
    assert_eq!(resp.append_acks.len(), 1);
    assert_eq!(resp.append_acks[0].start_offset.0, 0);
    assert_eq!(resp.append_acks[0].end_offset.0, 2);

    // Now read the records
    let fetch_req = flux_wire::reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: flux_common::ids::Offset(0),
        max_bytes: 1024 * 1024,
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for read response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    let fetch_resp = decode_read_response(&data);
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 2);
    assert_eq!(fetch_resp.results[0].high_watermark.0, 2);
    assert_eq!(
        fetch_resp.results[0].records[0].key,
        Some(Bytes::from("key-1"))
    );
    assert_eq!(
        fetch_resp.results[0].records[1].key,
        Some(Bytes::from("key-2"))
    );

    ws.close(None).await.ok();
}

/// Test read response keeps schema IDs correct when topic contains mixed-schema batches.
#[tokio::test]
async fn test_fetch_returns_schema_homogeneous_topic_results() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("ws-mixed-schema-read").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let writer_id = WriterId::new();

    let first = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k-100")),
                value: Bytes::from(r#"{"v":100}"#),
            }],
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Append(first),
        8192,
    )))
    .await
    .unwrap();
    let first_resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");
    let first_data = match first_resp {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let first_resp = decode_produce_response(&first_data);
    assert!(first_resp.success);

    let second = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(2),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(101),
            records: vec![Record {
                key: Some(Bytes::from("k-101")),
                value: Bytes::from(r#"{"v":101}"#),
            }],
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Append(second),
        8192,
    )))
    .await
    .unwrap();
    let second_resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");
    let second_data = match second_resp {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let second_resp = decode_produce_response(&second_data);
    assert!(second_resp.success);

    let fetch_req = flux_wire::reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: flux_common::ids::Offset(0),
        max_bytes: 1024 * 1024,
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Read(fetch_req),
        8192,
    )))
    .await
    .unwrap();

    let fetch_response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");
    let fetch_data = match fetch_response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let fetch_resp = decode_read_response(&fetch_data);
    assert!(fetch_resp.success);
    assert_eq!(
        fetch_resp.results.len(),
        2,
        "expected one TopicResult per schema in mixed-schema read"
    );

    let mut schemas: Vec<u32> = fetch_resp.results.iter().map(|r| r.schema_id.0).collect();
    schemas.sort_unstable();
    assert_eq!(schemas, vec![100, 101]);
    assert!(fetch_resp.results.iter().all(|r| r.records.len() == 1));

    ws.close(None).await.ok();
}

/// Test that duplicate sequence dedup survives broker restart (cache miss path).
#[tokio::test]
async fn test_duplicate_seq_dedup_survives_broker_restart() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("dedup-restart-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr1, server_handle1) = start_server(db.pool.clone(), &temp_dir).await;
    let url1 = format!("ws://{}", addr1);
    let (mut ws1, _) = connect_async(&url1).await.expect("Failed to connect");

    let writer_id = WriterId::new();
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from("k1")),
                    value: Bytes::from(r#"{"msg":"first"}"#),
                },
                Record {
                    key: Some(Bytes::from("k2")),
                    value: Bytes::from(r#"{"msg":"second"}"#),
                },
            ],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws1.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws1.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };

    let first_resp = decode_produce_response(&data);
    assert_eq!(first_resp.append_seq.0, 1);
    assert!(first_resp.success);
    assert_eq!(first_resp.append_acks.len(), 1);

    let offset_after_first: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();

    let batch_count_after_first: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(batch_count_after_first, 1);

    let (last_seq, last_acks): (i64, serde_json::Value) =
        sqlx::query_as("SELECT last_seq, last_acks FROM writer_state WHERE writer_id = $1")
            .bind(writer_id.0)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(last_seq, 1);
    assert!(
        last_acks
            .as_array()
            .map(|arr| !arr.is_empty())
            .unwrap_or(false),
        "last_acks should be a non-empty array"
    );

    ws1.close(None).await.ok();

    server_handle1.abort();
    let _ = server_handle1.await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (addr2, server_handle2) = start_server(db.pool.clone(), &temp_dir).await;
    let url2 = format!("ws://{}", addr2);
    let (mut ws2, _) = connect_async(&url2).await.expect("Failed to connect");

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws2.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws2.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };

    let retry_resp = decode_produce_response(&data);
    assert_eq!(retry_resp.append_seq.0, 1);
    assert!(retry_resp.success);
    assert_eq!(retry_resp.append_acks.len(), 1);
    assert_eq!(retry_resp.append_acks[0], first_resp.append_acks[0]);

    let offset_after_retry: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(offset_after_retry, offset_after_first);

    let batch_count_after_retry: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(batch_count_after_retry, 1);

    ws2.close(None).await.ok();
    server_handle2.abort();
    let _ = server_handle2.await;
}

/// Test writer UUID first byte colliding with old tags does not affect union framing.
#[tokio::test]
async fn test_produce_uuid_first_byte_collision_is_safe() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("unprefixed-collision").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let mut producer_uuid = [0u8; 16];
    producer_uuid[0] = 5;
    let req = writer::AppendRequest {
        writer_id: WriterId::from_uuid(uuid::Uuid::from_bytes(producer_uuid)),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k")),
                value: Bytes::from("v"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let resp = decode_produce_response(&data);
    assert_eq!(resp.append_seq.0, 1);
    assert!(
        resp.success,
        "error {}: {}",
        resp.error_code, resp.error_message
    );
    assert_eq!(resp.append_acks.len(), 1);

    let next_offset: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(next_offset, 1);

    ws.close(None).await.ok();
}

/// Test that in-flight duplicate append_seq requests are coalesced to a single commit.
#[tokio::test]
async fn test_inflight_duplicate_seq_coalesced_single_commit() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("inflight-dup-coalesce").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect ws1");
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect ws2");

    let writer_id = WriterId::new();
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k")),
                value: Bytes::from("v"),
            }],
        }],
    };

    let buf1 = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws1.send(Message::Binary(buf1)).await.unwrap();

    let buf2 = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws2.send(Message::Binary(buf2)).await.unwrap();

    let resp1 = tokio::time::timeout(Duration::from_secs(5), ws1.next())
        .await
        .expect("Timeout ws1")
        .expect("No ws1 response")
        .expect("ws1 error");
    let resp2 = tokio::time::timeout(Duration::from_secs(5), ws2.next())
        .await
        .expect("Timeout ws2")
        .expect("No ws2 response")
        .expect("ws2 error");

    let data1 = match resp1 {
        Message::Binary(data) => data,
        _ => panic!("Expected binary ws1 response"),
    };
    let data2 = match resp2 {
        Message::Binary(data) => data,
        _ => panic!("Expected binary ws2 response"),
    };

    let r1 = decode_produce_response(&data1);
    let r2 = decode_produce_response(&data2);
    assert!(r1.success);
    assert!(r2.success);
    assert_eq!(r1.append_seq.0, 1);
    assert_eq!(r2.append_seq.0, 1);
    assert_eq!(r1.append_acks.len(), 1);
    assert_eq!(r2.append_acks.len(), 1);
    assert_eq!(r1.append_acks[0], r2.append_acks[0]);

    let next_offset: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(next_offset, 1);

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test multiple writers to same topic.
#[tokio::test]
async fn test_multiple_producers() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("multi-writer-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let mut handles = vec![];
    for i in 0..5 {
        let url = url.clone();

        let handle = tokio::spawn(async move {
            let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

            let writer_id = WriterId::new();
            let req = writer::AppendRequest {
                writer_id,
                append_seq: AppendSeq(1),
                batches: vec![RecordBatch {
                    topic_id: TopicId(topic_id as u32),
                    schema_id: SchemaId(100),
                    records: vec![Record {
                        key: Some(Bytes::from(format!("writer-{}", i))),
                        value: Bytes::from(format!(r#"{{"id":{}}}"#, i)),
                    }],
                }],
            };

            let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
            ws.send(Message::Binary(buf)).await.unwrap();

            let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("Timeout")
                .expect("No response")
                .expect("Error");

            let data = match response {
                Message::Binary(data) => data,
                _ => panic!("Expected binary"),
            };

            let resp = decode_produce_response(&data);
            assert!(resp.success);
            ws.close(None).await.ok();

            resp.append_acks[0].clone()
        });

        handles.push(handle);
    }

    let mut append_acks = vec![];
    for handle in handles {
        append_acks.push(handle.await.unwrap());
    }

    let total_offset: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(total_offset, 5);

    // Read all records to verify
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let fetch_req = flux_wire::reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: flux_common::ids::Offset(0),
        max_bytes: 1024 * 1024,
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let fetch_resp = decode_read_response(&data);
    assert!(fetch_resp.success);
    let total_records: usize = fetch_resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(total_records, 5);

    ws.close(None).await.ok();
}

/// Test read from specific offset.
#[tokio::test]
async fn test_fetch_from_offset() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("read-offset-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!(r#"{{"append_seq":{}}}"#, i)),
        })
        .collect();

    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let produce_resp = decode_produce_response(&data);
    assert!(produce_resp.success);

    // Read from offset 5 (should get records 5-9)
    let fetch_req = flux_wire::reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: flux_common::ids::Offset(5),
        max_bytes: 1024 * 1024,
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let fetch_resp = decode_read_response(&data);
    assert!(fetch_resp.success);
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 5);
    assert_eq!(
        fetch_resp.results[0].records[0].key,
        Some(Bytes::from("key-5"))
    );

    ws.close(None).await.ok();
}
