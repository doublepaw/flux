// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Negative test cases for error handling.
//!
//! These tests verify that the broker handles error conditions gracefully.

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use flux_broker::buffer::BufferConfig;
use flux_broker::{BrokerConfig, BrokerState, LocalFsStore};
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{
    ClientMessage, ServerMessage, decode_server_message, encode_client_message, reader, writer,
};

use common::{CrashableWsBroker, TestDb};

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server in background.
async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(50),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
        #[cfg(feature = "iceberg")]
        iceberg: None,
        readahead_max_bytes: 0,
            flush_pipeline_depth: 4,
    };

    let state = BrokerState::new(pool, store, config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = flux_broker::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    (addr, handle)
}

fn encode_client_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; capacity];
    let len = encode_client_message(&msg, &mut buf).expect("encode client frame");
    buf.truncate(len);
    buf
}

fn decode_server_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
}

/// Test that sending malformed (garbage) bytes is handled gracefully.
#[tokio::test]
async fn test_malformed_websocket_message() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Send garbage bytes that cannot be decoded as a valid message
    let garbage = vec![0xFF, 0xFE, 0x00, 0x01, 0x02, 0x03, 0xFF, 0xFF];
    ws.send(Message::Binary(garbage)).await.unwrap();

    // The server MUST respond to malformed input - either close the connection or send an error.
    // A timeout is NOT acceptable as it indicates the server is not properly handling bad input.
    let result = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;

    let handled_correctly = match &result {
        Ok(Some(Ok(Message::Close(Some(f))))) => {
            // Close with an error code (not 1000 = normal closure)
            f.code != tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal
        }
        Ok(Some(Ok(Message::Close(None)))) => {
            true // Close without frame is acceptable
        }
        Ok(Some(Ok(Message::Binary(data)))) => {
            // Error response - should have content
            !data.is_empty()
        }
        Ok(Some(Err(_))) => {
            // WebSocket error - connection closed with error
            true
        }
        Ok(None) => {
            // Stream ended - connection closed
            true
        }
        Err(_) => {
            // Timeout - server didn't respond to malformed input
            false
        }
        _ => {
            // Text, Ping, Pong, Frame are unexpected but not crashes
            true
        }
    };

    assert!(
        handled_correctly,
        "Server should respond to malformed message (close connection or send error), got: {:?}",
        result
    );

    // Server should still be responsive for valid requests
    let (mut ws2, _) = connect_async(&url)
        .await
        .expect("Server should still accept connections");
    ws2.close(None).await.ok();
}

/// Test that producing to a non-existent topic returns an error response.
#[tokio::test]
async fn test_produce_to_nonexistent_topic() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Try to append to topic ID 99999 which doesn't exist
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(99999),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("key")),
                value: Bytes::from("value"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Should get a response with an error (topic doesn't exist in topic_offsets,
    // so the flush transaction will fail).
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    match response {
        Message::Binary(data) => {
            let msg = decode_server_frame(&data);
            match msg {
                ServerMessage::Append(resp) => {
                    assert!(
                        !resp.success,
                        "Append to non-existent topic should fail, got success with acks: {:?}",
                        resp.append_acks
                    );
                    assert_ne!(
                        resp.error_code, 0,
                        "Error code should be non-zero for non-existent topic"
                    );
                }
                other => panic!(
                    "Expected Append response for Append request, got {:?}",
                    std::mem::discriminant(&other)
                ),
            }
        }
        Message::Close(_) => {
            // Connection closed with error — acceptable
        }
        _ => panic!("Unexpected message type"),
    }

    ws.close(None).await.ok();
}

/// Test that fetching from a non-existent topic returns appropriate response.
#[tokio::test]
async fn test_fetch_from_nonexistent_topic() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Try to read from topic 99999 which doesn't exist
    let fetch_req = reader::ReadRequest {
        topic_id: TopicId(99999),
        offset: Offset(0),
        max_bytes: 1024 * 1024,
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Should get a response (empty results or error)
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    match response {
        Message::Binary(data) => {
            let msg = decode_server_frame(&data);
            match msg {
                ServerMessage::Read(resp) => {
                    if resp.success {
                        // Successful read of non-existent topic should return empty results
                        let total_records: usize =
                            resp.results.iter().map(|r| r.records.len()).sum();
                        assert_eq!(
                            total_records, 0,
                            "Read from non-existent topic should return 0 records, got {}",
                            total_records
                        );
                    } else {
                        // Error response is acceptable — verify error code is set
                        assert_ne!(
                            resp.error_code, 0,
                            "Failed read should have non-zero error code"
                        );
                    }
                }
                other => panic!(
                    "Expected Read response for Read request, got {:?}",
                    std::mem::discriminant(&other)
                ),
            }
        }
        Message::Close(_) => {
            // Connection closed with error — acceptable
        }
        _ => panic!("Unexpected message type"),
    }

    ws.close(None).await.ok();
}

/// Test that connection close during append doesn't corrupt data.
#[tokio::test]
async fn test_connection_close_during_produce() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("close-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First, append some baseline records
    {
        let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("baseline")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

        ws.send(Message::Binary(buf)).await.unwrap();

        // Wait for ack
        let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
        ws.close(None).await.ok();
    }

    // Now simulate connection close during append
    for _ in 0..5 {
        let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("interrupted")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

        // Send the request
        ws.send(Message::Binary(buf)).await.unwrap();

        // Immediately close without waiting for response
        drop(ws);

        // Small delay before next attempt
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for any pending operations to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data integrity by fetching all records
    let (mut ws, _) = connect_async(&url)
        .await
        .expect("Failed to connect after stress test");

    let fetch_req = reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: Offset(0),
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

    let fetch_resp = match decode_server_frame(&data) {
        ServerMessage::Read(resp) => resp,
        _ => panic!("Expected read response"),
    };

    // Verify we have at least the baseline record
    assert!(!fetch_resp.results.is_empty(), "Should have topic results");
    assert!(
        !fetch_resp.results[0].records.is_empty(),
        "Should have at least baseline record"
    );

    // Verify high watermark is consistent with record count
    let record_count = fetch_resp.results[0].records.len();
    let watermark = fetch_resp.results[0].high_watermark.0;
    assert!(
        watermark >= record_count as u64,
        "Watermark {} should be >= record count {}",
        watermark,
        record_count
    );

    // Verify records are valid (no corruption)
    for record in &fetch_resp.results[0].records {
        assert!(
            record.key.is_some(),
            "Record should have a key (no corruption)"
        );
        assert!(
            !record.value.is_empty(),
            "Record should have value (no corruption)"
        );
    }

    ws.close(None).await.ok();
}

/// Connection drop mid-append with slow flush must not corrupt broker state.
/// After the drop, a fresh client can write and read consistently.
#[tokio::test]
async fn test_connection_drop_mid_append_broker_stays_healthy() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("drop-mid-append").await;

    let broker = CrashableWsBroker::start(db.pool.clone()).await;

    // Slow down flush so the append is still in-flight when we drop
    broker.faulty_store().set_put_delay_ms(1000);

    let url = format!("ws://{}", broker.addr());

    // Connect, send append, immediately drop without awaiting response
    {
        let (mut ws, _) = connect_async(&url).await.expect("connect");
        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: None,
                    value: Bytes::from("dropped-value"),
                }],
            }],
        };
        let buf = encode_client_frame(ClientMessage::Append(req), 8192);
        ws.send(Message::Binary(buf)).await.unwrap();
        drop(ws);
    }

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remove delay for subsequent operations
    broker.faulty_store().set_put_delay_ms(0);

    // Fresh connection: write 1 record
    let (mut ws, _) = connect_async(&url).await.expect("reconnect");
    let fresh_writer = WriterId::new();
    let req = writer::AppendRequest {
        writer_id: fresh_writer,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("fresh-value"),
            }],
        }],
    };
    let buf = encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let resp = match msg {
        Message::Binary(d) => decode_server_frame(&d),
        _ => panic!("expected binary"),
    };
    let append_resp = match resp {
        ServerMessage::Append(r) => r,
        _ => panic!("expected append response"),
    };
    assert!(
        append_resp.success,
        "fresh write should succeed after connection drop"
    );

    // Read all records -- verify consistency
    let fetch_req = reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: Offset(0),
        max_bytes: 10 * 1024 * 1024,
    };
    let buf = encode_client_frame(ClientMessage::Read(fetch_req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let read_resp = match msg {
        Message::Binary(d) => match decode_server_frame(&d) {
            ServerMessage::Read(r) => r,
            _ => panic!("expected read response"),
        },
        _ => panic!("expected binary"),
    };
    assert!(read_resp.success, "read should succeed");

    // fresh-value must be present
    let all_values: Vec<&Bytes> = read_resp
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| &rec.value))
        .collect();
    assert!(
        all_values.contains(&&Bytes::from("fresh-value")),
        "fresh write must be visible"
    );

    // Contiguous offsets: record count == watermark
    let total_records: usize = read_resp.results.iter().map(|r| r.records.len()).sum();
    let hwm = read_resp
        .results
        .first()
        .map(|r| r.high_watermark.0)
        .unwrap_or(0);
    assert_eq!(
        total_records as u64, hwm,
        "offsets should be contiguous: {} records but watermark {}",
        total_records, hwm
    );
}

/// Rapid connect/disconnect stress test -- catches memory leaks, state accumulation.
#[tokio::test]
async fn test_rapid_connect_disconnect_no_state_corruption() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("rapid-churn").await;

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let url = format!("ws://{}", broker.addr());

    // 30 rapid connect/send/drop cycles
    for i in 0..30u64 {
        let (mut ws, _) = connect_async(&url).await.expect("connect");
        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: None,
                    value: Bytes::from(format!("churn-{}", i)),
                }],
            }],
        };
        let buf = encode_client_frame(ClientMessage::Append(req), 8192);
        ws.send(Message::Binary(buf)).await.unwrap();
        drop(ws);
    }

    // Wait for all pending flushes to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Fresh connection: write 1 more record and read all
    let (mut ws, _) = connect_async(&url).await.expect("reconnect");
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from("final-value"),
            }],
        }],
    };
    let buf = encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let resp = match msg {
        Message::Binary(d) => match decode_server_frame(&d) {
            ServerMessage::Append(r) => r,
            _ => panic!("expected append response"),
        },
        _ => panic!("expected binary"),
    };
    assert!(resp.success, "final write should succeed after rapid churn");

    // Read all records
    let fetch_req = reader::ReadRequest {
        topic_id: TopicId(topic_id as u32),
        offset: Offset(0),
        max_bytes: 10 * 1024 * 1024,
    };
    let buf = encode_client_frame(ClientMessage::Read(fetch_req), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("closed")
        .expect("error");
    let read_resp = match msg {
        Message::Binary(d) => match decode_server_frame(&d) {
            ServerMessage::Read(r) => r,
            _ => panic!("expected read response"),
        },
        _ => panic!("expected binary"),
    };
    assert!(read_resp.success, "read should succeed");

    // Verify: final-value is present
    let all_values: Vec<Bytes> = read_resp
        .results
        .iter()
        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
        .collect();
    assert!(
        all_values.contains(&Bytes::from("final-value")),
        "final write must be visible"
    );

    // All offsets unique (no gaps or duplicates relative to watermark)
    let hwm = read_resp
        .results
        .first()
        .map(|r| r.high_watermark.0)
        .unwrap_or(0);
    assert_eq!(
        all_values.len() as u64,
        hwm,
        "record count {} should equal watermark {}",
        all_values.len(),
        hwm
    );
}
