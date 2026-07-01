// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! End-to-end reader group tests.
//!
//! These tests verify reader group coordination:
//! - Single reader joins
//! - Multiple readers join and share work
//! - Heartbeat maintains membership
//! - Commit offsets
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_reader_groups
//! ```

mod common;

use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use bytes::Bytes;
use flux_broker::{BrokerConfig, BrokerState, BufferConfig, CoordinatorConfig, LocalFsStore};
use flux_common::ids::{AppendSeq, WriterId};
use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{
    ClientMessage, ServerMessage, decode_server_message, encode_client_message, reader, writer,
};

use common::TestDb;

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server with short session timeout for testing.
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
        flush_interval: Duration::from_millis(10),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
        #[cfg(feature = "iceberg")]
        iceberg: None,
    };

    // Short session timeout for testing
    let coordinator_config = CoordinatorConfig {
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let state = BrokerState::with_coordinator_config(pool, store, config, coordinator_config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = flux_broker::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

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

/// Helper: send a JoinGroup request and get response.
async fn join_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::JoinGroupResponse {
    let req = reader::JoinGroupRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        topic_ids: vec![topic_id],
    };

    let buf = encode_client_frame(ClientMessage::JoinGroup(req), 1024);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for join response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    match decode_server_frame(&data) {
        ServerMessage::JoinGroup(resp) => resp,
        _ => panic!("expected join response"),
    }
}

/// Helper: send a Heartbeat request and get response.
async fn send_heartbeat(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::HeartbeatResponseExt {
    let req = reader::HeartbeatRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
    };

    let buf = encode_client_frame(ClientMessage::Heartbeat(req), 256);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for heartbeat response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    match decode_server_frame(&data) {
        ServerMessage::Heartbeat(resp) => resp,
        _ => panic!("expected heartbeat response"),
    }
}

/// Helper: send a LeaveGroup request and get response.
async fn leave_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::LeaveGroupResponse {
    let req = reader::LeaveGroupRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
    };

    let buf = encode_client_frame(ClientMessage::LeaveGroup(req), 256);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for leave response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    match decode_server_frame(&data) {
        ServerMessage::LeaveGroup(resp) => resp,
        _ => panic!("expected leave response"),
    }
}

/// Helper: send a Commit request and get response.
async fn commit_offsets(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    reader_id: &str,
    topic_id: TopicId,
    start_offset: Offset,
    end_offset: Offset,
) -> reader::CommitResponse {
    let req = reader::CommitRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        topic_id,
        start_offset,
        end_offset,
    };

    let buf = encode_client_frame(ClientMessage::Commit(req), 1024);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for commit response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    match decode_server_frame(&data) {
        ServerMessage::Commit(resp) => resp,
        _ => panic!("expected commit response"),
    }
}

/// Test: Single reader joins.
#[tokio::test]
async fn test_single_reader_joins_group() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-single-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "reader-1").await;

    assert!(resp.success);

    ws.close(None).await.ok();
}

/// Test: Two readers join the same group.
#[tokio::test]
async fn test_two_readers_join_group() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-two-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;

    assert!(resp1.success);

    // Second reader joins
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;

    assert!(resp2.success);

    // First reader heartbeats - should succeed (still a member)
    let hb1 = send_heartbeat(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;
    assert_eq!(
        hb1.status,
        reader::HeartbeatStatus::Ok,
        "First reader should still be a member"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Reader leave triggers membership change.
#[tokio::test]
async fn test_reader_leave_triggers_membership_change() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-leave-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Two readers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;

    // Reader 2 leaves
    let leave_resp =
        leave_group(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;
    assert!(leave_resp.success);

    // Reader 1 heartbeats - should still be a member
    let hb1 = send_heartbeat(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;

    assert_eq!(hb1.status, reader::HeartbeatStatus::Ok);

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Helper: send an append request and get response.
async fn ws_append(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    topic_id: TopicId,
    records: Vec<Record>,
) -> writer::AppendResponse {
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(1),
            records,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req), 8192);
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

    match decode_server_frame(&data) {
        ServerMessage::Append(resp) => resp,
        _ => panic!("expected append response"),
    }
}

/// Helper: send a PollRequest and get response.
async fn ws_poll(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::PollResponse {
    let req = reader::PollRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
        max_bytes: 1024 * 1024,
    };

    let buf = encode_client_frame(ClientMessage::Poll(req), 1024);
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

    match decode_server_frame(&data) {
        ServerMessage::Poll(resp) => resp,
        _ => panic!("expected poll response"),
    }
}

/// Test: Produce records, poll for work, then commit the polled range.
#[tokio::test]
async fn test_commit_offsets() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-commit-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Produce some records first.
    let (mut ws_writer, _) = connect_async(&url).await.expect("Failed to connect");
    let records = vec![
        Record {
            key: Some(Bytes::from("k1")),
            value: Bytes::from("v1"),
        },
        Record {
            key: Some(Bytes::from("k2")),
            value: Bytes::from("v2"),
        },
    ];
    let append_resp = ws_append(&mut ws_writer, TopicId(topic_id as u32), records).await;
    assert!(append_resp.success, "Append should succeed");
    ws_writer.close(None).await.ok();

    // Wait for flush.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reader joins, polls, and commits.
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let join_resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "reader-1").await;
    assert!(join_resp.success);

    // Poll for dispatched work.
    let poll_resp = ws_poll(&mut ws, "test-group", TopicId(topic_id as u32), "reader-1").await;
    assert!(poll_resp.success, "Poll should succeed");
    assert!(
        poll_resp.end_offset.0 > poll_resp.start_offset.0,
        "Should have dispatched records"
    );

    // Commit the polled range.
    let commit_resp = commit_offsets(
        &mut ws,
        "test-group",
        "reader-1",
        TopicId(topic_id as u32),
        poll_resp.start_offset,
        poll_resp.end_offset,
    )
    .await;

    assert!(commit_resp.success, "Commit should succeed");

    ws.close(None).await.ok();
}

/// Test: Heartbeat keeps membership alive.
#[tokio::test]
async fn test_heartbeat_maintains_membership() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-heartbeat-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "reader-1").await;

    // Send heartbeats
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let hb = send_heartbeat(&mut ws, "test-group", TopicId(topic_id as u32), "reader-1").await;

        assert_eq!(hb.status, reader::HeartbeatStatus::Ok);
    }

    ws.close(None).await.ok();
}

/// Test: Session timeout removes member.
#[tokio::test]
async fn test_session_timeout_removes_member() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-timeout-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins but doesn't heartbeat
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;

    // Wait for session timeout (2 seconds + buffer)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Second reader joins and heartbeats to trigger eviction of reader-1.
    // Expired member detection runs during heartbeat processing, so reader-2
    // must heartbeat before reader-1 tries.
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;

    let hb2 = send_heartbeat(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;
    assert_eq!(hb2.status, reader::HeartbeatStatus::Ok);

    // First reader heartbeat should fail with UnknownMember (evicted by reader-2's heartbeat)
    let hb1 = send_heartbeat(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;
    assert_eq!(
        hb1.status,
        reader::HeartbeatStatus::UnknownMember,
        "Evicted reader should be unknown: {:?}",
        hb1.status
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Pipelined poll and out-of-order commit via wire protocol.
#[tokio::test]
async fn test_pipelined_poll_and_out_of_order_commit() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-pipeline-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Produce records
    let (mut ws_writer, _) = connect_async(&url).await.expect("Failed to connect");
    for i in 0..20 {
        let records = vec![Record {
            key: Some(Bytes::from(format!("k{}", i))),
            value: Bytes::from(format!("v{}", i)),
        }];
        ws_append(&mut ws_writer, TopicId(topic_id as u32), records).await;
    }
    ws_writer.close(None).await.ok();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reader joins
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let join_resp = join_group(
        &mut ws,
        "pipeline-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    assert!(join_resp.success);

    // Pipeline: poll twice without committing
    let poll1 = ws_poll(
        &mut ws,
        "pipeline-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    assert!(poll1.success, "poll1 should succeed");

    let poll2 = ws_poll(
        &mut ws,
        "pipeline-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    assert!(poll2.success, "poll2 should succeed");

    // Verify lease_deadline_ms is populated
    assert!(
        poll1.lease_deadline_ms > 0,
        "poll1 lease_deadline_ms should be non-zero"
    );
    assert!(
        poll2.lease_deadline_ms > 0,
        "poll2 lease_deadline_ms should be non-zero"
    );

    // Commit second batch FIRST (out of order)
    if poll2.start_offset != poll2.end_offset {
        let commit2 = commit_offsets(
            &mut ws,
            "pipeline-group",
            "reader-1",
            TopicId(topic_id as u32),
            poll2.start_offset,
            poll2.end_offset,
        )
        .await;
        assert!(commit2.success, "commit2 should succeed");
    }

    // Commit first batch
    if poll1.start_offset != poll1.end_offset {
        let commit1 = commit_offsets(
            &mut ws,
            "pipeline-group",
            "reader-1",
            TopicId(topic_id as u32),
            poll1.start_offset,
            poll1.end_offset,
        )
        .await;
        assert!(commit1.success, "commit1 should succeed");
    }

    ws.close(None).await.ok();
}

/// Test: Three readers join the same group.
#[tokio::test]
async fn test_three_readers_join_group() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-uneven-test").await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "reader-1").await;
    assert!(resp1.success);

    // Second reader joins
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "reader-2").await;
    assert!(resp2.success);

    // Third reader joins
    let (mut ws3, _) = connect_async(&url).await.expect("Failed to connect");
    let resp3 = join_group(&mut ws3, "test-group", TopicId(topic_id as u32), "reader-3").await;
    assert!(resp3.success);

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
    ws3.close(None).await.ok();
}
