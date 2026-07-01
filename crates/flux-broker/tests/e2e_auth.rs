// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! End-to-end WebSocket authorization tests.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_auth
//! ```

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tempfile::TempDir;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use flux_broker::{AclChecker, ApiKeyValidator, Operation, ResourceType};
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, ERR_AUTHZ_DENIED, ERR_DECODE_ERROR, reader, writer};

use common::TestDb;
use common::ws_helpers::*;

/// Test that unprefixed append path enforces ACL when auth is enabled.
#[tokio::test]
async fn test_unprefixed_produce_acl_enforced_with_auth() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("unprefixed-append-authz").await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());

    let (deny_key, _) = validator
        .create_key("deny-writer", "service:deny-writer", None)
        .await
        .unwrap();
    let (allow_key, _) = validator
        .create_key("allow-writer", "service:allow-writer", None)
        .await
        .unwrap();

    checker
        .create_acl(
            "service:allow-writer",
            ResourceType::Topic,
            &topic_id.to_string(),
            Operation::Append,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);

    // Denied writer
    let (mut ws_deny, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws_deny, &deny_key).await;

    let deny_req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("deny")),
                value: Bytes::from("should-not-write"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(deny_req.clone()), 8192);
    ws_deny.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws_deny.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let deny_resp = decode_produce_response(&data);
    assert_eq!(deny_resp.append_seq.0, 0);
    assert!(!deny_resp.success);
    assert_eq!(deny_resp.error_code, ERR_AUTHZ_DENIED);
    assert!(deny_resp.append_acks.is_empty());

    let offset_after_deny: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(offset_after_deny, 0);
    ws_deny.close(None).await.ok();

    // Allowed writer
    let (mut ws_allow, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws_allow, &allow_key).await;

    let allow_req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("allow")),
                value: Bytes::from("should-write"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(allow_req.clone()), 8192);
    ws_allow.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws_allow.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let allow_resp = decode_produce_response(&data);
    assert_eq!(allow_resp.append_seq.0, 1);
    assert!(allow_resp.success);
    assert_eq!(allow_resp.append_acks.len(), 1);

    let offset_after_allow: i64 =
        sqlx::query_scalar("SELECT next_offset FROM topic_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(offset_after_allow, 1);

    ws_allow.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that prefixed JoinGroup ACL denial returns JoinGroupResponse wire shape.
#[tokio::test]
async fn test_prefixed_join_acl_denied_returns_typed_error_response() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("prefixed-join-authz").await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());
    let (deny_key, _) = validator
        .create_key("deny-reader", "service:deny-reader", None)
        .await
        .unwrap();
    checker
        .create_acl(
            "service:deny-reader",
            ResourceType::Group,
            "authz-denied-group",
            Operation::Consume,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws, &deny_key).await;

    let req = reader::JoinGroupRequest {
        group_id: "authz-denied-group".to_string(),
        reader_id: "reader-1".to_string(),
        topic_ids: vec![TopicId(topic_id as u32)],
    };

    let buf = encode_client_frame(ClientMessage::JoinGroup(req.clone()), 1024);
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
    let join_resp = decode_join_response(&data);
    assert!(!join_resp.success);
    assert_eq!(join_resp.error_code, ERR_AUTHZ_DENIED);

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that commit requires topic consume ACL in addition to group ACL.
#[tokio::test]
async fn test_prefixed_commit_requires_topic_acl() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("prefixed-commit-authz").await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());
    let (api_key, _) = validator
        .create_key("commit-reader", "service:commit-reader", None)
        .await
        .unwrap();

    checker
        .create_acl(
            "service:commit-reader",
            ResourceType::Group,
            "commit-authz-group",
            Operation::Consume,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws, &api_key).await;

    let req = reader::CommitRequest {
        group_id: "commit-authz-group".to_string(),
        reader_id: "reader-1".to_string(),
        topic_id: TopicId(topic_id as u32),
        start_offset: Offset(0),
        end_offset: Offset(1),
    };

    let buf = encode_client_frame(ClientMessage::Commit(req.clone()), 1024);
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
    let commit_resp = decode_commit_response(&data);
    assert!(!commit_resp.success);
    assert_eq!(commit_resp.error_code, ERR_AUTHZ_DENIED);

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that malformed proto-framed JoinGroup returns JoinGroupResponse wire shape.
#[tokio::test]
async fn test_prefixed_join_decode_failure_returns_typed_error_response() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();

    // Malformed JoinGroup frame
    ws.send(Message::Binary(vec![26, 0x80])).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let join_resp = decode_join_response(&data);
    assert!(!join_resp.success);
    assert_eq!(join_resp.error_code, ERR_DECODE_ERROR);

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}
