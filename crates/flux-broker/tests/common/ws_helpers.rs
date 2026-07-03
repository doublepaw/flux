// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! WebSocket test helpers for E2E tests.

use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use flux_broker::buffer::BufferConfig;
use flux_broker::{BrokerConfig, BrokerState, LocalFsStore};
use flux_wire::{
    ClientMessage, ServerMessage, auth as wire_auth, decode_server_message, encode_client_message,
    reader, writer,
};

/// Find an available port.
pub async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server in background.
pub async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    start_server_with_auth(pool, temp_dir, false).await
}

/// Start the broker server in background with configurable auth.
pub async fn start_server_with_auth(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
    require_auth: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(50),
        require_auth,
        auth_timeout: Duration::from_secs(10),
        #[cfg(feature = "iceberg")]
        iceberg: None,
        readahead_max_bytes: 0,
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

pub async fn authenticate_ws(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, api_key: &str) {
    let auth_req = wire_auth::AuthRequest {
        api_key: api_key.to_string(),
    };

    let buf = encode_client_frame(ClientMessage::Auth(auth_req), 512);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for auth response")
        .expect("No auth response")
        .expect("WebSocket auth response error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary auth response"),
    };
    let auth_resp = decode_auth_response(&data);
    assert!(
        auth_resp.success,
        "auth failed: {}",
        auth_resp.error_message
    );
}

pub fn encode_client_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; capacity];
    let len = encode_client_message(&msg, &mut buf).expect("encode client frame");
    buf.truncate(len);
    buf
}

pub fn decode_server_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
}

pub fn decode_auth_response(data: &[u8]) -> wire_auth::AuthResponse {
    match decode_server_frame(data) {
        ServerMessage::Auth(resp) => resp,
        _ => panic!("expected auth response"),
    }
}

pub fn decode_produce_response(data: &[u8]) -> writer::AppendResponse {
    match decode_server_frame(data) {
        ServerMessage::Append(resp) => resp,
        _ => panic!("expected append response"),
    }
}

pub fn decode_read_response(data: &[u8]) -> reader::ReadResponse {
    match decode_server_frame(data) {
        ServerMessage::Read(resp) => resp,
        _ => panic!("expected read response"),
    }
}

pub fn decode_join_response(data: &[u8]) -> reader::JoinGroupResponse {
    match decode_server_frame(data) {
        ServerMessage::JoinGroup(resp) => resp,
        _ => panic!("expected join response"),
    }
}

pub fn decode_poll_response(data: &[u8]) -> reader::PollResponse {
    match decode_server_frame(data) {
        ServerMessage::Poll(resp) => resp,
        _ => panic!("expected poll response"),
    }
}

pub fn decode_commit_response(data: &[u8]) -> reader::CommitResponse {
    match decode_server_frame(data) {
        ServerMessage::Commit(resp) => resp,
        _ => panic!("expected commit response"),
    }
}
