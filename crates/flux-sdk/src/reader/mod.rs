// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Reader client for reading messages from Flux.
//!
//! Features:
//! - Reader group support with broker-side work dispatch (poll model)
//! - Heartbeat loop for membership maintenance
//! - Offset range tracking and commit

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use flux_common::ids::{Offset, TopicId};
use flux_wire::{
    ClientMessage, ServerMessage, auth as wire_auth, decode_server_message, encode_client_message,
    reader,
};

use crate::SdkError;

/// Primary SDK reader (group-aware).
pub type Reader = GroupReader;

/// Configuration for the reader.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// API key for authentication (optional).
    pub api_key: Option<String>,
    /// Reader group ID.
    pub group_id: String,
    /// Reader ID within the group.
    pub reader_id: String,
    /// Topic to subscribe to.
    pub topic_id: TopicId,
    /// Default max bytes per read.
    pub max_bytes: u32,
    /// Request timeout.
    pub timeout: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            api_key: None,
            group_id: "default".to_string(),
            reader_id: uuid::Uuid::new_v4().to_string(),
            topic_id: TopicId(1),
            max_bytes: 1024 * 1024, // 1 MB
            timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
        }
    }
}

/// Reader state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderState {
    Init,
    Active,
    Stopped,
}

/// Result of a read operation.
#[derive(Debug)]
pub struct ReadResult {
    pub topic_id: TopicId,
    pub schema_id: flux_common::ids::SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<flux_common::types::Record>,
}

/// A batch of results from a single poll, with its offset range and lease deadline.
///
/// Pass this to `commit()` to commit the specific range. Multiple `PollBatch`es
/// can be outstanding simultaneously (pipelined polling).
#[derive(Debug)]
pub struct PollBatch {
    pub results: Vec<ReadResult>,
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub lease_deadline_ms: u64,
}

/// Reader client with broker-side work dispatch.
pub struct GroupReader {
    config: ReaderConfig,
    ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    state: RwLock<ReaderState>,
    inflight: RwLock<Vec<(Offset, Offset)>>,
    running: AtomicBool,
}

impl GroupReader {
    /// Create a new group reader and join the group.
    pub async fn join(config: ReaderConfig) -> Result<Arc<Self>, SdkError> {
        let (mut ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        if let Some(ref api_key) = config.api_key {
            Self::authenticate(&mut ws, api_key).await?;
        }

        let reader = Arc::new(Self {
            config,
            ws: Mutex::new(ws),
            state: RwLock::new(ReaderState::Init),
            inflight: RwLock::new(Vec::new()),
            running: AtomicBool::new(true),
        });

        reader.do_join().await?;
        Ok(reader)
    }

    /// Perform authentication handshake.
    async fn authenticate(
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        api_key: &str,
    ) -> Result<(), SdkError> {
        let auth_req = wire_auth::AuthRequest {
            api_key: api_key.to_string(),
        };

        let mut buf = vec![0u8; 512];
        let len = encode_client_message(&ClientMessage::Auth(auth_req), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let resp = ws
            .next()
            .await
            .ok_or_else(|| SdkError::Connection("connection closed during auth".to_string()))?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let data = match resp {
            Message::Binary(d) => d,
            _ => {
                return Err(SdkError::Protocol(
                    "unexpected auth response type".to_string(),
                ));
            }
        };

        let (resp_msg, used) =
            decode_server_message(&data).map_err(|e| SdkError::Protocol(e.to_string()))?;
        if used != data.len() {
            return Err(SdkError::Protocol(
                "trailing bytes in auth response".to_string(),
            ));
        }
        let auth_resp = match resp_msg {
            ServerMessage::Auth(resp) => resp,
            _ => {
                return Err(SdkError::Protocol(
                    "unexpected auth response type".to_string(),
                ));
            }
        };

        if !auth_resp.success {
            return Err(SdkError::Auth(auth_resp.error_message));
        }

        debug!("Authentication successful");
        Ok(())
    }

    /// Start the heartbeat loop in the background.
    pub fn start_heartbeat(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let reader = self.clone();
        tokio::spawn(async move {
            reader.heartbeat_loop().await;
        })
    }

    /// Stop the reader.
    pub async fn stop(&self) -> Result<(), SdkError> {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write().await = ReaderState::Stopped;
        self.do_leave().await?;
        Ok(())
    }

    pub async fn state(&self) -> ReaderState {
        *self.state.read().await
    }

    /// Poll the broker for records. The broker dispatches work (offset ranges).
    ///
    /// Multiple polls can be outstanding simultaneously (pipelined polling).
    /// Pass the returned `PollBatch` to `commit()` to commit that specific range.
    pub async fn poll(&self) -> Result<PollBatch, SdkError> {
        let state = *self.state.read().await;
        if state != ReaderState::Active {
            return Err(SdkError::InvalidState(format!("{:?}", state)));
        }

        let req = reader::PollRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            reader_id: self.config.reader_id.clone(),
            max_bytes: self.config.max_bytes,
        };

        let resp = self.send_request(ClientMessage::Poll(req), 8192).await?;
        let response = match resp {
            ServerMessage::Poll(r) if r.success => r,
            ServerMessage::Poll(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                });
            }
            _ => return Err(SdkError::InvalidResponse),
        };

        let start_offset = response.start_offset;
        let end_offset = response.end_offset;
        let lease_deadline_ms = response.lease_deadline_ms;

        if start_offset != end_offset {
            self.inflight.write().await.push((start_offset, end_offset));
        }

        let results = response
            .results
            .into_iter()
            .map(|r| ReadResult {
                topic_id: r.topic_id,
                schema_id: r.schema_id,
                high_watermark: r.high_watermark,
                records: r.records,
            })
            .collect();

        Ok(PollBatch {
            results,
            start_offset,
            end_offset,
            lease_deadline_ms,
        })
    }

    /// Commit a specific polled batch's offset range.
    pub async fn commit(&self, batch: &PollBatch) -> Result<(), SdkError> {
        let req = reader::CommitRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
            topic_id: self.config.topic_id,
            start_offset: batch.start_offset,
            end_offset: batch.end_offset,
        };

        let resp = self.send_request(ClientMessage::Commit(req), 8192).await?;
        match resp {
            ServerMessage::Commit(r) if r.success => {
                let s = batch.start_offset;
                let e = batch.end_offset;
                self.inflight
                    .write()
                    .await
                    .retain(|(ss, ee)| *ss != s || *ee != e);
                Ok(())
            }
            ServerMessage::Commit(r) => Err(SdkError::Server {
                code: r.error_code,
                message: r.error_message,
            }),
            _ => Err(SdkError::InvalidResponse),
        }
    }

    /// Join the reader group.
    async fn do_join(&self) -> Result<(), SdkError> {
        let req = reader::JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
            topic_ids: vec![self.config.topic_id],
        };

        let resp = self
            .send_request(ClientMessage::JoinGroup(req), 8192)
            .await?;
        match resp {
            ServerMessage::JoinGroup(r) if r.success => {}
            ServerMessage::JoinGroup(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                });
            }
            _ => return Err(SdkError::InvalidResponse),
        }

        self.inflight.write().await.clear();
        *self.state.write().await = ReaderState::Active;

        info!("Joined group {}", self.config.group_id);

        Ok(())
    }

    /// Leave the reader group, committing all outstanding inflight ranges first.
    async fn do_leave(&self) -> Result<(), SdkError> {
        // Commit all inflight ranges before leaving
        let ranges: Vec<(Offset, Offset)> = self.inflight.read().await.clone();
        for (start, end) in &ranges {
            let batch = PollBatch {
                results: vec![],
                start_offset: *start,
                end_offset: *end,
                lease_deadline_ms: 0,
            };
            if let Err(e) = self.commit(&batch).await {
                warn!(
                    "Failed to commit range [{}, {}) during leave: {}",
                    start.0, end.0, e
                );
            }
        }

        let req = reader::LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            reader_id: self.config.reader_id.clone(),
        };

        let resp = self
            .send_request(ClientMessage::LeaveGroup(req), 256)
            .await?;
        match resp {
            ServerMessage::LeaveGroup(r) if r.success => {}
            ServerMessage::LeaveGroup(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                });
            }
            _ => return Err(SdkError::InvalidResponse),
        }

        info!("Left group {}", self.config.group_id);
        Ok(())
    }

    /// Heartbeat loop.
    async fn heartbeat_loop(&self) {
        while self.running.load(Ordering::SeqCst) {
            tokio::time::sleep(self.config.heartbeat_interval).await;

            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            let req = reader::HeartbeatRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                reader_id: self.config.reader_id.clone(),
            };

            let result = self.send_request(ClientMessage::Heartbeat(req), 256).await;

            match result {
                Ok(ServerMessage::Heartbeat(response)) if response.success => {
                    match response.status {
                        reader::HeartbeatStatus::Ok => {
                            debug!("Heartbeat OK");
                        }
                        reader::HeartbeatStatus::UnknownMember => {
                            warn!("Unknown member, rejoining group");
                            if let Err(e) = self.do_join().await {
                                error!("Failed to rejoin: {}", e);
                            }
                        }
                    }
                }
                Ok(ServerMessage::Heartbeat(response)) => {
                    error!(
                        "Heartbeat failed: {} {}",
                        response.error_code, response.error_message
                    );
                }
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
                _ => {
                    error!("Heartbeat: unexpected response type");
                }
            }
        }
    }

    // ============ Wire protocol ============

    /// Encode a client message, send it, receive and decode the server response.
    async fn send_request(
        &self,
        msg: ClientMessage,
        buf_size: usize,
    ) -> Result<ServerMessage, SdkError> {
        let mut buf = vec![0u8; buf_size];
        let len =
            encode_client_message(&msg, &mut buf).map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;

        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode("trailing bytes in response".to_string()));
        }

        Ok(response_msg)
    }

    async fn send_and_receive(&self, buf: Vec<u8>) -> Result<Vec<u8>, SdkError> {
        let mut ws = self.ws.lock().await;

        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let response = tokio::time::timeout(self.config.timeout, ws.next())
            .await
            .map_err(|_| SdkError::Timeout)?
            .ok_or(SdkError::Disconnected)?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        match response {
            Message::Binary(data) => Ok(data),
            Message::Close(_) => Err(SdkError::Disconnected),
            _ => Err(SdkError::InvalidResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_config_default() {
        let config = ReaderConfig::default();
        assert_eq!(config.max_bytes, 1024 * 1024);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_poll_request_encoding() {
        let req = reader::PollRequest {
            group_id: "test-group".to_string(),
            topic_id: TopicId(1),
            reader_id: "test-reader".to_string(),
            max_bytes: 2048,
        };

        let mut buf = vec![0u8; 1024];
        let len = reader::encode_poll_request(&req, &mut buf);
        assert!(len > 0);

        let (decoded, _) = reader::decode_poll_request(&buf[..len]).unwrap();
        assert_eq!(decoded.group_id, "test-group");
        assert_eq!(decoded.topic_id, TopicId(1));
        assert_eq!(decoded.reader_id, "test-reader");
        assert_eq!(decoded.max_bytes, 2048);
    }
}
