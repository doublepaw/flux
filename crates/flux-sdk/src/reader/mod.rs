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

use futures::stream::{SplitSink, SplitStream};
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
    /// Zero-copy polls: the broker returns compressed segments and the SDK
    /// decodes them locally (CRC-verified).
    pub raw: bool,
    /// Outstanding poll requests kept in flight by the prefetcher (1-8).
    pub pipeline_depth: usize,
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
            raw: false,
            pipeline_depth: 2,
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
type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
type MsgRx = Mutex<tokio::sync::mpsc::UnboundedReceiver<ServerMessage>>;

pub struct GroupReader {
    config: ReaderConfig,
    /// Write half; a router task owns the read half and dispatches responses
    /// by type so poll prefetching, commits, and heartbeats never race.
    writer: Mutex<WsSink>,
    commit_rx: MsgRx,
    heartbeat_rx: MsgRx,
    join_rx: MsgRx,
    leave_rx: MsgRx,
    /// Prefetched, decoded batches, bounded to `pipeline_depth`.
    ready_rx: Mutex<tokio::sync::mpsc::Receiver<Result<PollBatch, SdkError>>>,
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

        let (sink, stream) = ws.split();

        let (poll_tx, poll_rx) = tokio::sync::mpsc::unbounded_channel();
        let (commit_tx, commit_rx) = tokio::sync::mpsc::unbounded_channel();
        let (heartbeat_tx, heartbeat_rx) = tokio::sync::mpsc::unbounded_channel();
        let (join_tx, join_rx) = tokio::sync::mpsc::unbounded_channel();
        let (leave_tx, leave_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(route_responses(
            stream,
            poll_tx,
            commit_tx,
            heartbeat_tx,
            join_tx,
            leave_tx,
        ));

        let depth = config.pipeline_depth.clamp(1, 8);
        let (ready_tx, ready_rx) = tokio::sync::mpsc::channel(depth);

        let reader = Arc::new(Self {
            config,
            writer: Mutex::new(sink),
            commit_rx: Mutex::new(commit_rx),
            heartbeat_rx: Mutex::new(heartbeat_rx),
            join_rx: Mutex::new(join_rx),
            leave_rx: Mutex::new(leave_rx),
            ready_rx: Mutex::new(ready_rx),
            state: RwLock::new(ReaderState::Init),
            inflight: RwLock::new(Vec::new()),
            running: AtomicBool::new(true),
        });

        reader.do_join().await?;

        // Prefetcher: keeps `depth` poll requests outstanding and decodes
        // responses ahead of the application.
        let prefetch_reader = reader.clone();
        tokio::spawn(async move {
            prefetch_reader
                .prefetch_loop(depth, poll_rx, ready_tx)
                .await;
        });

        Ok(reader)
    }

    async fn send_msg(&self, msg: &ClientMessage, buf_size: usize) -> Result<(), SdkError> {
        let mut buf = vec![0u8; buf_size];
        let len =
            encode_client_message(msg, &mut buf).map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);
        self.writer
            .lock()
            .await
            .send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))
    }

    fn send_poll_request(&self) -> ClientMessage {
        ClientMessage::Poll(reader::PollRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            reader_id: self.config.reader_id.clone(),
            max_bytes: self.config.max_bytes,
            raw: self.config.raw,
        })
    }

    async fn prefetch_loop(
        &self,
        depth: usize,
        mut poll_rx: tokio::sync::mpsc::UnboundedReceiver<ServerMessage>,
        ready_tx: tokio::sync::mpsc::Sender<Result<PollBatch, SdkError>>,
    ) {
        let result: Result<(), SdkError> = async {
            for _ in 0..depth {
                self.send_msg(&self.send_poll_request(), 8192).await?;
            }
            while self.running.load(Ordering::SeqCst) {
                let msg = poll_rx
                    .recv()
                    .await
                    .ok_or_else(|| SdkError::Connection("connection closed".into()))?;
                let response = match msg {
                    ServerMessage::Poll(r) if r.success => r,
                    ServerMessage::Poll(r) => {
                        return Err(SdkError::Server {
                            code: r.error_code,
                            message: r.error_message,
                        });
                    }
                    _ => return Err(SdkError::InvalidResponse),
                };

                let batch = self.build_batch(response).await?;
                let empty = batch.start_offset == batch.end_offset;
                if ready_tx.send(Ok(batch)).await.is_err() {
                    break; // reader dropped
                }
                if empty {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                if self.running.load(Ordering::SeqCst) {
                    self.send_msg(&self.send_poll_request(), 8192).await?;
                }
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            let _ = ready_tx.send(Err(e)).await;
        }
    }

    async fn build_batch(&self, response: reader::PollResponse) -> Result<PollBatch, SdkError> {
        let start_offset = response.start_offset;
        let end_offset = response.end_offset;
        let lease_deadline_ms = response.lease_deadline_ms;

        if start_offset != end_offset {
            self.inflight.write().await.push((start_offset, end_offset));
        }

        let mut results: Vec<ReadResult> = response
            .results
            .into_iter()
            .map(|r| ReadResult {
                topic_id: r.topic_id,
                schema_id: r.schema_id,
                high_watermark: r.high_watermark,
                records: r.records,
            })
            .collect();

        for seg in response.raw_segments {
            let records = flux_wire::segment::decode_segment(&seg.payload, Some(seg.crc32))
                .map_err(|e| SdkError::Decode(format!("segment decode: {e}")))?;
            results.push(ReadResult {
                topic_id: seg.topic_id,
                schema_id: seg.schema_id,
                high_watermark: end_offset,
                records,
            });
        }

        Ok(PollBatch {
            results,
            start_offset,
            end_offset,
            lease_deadline_ms,
        })
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

        let mut ready = self.ready_rx.lock().await;
        match tokio::time::timeout(self.config.timeout, ready.recv()).await {
            Ok(Some(batch)) => batch,
            Ok(None) => Err(SdkError::Connection("prefetcher stopped".into())),
            Err(_) => Err(SdkError::Timeout),
        }
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
        let rx = match &msg {
            ClientMessage::Commit(_) => &self.commit_rx,
            ClientMessage::Heartbeat(_) => &self.heartbeat_rx,
            ClientMessage::JoinGroup(_) => &self.join_rx,
            ClientMessage::LeaveGroup(_) => &self.leave_rx,
            other => {
                return Err(SdkError::Protocol(format!(
                    "unsupported request type: {other:?}"
                )));
            }
        };

        // Hold the type's receiver across send+receive so concurrent callers
        // of the same request type serialize.
        let mut rx = rx.lock().await;
        self.send_msg(&msg, buf_size).await?;
        match tokio::time::timeout(self.config.timeout, rx.recv()).await {
            Ok(Some(response)) => Ok(response),
            Ok(None) => Err(SdkError::Connection("connection closed".into())),
            Err(_) => Err(SdkError::Timeout),
        }
    }
}

/// Reads frames from the socket and dispatches each server message to its
/// request type's channel. Exits (dropping all senders, which surfaces
/// connection-closed errors to waiters) when the socket ends.
async fn route_responses(
    mut stream: WsStream,
    poll_tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    commit_tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    heartbeat_tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    join_tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    leave_tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some(frame) = stream.next().await {
        let data = match frame {
            Ok(Message::Binary(data)) => data,
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => continue,
        };
        let msg = match decode_server_message(&data) {
            Ok((msg, used)) if used == data.len() => msg,
            _ => {
                warn!("dropping undecodable server frame ({} bytes)", data.len());
                continue;
            }
        };
        let sent = match &msg {
            ServerMessage::Poll(_) => poll_tx.send(msg).is_ok(),
            ServerMessage::Commit(_) => commit_tx.send(msg).is_ok(),
            ServerMessage::Heartbeat(_) => heartbeat_tx.send(msg).is_ok(),
            ServerMessage::JoinGroup(_) => join_tx.send(msg).is_ok(),
            ServerMessage::LeaveGroup(_) => leave_tx.send(msg).is_ok(),
            other => {
                warn!("unexpected server message: {other:?}");
                true
            }
        };
        if !sent {
            break;
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
            raw: false,
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
