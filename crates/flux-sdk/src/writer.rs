// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Writer client for sending messages to Flux.
//!
//! Features:
//! - Automatic sequence number tracking
//! - Retry with exponential backoff on backpressure
//! - Pipelined in-flight requests over a single websocket connection
//! - Async fire-and-forget helpers (`append_async` / `append_batch_async`)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, warn};

use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{BatchAck, Record, RecordBatch};
use flux_wire::{
    ClientMessage, ERR_BACKPRESSURE, ServerMessage, auth as wire_auth, decode_server_message,
    encode_client_message, writer,
};

use crate::SdkError;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;
type PendingMap = HashMap<u64, oneshot::Sender<Result<Vec<BatchAck>, SdkError>>>;

/// Configuration for the writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// API key for authentication (optional).
    pub api_key: Option<String>,
    /// Maximum retries on backpressure.
    pub max_retries: u32,
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Request timeout.
    pub timeout: Duration,
    /// Maximum number of in-flight requests on one connection.
    pub max_in_flight: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            api_key: None,
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
            max_in_flight: 256,
        }
    }
}

/// Writer client for sending messages.
pub struct Writer {
    /// Writer ID (UUID).
    id: WriterId,
    /// Current sequence number.
    append_seq: AtomicU64,
    /// WebSocket writer half.
    writer: Mutex<WsWriter>,
    /// Pending append requests by sequence number.
    pending: Arc<Mutex<PendingMap>>,
    /// In-flight request limiter.
    in_flight: Semaphore,
    /// Configuration.
    config: WriterConfig,
}

impl Writer {
    /// Connect to the server with default configuration.
    pub async fn connect(url: &str) -> Result<Arc<Self>, SdkError> {
        let config = WriterConfig {
            url: url.to_string(),
            ..Default::default()
        };
        Self::connect_with_config(config).await
    }

    /// Connect to the server with custom configuration.
    pub async fn connect_with_config(config: WriterConfig) -> Result<Arc<Self>, SdkError> {
        let (mut ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        // Perform auth handshake if API key is provided.
        if let Some(ref api_key) = config.api_key {
            Self::authenticate(&mut ws, api_key).await?;
        }

        let (writer, reader) = ws.split();
        let pending = Arc::new(Mutex::new(HashMap::new()));

        let writer = Arc::new(Self {
            id: WriterId::new(),
            append_seq: AtomicU64::new(1),
            writer: Mutex::new(writer),
            pending: pending.clone(),
            in_flight: Semaphore::new(config.max_in_flight.max(1)),
            config,
        });

        tokio::spawn(Self::response_loop(reader, pending));
        Ok(writer)
    }

    async fn response_loop(mut reader: WsReader, pending: Arc<Mutex<PendingMap>>) {
        loop {
            let next = reader.next().await;
            let message = match next {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    Self::fail_all_pending_connection(&pending, e.to_string()).await;
                    break;
                }
                None => {
                    Self::fail_all_pending_disconnected(&pending).await;
                    break;
                }
            };

            match message {
                Message::Binary(data) => {
                    let (resp_msg, used) = match decode_server_message(&data) {
                        Ok(decoded) => decoded,
                        Err(_) => continue,
                    };
                    if used != data.len() {
                        continue;
                    }

                    if let ServerMessage::Append(resp) = resp_msg {
                        let tx = {
                            let mut map = pending.lock().await;
                            map.remove(&resp.append_seq.0)
                        };
                        if let Some(tx) = tx {
                            let result = if !resp.success {
                                if resp.error_code == ERR_BACKPRESSURE {
                                    Err(SdkError::Backpressure)
                                } else {
                                    Err(SdkError::Server {
                                        code: resp.error_code,
                                        message: resp.error_message,
                                    })
                                }
                            } else {
                                Ok(resp.append_acks)
                            };
                            let _ = tx.send(result);
                        }
                    }
                }
                Message::Close(_) => {
                    Self::fail_all_pending_disconnected(&pending).await;
                    break;
                }
                _ => {}
            }
        }
    }

    async fn fail_all_pending_disconnected(pending: &Arc<Mutex<PendingMap>>) {
        let mut map = pending.lock().await;
        for (_, tx) in map.drain() {
            let _ = tx.send(Err(SdkError::Disconnected));
        }
    }

    async fn fail_all_pending_connection(pending: &Arc<Mutex<PendingMap>>, message: String) {
        let mut map = pending.lock().await;
        for (_, tx) in map.drain() {
            let _ = tx.send(Err(SdkError::Connection(message.clone())));
        }
    }

    /// Perform authentication handshake.
    async fn authenticate(ws: &mut WsStream, api_key: &str) -> Result<(), SdkError> {
        let auth_req = wire_auth::AuthRequest {
            api_key: api_key.to_string(),
        };

        let msg = ClientMessage::Auth(auth_req);
        let buf = encode_client_message_vec(&msg, 512)?;
        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        // Wait for response.
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

    /// Get the writer ID.
    pub fn id(&self) -> WriterId {
        self.id
    }

    /// Get the current sequence number.
    pub fn append_seq(&self) -> AppendSeq {
        AppendSeq(self.append_seq.load(Ordering::SeqCst))
    }

    /// Append records to a topic.
    pub async fn append(
        &self,
        topic_id: TopicId,
        schema_id: SchemaId,
        records: Vec<Record>,
    ) -> Result<BatchAck, SdkError> {
        let batches = vec![RecordBatch {
            topic_id,
            schema_id,
            records,
        }];
        let append_acks = self.append_batch(batches).await?;
        append_acks
            .into_iter()
            .next()
            .ok_or(SdkError::InvalidResponse)
    }

    /// Append records across multiple topics in a single request.
    pub async fn append_batch(&self, batches: Vec<RecordBatch>) -> Result<Vec<BatchAck>, SdkError> {
        let append_seq = AppendSeq(self.append_seq.fetch_add(1, Ordering::SeqCst));
        let mut retries = 0;
        let mut backoff = self.config.initial_backoff;

        loop {
            match self.append_request(append_seq, batches.clone()).await {
                Err(SdkError::Backpressure) => {
                    if retries >= self.config.max_retries {
                        return Err(SdkError::Backpressure);
                    }

                    warn!(
                        "Backpressure from server, retrying in {:?} (attempt {}/{})",
                        backoff,
                        retries + 1,
                        self.config.max_retries
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    retries += 1;
                }
                Ok(append_acks) => {
                    debug!(
                        "Sent {} batches, got {} append_acks",
                        batches.len(),
                        append_acks.len()
                    );
                    return Ok(append_acks);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Async helper that starts an append and returns immediately.
    pub fn append_batch_async(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
    ) -> tokio::task::JoinHandle<Result<Vec<BatchAck>, SdkError>> {
        let writer = Arc::clone(self);
        tokio::spawn(async move { writer.append_batch(batches).await })
    }

    /// Async helper that starts an append and returns immediately.
    pub fn append_async(
        self: &Arc<Self>,
        topic_id: TopicId,
        schema_id: SchemaId,
        records: Vec<Record>,
    ) -> tokio::task::JoinHandle<Result<BatchAck, SdkError>> {
        let writer = Arc::clone(self);
        tokio::spawn(async move { writer.append(topic_id, schema_id, records).await })
    }

    /// Send a single append request and wait for response.
    async fn append_request(
        &self,
        append_seq: AppendSeq,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<BatchAck>, SdkError> {
        let _permit =
            self.in_flight.acquire().await.map_err(|_| {
                SdkError::InvalidState("writer in-flight limiter closed".to_string())
            })?;

        let req = writer::AppendRequest {
            writer_id: self.id,
            append_seq,
            batches,
        };
        let msg = ClientMessage::Append(req);
        let payload = encode_client_message_vec(&msg, 64 * 1024)?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(append_seq.0, tx);
        }

        let send_result = {
            let mut writer = self.writer.lock().await;
            writer.send(Message::Binary(payload)).await
        };

        if let Err(e) = send_result {
            let mut pending = self.pending.lock().await;
            pending.remove(&append_seq.0);
            return Err(SdkError::Connection(e.to_string()));
        }

        match tokio::time::timeout(self.config.timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SdkError::Disconnected),
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&append_seq.0);
                Err(SdkError::Timeout)
            }
        }
    }

    /// Append a single record (convenience method).
    pub async fn append_one(
        &self,
        topic_id: TopicId,
        schema_id: SchemaId,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<BatchAck, SdkError> {
        self.append(topic_id, schema_id, vec![Record { key, value }])
            .await
    }
}

fn encode_client_message_vec(msg: &ClientMessage, capacity: usize) -> Result<Vec<u8>, SdkError> {
    let mut buf_size = capacity.max(256);
    loop {
        let mut buf = vec![0u8; buf_size];
        match encode_client_message(msg, &mut buf) {
            Ok(len) => {
                buf.truncate(len);
                return Ok(buf);
            }
            Err(flux_wire::EncodeError::BufferTooSmall { needed, .. }) => {
                buf_size = needed.max(buf_size.saturating_mul(2));
            }
            Err(e) => return Err(SdkError::Encode(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_config_default() {
        let config = WriterConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_in_flight, 256);
    }

    #[test]
    fn test_append_request_encoding() {
        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(1),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("key")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let mut buf = vec![0u8; 1024];
        let len = writer::encode_request(&req, &mut buf);
        assert!(len > 0);

        let (decoded, _) = writer::decode_request(&buf[..len]).unwrap();
        assert_eq!(decoded.append_seq.0, 1);
        assert_eq!(decoded.batches.len(), 1);
    }

    #[test]
    fn test_backoff_calculation() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);

        let mut backoff = initial;
        for _ in 0..10 {
            backoff = (backoff * 2).min(max);
        }

        assert_eq!(backoff, max);
    }
}
