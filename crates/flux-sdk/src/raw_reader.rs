// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Zero-copy reader: fetches compressed FL segments from the broker and
//! decodes them locally (`flux_wire::segment`), keeping broker CPU out of
//! the read path. Suitable for high-throughput catch-up and backfill reads;
//! for coordinated group consumption use [`crate::Reader`].

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_common::types::Record;
use flux_wire::{ClientMessage, ServerMessage, decode_server_message, encode_client_message, reader, segment};

use crate::SdkError;

/// Configuration for [`RawReader`].
#[derive(Debug, Clone)]
pub struct RawReaderConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// Topic to read.
    pub topic_id: TopicId,
    /// Budget in *compressed* bytes per read.
    pub max_bytes: u32,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for RawReaderConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            topic_id: TopicId(1),
            max_bytes: 8 * 1024 * 1024,
            timeout: Duration::from_secs(30),
        }
    }
}

/// One decoded segment from a raw read.
#[derive(Debug)]
pub struct RawSegmentResult {
    pub schema_id: SchemaId,
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub records: Vec<Record>,
}

/// Result of one raw read.
#[derive(Debug)]
pub struct RawReadResult {
    pub high_watermark: Offset,
    pub segments: Vec<RawSegmentResult>,
}

/// Zero-copy reader over a dedicated connection.
pub struct RawReader {
    config: RawReaderConfig,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RawReader {
    /// Connect to the broker.
    pub async fn connect(config: RawReaderConfig) -> Result<Self, SdkError> {
        let ws_config = WebSocketConfig {
            max_message_size: Some(1024 * 1024 * 1024),
            max_frame_size: Some(1024 * 1024 * 1024),
            ..Default::default()
        };
        let (ws, _) = connect_async_with_config(&config.url, Some(ws_config), false)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;
        Ok(Self { config, ws })
    }

    /// Read up to `max_bytes` of compressed segments starting at `offset`,
    /// decoding (with CRC verification) locally.
    pub async fn read(&mut self, offset: Offset) -> Result<RawReadResult, SdkError> {
        let req = reader::RawReadRequest {
            topic_id: self.config.topic_id,
            offset,
            max_bytes: self.config.max_bytes,
        };

        let mut buf = vec![0u8; 64 * 1024];
        let len = encode_client_message(&ClientMessage::RawRead(req), &mut buf)
            .map_err(|e| SdkError::Protocol(format!("encode raw read: {e:?}")))?;
        buf.truncate(len);
        self.ws
            .send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let msg = tokio::time::timeout(self.config.timeout, self.ws.next())
            .await
            .map_err(|_| SdkError::Timeout)?
            .ok_or_else(|| SdkError::Connection("connection closed".into()))?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let data = match msg {
            Message::Binary(data) => data,
            other => return Err(SdkError::Protocol(format!("unexpected frame: {other:?}"))),
        };
        let (decoded, _) = decode_server_message(&data)
            .map_err(|e| SdkError::Protocol(format!("decode: {e:?}")))?;
        let resp = match decoded {
            ServerMessage::RawRead(resp) => resp,
            other => {
                return Err(SdkError::Protocol(format!(
                    "unexpected server message: {other:?}"
                )));
            }
        };
        if !resp.success {
            return Err(SdkError::Server {
                code: resp.error_code,
                message: resp.error_message,
            });
        }

        let mut segments = Vec::with_capacity(resp.segments.len());
        for seg in resp.segments {
            let records = segment::decode_segment(&seg.payload, Some(seg.crc32))
                .map_err(|e| SdkError::Protocol(format!("segment decode: {e}")))?;
            segments.push(RawSegmentResult {
                schema_id: seg.schema_id,
                start_offset: seg.start_offset,
                end_offset: seg.end_offset,
                records,
            });
        }

        Ok(RawReadResult {
            high_watermark: resp.high_watermark,
            segments,
        })
    }
}
