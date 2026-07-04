// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! End-to-end tests driving a real broker through the Rust SDK's group
//! consumer, covering pipelined polling in classic and raw (zero-copy) modes.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
use tempfile::TempDir;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_sdk::{Reader, ReaderConfig};
use flux_wire::{ClientMessage, writer};

use common::TestDb;
use common::ws_helpers::{self, encode_client_frame};

async fn start_broker(pool: sqlx::PgPool, temp_dir: &TempDir) -> std::net::SocketAddr {
    let addr = ws_helpers::find_available_port().await;
    let config = flux_broker::BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: flux_broker::buffer::BufferConfig::default(),
        flush_interval: Duration::from_millis(50),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
        readahead_max_bytes: 0,
            flush_pipeline_depth: 4,
        #[cfg(feature = "iceberg")]
        iceberg: None,
    };
    let store = flux_broker::LocalFsStore::new(temp_dir.path().to_path_buf());
    let state = flux_broker::BrokerState::new(pool, store, config).await;
    tokio::spawn(async move {
        let _ = flux_broker::run(state).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    addr
}

async fn produce(url: &str, topic_id: TopicId, count: usize) {
    let (mut ws, _) = connect_async(url).await.expect("connect");
    let writer_id = WriterId::new();
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            key: Some(Bytes::from(format!("k{i}"))),
            value: Bytes::from(format!("value-{i}")),
        })
        .collect();
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records,
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Append(req),
        256 * 1024,
    )))
    .await
    .expect("send append");
    // Wait for the ack + flush.
    tokio::time::sleep(Duration::from_millis(300)).await;
    ws.close(None).await.ok();
}

async fn consume_all(url: &str, topic_id: TopicId, raw: bool, expected: usize) {
    let config = ReaderConfig {
        url: url.to_string(),
        group_id: format!("sdk-e2e-{}", if raw { "raw" } else { "classic" }),
        topic_id,
        max_bytes: 64 * 1024,
        raw,
        pipeline_depth: 3,
        ..Default::default()
    };
    let reader = Reader::join(config).await.expect("join");

    let mut seen = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen.len() < expected && std::time::Instant::now() < deadline {
        let batch = reader.poll().await.expect("poll");
        for result in &batch.results {
            for rec in &result.records {
                seen.push(rec.value.clone());
            }
        }
        reader.commit(&batch).await.expect("commit");
    }

    assert_eq!(seen.len(), expected, "should consume every record");
    for (i, value) in seen.iter().enumerate() {
        assert_eq!(value.as_ref(), format!("value-{i}").as_bytes());
    }
    reader.stop().await.ok();
}

#[tokio::test]
async fn test_sdk_pipelined_classic_consume() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("sdk-classic").await as u32);
    let temp_dir = TempDir::new().unwrap();
    let addr = start_broker(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{addr}");

    produce(&url, topic_id, 500).await;
    consume_all(&url, topic_id, false, 500).await;
}

#[tokio::test]
async fn test_sdk_pipelined_raw_consume() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("sdk-raw").await as u32);
    let temp_dir = TempDir::new().unwrap();
    let addr = start_broker(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{addr}");

    produce(&url, topic_id, 500).await;
    consume_all(&url, topic_id, true, 500).await;
}
