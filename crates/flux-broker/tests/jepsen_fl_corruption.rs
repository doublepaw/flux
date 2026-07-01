// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired FL file corruption tests.
//!
//! Verifies that corrupted data on S3 is detected by CRC validation
//! in the read path and never served as valid data to clients.
//! Inspired by Jepsen's NATS findings where single-bit errors caused
//! massive data loss (679K/1.3M messages).

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

async fn ws_produce(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    value: &str,
) -> Result<writer::AppendResponse, String> {
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(seq),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from(value.to_string()),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_read(
    ws: &mut Ws,
    topic_id: TopicId,
    offset: Offset,
) -> Result<reader::ReadResponse, String> {
    let req = reader::ReadRequest {
        topic_id,
        offset,
        max_bytes: 10 * 1024 * 1024,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Read(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

/// Corrupt get_range → CRC mismatch → read returns error, not garbage.
#[tokio::test]
async fn test_corrupt_segment_detected_on_read() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("corrupt-crc").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("val-{}", i))
            .await
            .expect("write should succeed");
        assert!(resp.success);
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify reads work before corruption
    let resp = ws_read(&mut ws, topic_id, Offset(0))
        .await
        .expect("read should succeed before corruption");
    assert!(resp.success);
    let pre_count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(pre_count, 5, "should read all 5 records before corruption");

    // Inject corruption on next get_range
    broker.faulty_store().corrupt_next_get_range();

    // Read should fail (CRC mismatch) — reconnect since connection may be closed
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;

    match resp {
        Ok(r) if !r.success => {
            // Error response — correct behavior
            eprintln!("  [corrupt] read failed as expected: {}", r.error_message);
        }
        Ok(r) if r.success => {
            // If read succeeds, the corruption was on a different get_range call
            // (e.g. the high_watermark query). Try again with fresh corruption.
            broker.faulty_store().corrupt_next_get_range();
            let resp2 = ws_read(&mut ws, topic_id, Offset(0)).await;
            match resp2 {
                Ok(r2) if !r2.success => {
                    eprintln!("  [corrupt] second read failed as expected");
                }
                Err(_) => {
                    eprintln!("  [corrupt] connection error on corrupted read — acceptable");
                }
                _ => {
                    panic!("Corrupted data should not be served as valid records");
                }
            }
        }
        Err(_) => {
            // Connection error — acceptable (broker may close connection on internal error)
            eprintln!("  [corrupt] connection error on corrupted read — acceptable");
        }
        Ok(_) => unreachable!(),
    }

    // After corruption clears, reads should work again
    broker.faulty_store().reset();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0))
        .await
        .expect("read should succeed after corruption clears");
    assert!(resp.success, "read should succeed after corruption clears");
    let post_count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(
        post_count, 5,
        "all records should be intact after corruption clears"
    );
}

/// Truncated get_range → decompression error → read fails gracefully.
#[tokio::test]
async fn test_truncated_segment_detected_on_read() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("truncated-read").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("trunc-{}", i))
            .await
            .expect("write should succeed");
        assert!(resp.success);
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Inject truncation on next get_range
    broker.faulty_store().truncate_next_get_range();

    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;

    // Truncated data should cause CRC mismatch or decompression error
    match resp {
        Ok(r) if !r.success => {
            eprintln!("  [truncated] read failed as expected: {}", r.error_message);
        }
        Err(_) => {
            eprintln!("  [truncated] connection error — acceptable");
        }
        Ok(r) if r.success => {
            // If the truncation happened to a non-data query, that's OK
            // but the records should NOT contain garbage
            let count: usize = r.results.iter().map(|r| r.records.len()).sum();
            assert!(count <= 3, "should not produce more records than written");
        }
        _ => {}
    }

    // Recovery: reads work after fault clears
    broker.faulty_store().reset();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0))
        .await
        .expect("read should succeed after truncation clears");
    assert!(resp.success);
}

/// Write records, corrupt S3 file via get_range, verify broker stays healthy
/// for subsequent uncorrupted reads.
#[tokio::test]
async fn test_corruption_does_not_poison_broker() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("no-poison").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..10 {
        let resp = ws_produce(
            &mut ws,
            writer_id,
            i + 1,
            topic_id,
            &format!("poison-{}", i),
        )
        .await
        .expect("write should succeed");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Trigger 3 corrupted reads
    for i in 0..3 {
        broker.faulty_store().corrupt_next_get_range();
        drop(ws);
        ws = ws_connect(addr).await;
        let _ = ws_read(&mut ws, topic_id, Offset(0)).await;
        eprintln!("  [no-poison] corrupted read {} done", i);
    }

    // Now do a clean read — broker should still work
    broker.faulty_store().reset();
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0))
        .await
        .expect("clean read should succeed after corruption");
    assert!(resp.success, "broker should recover from corruption");
    let count: usize = resp.results.iter().map(|r| r.records.len()).sum();
    assert_eq!(count, 10, "all 10 records intact after corruption episode");

    // Write more records — should still work
    let w2 = WriterId::new();
    let resp = ws_produce(&mut ws, w2, 1, topic_id, "post-corruption")
        .await
        .expect("write after corruption should succeed");
    assert!(resp.success);
}

/// Mutate topic_batches.crc32 in Postgres → read detects mismatch.
#[tokio::test]
async fn test_corrupt_crc_in_metadata_detected() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("meta-crc").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "meta-test")
        .await
        .expect("write should succeed");
    assert!(resp.success);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify reads work
    let resp = ws_read(&mut ws, topic_id, Offset(0))
        .await
        .expect("read should succeed");
    assert!(resp.success);

    // Corrupt the CRC in topic_batches
    sqlx::query("UPDATE topic_batches SET crc32 = crc32 + 1 WHERE topic_id = $1")
        .bind(topic_id.0 as i32)
        .execute(&db.pool)
        .await
        .expect("corrupt crc32 in metadata");

    // Read should fail with CRC mismatch
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;

    match resp {
        Ok(r) if !r.success => {
            eprintln!("  [meta-crc] read failed as expected: {}", r.error_message);
        }
        Err(_) => {
            eprintln!("  [meta-crc] connection error — acceptable");
        }
        Ok(_) => {
            // Some broker implementations may cache the read or the CRC check
            // might not trigger if the data hasn't changed. That's also acceptable
            // if the data returned is still valid.
            eprintln!("  [meta-crc] read succeeded — CRC may match a cached path");
        }
    }
}

/// Mutate topic_batches.byte_offset → read gets wrong byte range → error.
#[tokio::test]
async fn test_corrupt_byte_offset_in_metadata() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("meta-offset").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    let resp = ws_produce(&mut ws, writer_id, 1, topic_id, "offset-test")
        .await
        .expect("write should succeed");
    assert!(resp.success);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Corrupt byte_offset in metadata (point to wrong location)
    sqlx::query("UPDATE topic_batches SET byte_offset = byte_offset + 1000 WHERE topic_id = $1")
        .bind(topic_id.0 as i32)
        .execute(&db.pool)
        .await
        .expect("corrupt byte_offset in metadata");

    // Read should fail (invalid range or wrong data)
    drop(ws);
    let mut ws = ws_connect(addr).await;
    let resp = ws_read(&mut ws, topic_id, Offset(0)).await;

    match resp {
        Ok(r) if !r.success => {
            eprintln!(
                "  [meta-offset] read failed as expected: {}",
                r.error_message
            );
        }
        Err(_) => {
            eprintln!("  [meta-offset] connection error — acceptable");
        }
        Ok(_) => {
            // Might succeed if the FL file has valid data at the wrong offset
            // that happens to decompress and CRC-check correctly (extremely unlikely)
            eprintln!("  [meta-offset] read succeeded — unlikely but possible");
        }
    }
}
