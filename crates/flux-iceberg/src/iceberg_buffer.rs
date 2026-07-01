// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Per-table buffer that accumulates committed records and flushes to Iceberg.
//!
//! Mirrors the `BrokerBuffer` pattern: records are pushed after each FL
//! flush, grouped by topic, and flushed when a size or time threshold
//! is crossed.  A separate tokio task drains the flush channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info};

use flux_common::ids::{SchemaId, TopicId};
use flux_common::types::{Record, RecordBatch};

use crate::config::IcebergConfig;
use crate::iceberg_writer::TableWriter;

/// A segment that has been committed to Postgres (offsets assigned).
#[derive(Debug, Clone)]
pub struct CommittedSegment {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
    pub records: Vec<Record>,
    pub start_offset: u64,
    pub end_offset: u64,
    pub batch_id: i64,
    pub ingest_time: DateTime<Utc>,
}

/// Flush request sent to the flush task.
pub struct FlushRequest {
    pub topic_id: TopicId,
    pub segments: Vec<CommittedSegment>,
    pub total_bytes: usize,
}

/// Per-topic accumulator.
struct TableBuffer {
    segments: Vec<CommittedSegment>,
    total_bytes: usize,
    first_record_at: Option<Instant>,
}

impl TableBuffer {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            total_bytes: 0,
            first_record_at: None,
        }
    }

    fn push(&mut self, segment: CommittedSegment) {
        let bytes: usize = segment.records.iter().map(|r| r.size()).sum();
        self.total_bytes += bytes;
        if self.first_record_at.is_none() {
            self.first_record_at = Some(Instant::now());
        }
        self.segments.push(segment);
    }

    fn drain(&mut self) -> (Vec<CommittedSegment>, usize) {
        self.first_record_at = None;
        let bytes = self.total_bytes;
        self.total_bytes = 0;
        (std::mem::take(&mut self.segments), bytes)
    }
}

/// Manages per-table buffers and dispatches flushes.
pub struct IcebergBuffer {
    tables: RwLock<HashMap<TopicId, TableBuffer>>,
    config: IcebergConfig,
    flush_tx: mpsc::Sender<FlushRequest>,
}

impl IcebergBuffer {
    /// Create a new buffer and spawn the flush loop.
    pub fn new(config: IcebergConfig, writer: Arc<TableWriter>) -> Arc<Self> {
        let (flush_tx, flush_rx) = mpsc::channel(64);
        let buf = Arc::new(Self {
            tables: RwLock::new(HashMap::new()),
            config: config.clone(),
            flush_tx,
        });

        let buf_clone = buf.clone();
        tokio::spawn(async move {
            flush_loop(flush_rx, writer, buf_clone).await;
        });

        buf
    }

    /// Push committed segments into per-topic buffers.
    ///
    /// Called from `execute_flush` after DB commit. The `Bytes` inside
    /// each `Record` are Arc-backed so cloning is O(1).
    pub async fn push_committed_batches(
        &self,
        batches: &[RecordBatch],
        segment_offsets: &[(u64, u64)],
        batch_ids: &[i64],
        ingest_time: DateTime<Utc>,
    ) {
        let mut tables = self.tables.write().await;
        let mut global_bytes: usize = tables.values().map(|t| t.total_bytes).sum();

        for (i, batch) in batches.iter().enumerate() {
            let (start_offset, end_offset) = segment_offsets[i];
            let batch_id = batch_ids.get(i).copied().unwrap_or(0);

            let segment = CommittedSegment {
                topic_id: batch.topic_id,
                schema_id: batch.schema_id,
                records: batch.records.clone(),
                start_offset,
                end_offset,
                batch_id,
                ingest_time,
            };

            let seg_bytes: usize = segment.records.iter().map(|r| r.size()).sum();
            global_bytes += seg_bytes;

            let table_buf = tables
                .entry(batch.topic_id)
                .or_insert_with(TableBuffer::new);
            table_buf.push(segment);

            if table_buf.total_bytes >= self.config.table_flush_size {
                let (segments, bytes) = table_buf.drain();
                global_bytes -= bytes;
                self.send_flush(batch.topic_id, segments, bytes).await;
            }
        }

        // Force-flush largest buffer if over global cap
        while global_bytes > self.config.global_memory_cap {
            let largest = tables
                .iter()
                .max_by_key(|(_, buf)| buf.total_bytes)
                .map(|(tid, _)| *tid);

            if let Some(tid) = largest {
                if let Some(buf) = tables.get_mut(&tid) {
                    if buf.total_bytes == 0 {
                        break;
                    }
                    let (segments, bytes) = buf.drain();
                    global_bytes -= bytes;
                    self.send_flush(tid, segments, bytes).await;
                }
            } else {
                break;
            }
        }
    }

    /// Check all tables for time-trigger and flush if needed.
    pub async fn check_time_triggers(&self) {
        let mut tables = self.tables.write().await;
        let deadline = self.config.table_flush_interval;

        let mut to_flush = Vec::new();
        for (tid, buf) in tables.iter() {
            if let Some(first) = buf.first_record_at
                && first.elapsed() >= deadline
                && buf.total_bytes > 0
            {
                to_flush.push(*tid);
            }
        }

        for tid in to_flush {
            if let Some(buf) = tables.get_mut(&tid) {
                let (segments, bytes) = buf.drain();
                self.send_flush(tid, segments, bytes).await;
            }
        }
    }

    /// Flush all remaining buffers (shutdown path).
    pub async fn flush_all(&self) {
        let mut tables = self.tables.write().await;
        for (tid, buf) in tables.iter_mut() {
            if buf.total_bytes > 0 {
                let (segments, bytes) = buf.drain();
                self.send_flush(*tid, segments, bytes).await;
            }
        }
    }

    async fn send_flush(
        &self,
        topic_id: TopicId,
        segments: Vec<CommittedSegment>,
        total_bytes: usize,
    ) {
        let req = FlushRequest {
            topic_id,
            segments,
            total_bytes,
        };
        if let Err(e) = self.flush_tx.send(req).await {
            error!("iceberg flush channel closed: {}", e);
        }
    }
}

/// Background task that processes flush requests.
async fn flush_loop(
    mut rx: mpsc::Receiver<FlushRequest>,
    writer: Arc<TableWriter>,
    buffer: Arc<IcebergBuffer>,
) {
    let mut time_check = tokio::time::interval(std::time::Duration::from_secs(10));

    loop {
        tokio::select! {
            Some(req) = rx.recv() => {
                let topic_id = req.topic_id;
                let record_count: usize = req.segments.iter().map(|s| s.records.len()).sum();
                debug!(
                    topic_id = topic_id.0,
                    segments = req.segments.len(),
                    records = record_count,
                    bytes = req.total_bytes,
                    "flushing to iceberg"
                );

                if let Err(e) = writer.write_segments(req.topic_id, req.segments).await {
                    error!(topic_id = topic_id.0, error = %e, "iceberg flush failed");
                }
            }
            _ = time_check.tick() => {
                buffer.check_time_triggers().await;
            }
            else => {
                info!("iceberg flush loop shutting down");
                break;
            }
        }
    }
}
