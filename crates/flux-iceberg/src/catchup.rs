// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Catch-up task: fills Iceberg gaps from topic_batches via S3 reads.
//!
//! Runs periodically on each broker. Uses claim-based concurrency
//! so multiple brokers don't duplicate work.

use std::sync::Arc;

use bytes::Bytes;
use sqlx::PgPool;
use tracing::{debug, error, info};

use flux_common::ids::{SchemaId, TopicId};

use crate::config::IcebergConfig;
use crate::iceberg_buffer::CommittedSegment;
use crate::iceberg_writer::TableWriter;
use crate::tracking::{self, UnclaimedBatch};

/// Async function type for reading byte ranges from S3.
pub type ReadRangeFn = Arc<
    dyn Fn(String, u64, u64) -> futures::future::BoxFuture<'static, std::io::Result<Bytes>>
        + Send
        + Sync,
>;

/// Spawn the catch-up background task.
pub fn spawn_catchup_task(
    config: IcebergConfig,
    pool: PgPool,
    writer: Arc<TableWriter>,
    read_fn: ReadRangeFn,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        catchup_loop(config, pool, writer, read_fn).await;
    })
}

async fn catchup_loop(
    config: IcebergConfig,
    pool: PgPool,
    writer: Arc<TableWriter>,
    read_fn: ReadRangeFn,
) {
    let mut interval = tokio::time::interval(config.catchup_interval);

    loop {
        interval.tick().await;

        // Expire stale claims
        match tracking::expire_stale_claims(&pool, config.claim_expiry.as_secs() as i64).await {
            Ok(expired) if expired > 0 => {
                info!(expired, "expired stale iceberg claims");
            }
            Err(e) => {
                error!(error = %e, "failed to expire stale claims");
            }
            _ => {}
        }

        // Get all topic IDs
        let topic_ids: Vec<i32> = match sqlx::query_scalar("SELECT topic_id FROM topics")
            .fetch_all(&pool)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                error!(error = %e, "failed to list topics for catchup");
                continue;
            }
        };

        if topic_ids.is_empty() {
            continue;
        }

        // Claim unclaimed batches
        let claimed =
            match tracking::claim_batches(&pool, &topic_ids, config.catchup_batch_limit).await {
                Ok(batches) => batches,
                Err(e) => {
                    error!(error = %e, "failed to claim batches for catchup");
                    continue;
                }
            };

        if claimed.is_empty() {
            debug!("catchup: no unclaimed batches");
            continue;
        }

        info!(count = claimed.len(), "catchup: processing claimed batches");

        // Group by S3 key to minimize reads
        let mut by_key: std::collections::HashMap<String, Vec<UnclaimedBatch>> =
            std::collections::HashMap::new();
        for batch in claimed {
            by_key.entry(batch.s3_key.clone()).or_default().push(batch);
        }

        for (s3_key, batches) in by_key {
            if let Err(e) = process_fl_file(&s3_key, &batches, &writer, &read_fn).await {
                error!(s3_key = %s3_key, error = %e, "catchup: failed to process FL file");
                continue;
            }

            let batch_ids: Vec<(i64, chrono::DateTime<chrono::Utc>)> = batches
                .iter()
                .map(|b| (b.batch_id, b.ingest_time))
                .collect();
            if let Err(e) = tracking::mark_completed(&pool, &batch_ids).await {
                error!(error = %e, "catchup: failed to mark batches completed");
            }
        }
    }
}

/// Read an FL file segment from S3, decode records, and write to Iceberg.
async fn process_fl_file(
    s3_key: &str,
    batches: &[UnclaimedBatch],
    writer: &TableWriter,
    read_fn: &ReadRangeFn,
) -> crate::Result<()> {
    let mut by_topic: std::collections::HashMap<TopicId, Vec<CommittedSegment>> =
        std::collections::HashMap::new();

    for batch in batches {
        let data = read_fn(
            s3_key.to_string(),
            batch.byte_offset as u64,
            batch.byte_length as u64,
        )
        .await
        .map_err(crate::IcebergError::Io)?;

        // Decompress and decode records
        let decompressed = zstd::decode_all(data.as_ref()).map_err(|e| {
            crate::IcebergError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;

        let mut records = Vec::new();
        let mut offset = 0;
        while offset < decompressed.len() {
            match flux_wire::record::decode(&decompressed[offset..]) {
                Ok((rec, len)) => {
                    records.push(rec);
                    offset += len;
                }
                Err(_) if records.len() == batch.record_count as usize => break,
                Err(e) => {
                    return Err(crate::IcebergError::Conversion(format!(
                        "decode record: {e}"
                    )));
                }
            }
        }

        let segment = CommittedSegment {
            topic_id: TopicId(batch.topic_id as u32),
            schema_id: SchemaId(batch.schema_id as u32),
            records,
            start_offset: batch.start_offset as u64,
            end_offset: batch.end_offset as u64,
            batch_id: batch.batch_id,
            ingest_time: batch.ingest_time,
        };

        by_topic
            .entry(TopicId(batch.topic_id as u32))
            .or_default()
            .push(segment);
    }

    for (topic_id, segments) in by_topic {
        writer.write_segments(topic_id, segments).await?;
    }

    Ok(())
}
