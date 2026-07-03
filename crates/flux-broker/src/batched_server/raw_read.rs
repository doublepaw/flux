// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Zero-copy read handling: compressed FL segments are shipped to the
//! client as-is (metadata + payload), with no broker-side decompression or
//! record re-encoding. The SDK decodes locally. `max_bytes` budgets
//! *compressed* bytes.

use std::sync::Arc;

use futures::StreamExt;
use tracing::error;

use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_wire::{ERR_INTERNAL_ERROR, STATUS_OK, ServerMessage, reader};

use crate::BrokerError;
use crate::metrics::{LatencyTimer, READ_LATENCY_SECONDS, READ_REQUESTS_TOTAL};
use crate::object_store::ObjectStore;

use super::BrokerState;
use super::encoding::encode_server_message_vec;
use super::fetch::{MAX_BATCHES_PER_FETCH, SEGMENT_FETCH_CONCURRENCY};

type RawWindow = (Vec<reader::RawSegment>, Offset);

pub(crate) async fn handle_raw_read_request<S: ObjectStore + Send + Sync + 'static>(
    req: reader::RawReadRequest,
    state: &Arc<BrokerState<S>>,
) -> Vec<u8> {
    let _timer = LatencyTimer::new(&READ_LATENCY_SECONDS);
    READ_REQUESTS_TOTAL.inc();

    let cache_key = (req.topic_id.0, req.offset.0);
    let result = match state.raw_readahead.take(cache_key) {
        Some((segments, high_watermark)) => Ok((segments, high_watermark)),
        None => fetch_raw_segments(req.topic_id, req.offset, None, req.max_bytes as usize, state).await,
    };

    // Prefetch the next window while the consumer decodes this one.
    if let Ok((segments, high_watermark)) = &result {
        let next_offset = segments.last().map(|s| s.end_offset.0);
        if let Some(next_offset) = next_offset
            && next_offset < high_watermark.0
        {
            let next_key = (req.topic_id.0, next_offset);
            if state.raw_readahead.try_begin(next_key) {
                let state = state.clone();
                let topic_id = req.topic_id;
                let max_bytes = req.max_bytes as usize;
                tokio::spawn(async move {
                    let prefetched =
                        fetch_raw_segments(topic_id, Offset(next_offset), None, max_bytes, &state)
                            .await
                            .ok()
                            .map(|(segments, hwm)| {
                                let bytes = segments.iter().map(|s| s.payload.len()).sum();
                                (((segments, hwm)), bytes)
                            });
                    state.raw_readahead.complete(next_key, prefetched);
                });
            }
        }
    }

    let response = match result {
        Ok((segments, high_watermark)) => reader::RawReadResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            high_watermark,
            segments,
        },
        Err(e) => {
            error!("Raw read error: {}", e);
            reader::RawReadResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "read failed".to_string(),
                high_watermark: Offset(0),
                segments: vec![],
            }
        }
    };

    let payload_bytes: usize = response.segments.iter().map(|s| s.payload.len()).sum();
    let buf_size = (payload_bytes + 4096).max(64 * 1024);
    encode_server_message_vec(ServerMessage::RawRead(response), buf_size + 16)
}

/// Select index rows covering `max_bytes` of *compressed* data starting at
/// `offset`, fetch the raw segment ranges concurrently, and return them
/// untouched.
pub(crate) async fn fetch_raw_segments<S: ObjectStore + Send + Sync>(
    topic_id: TopicId,
    offset: Offset,
    end_offset: Option<Offset>,
    max_bytes: usize,
    state: &BrokerState<S>,
) -> Result<RawWindow, BrokerError> {
    let rows: Vec<(i32, i64, i64, String, i64, i64, i64, i64)> = sqlx::query_as(
        r#"
        WITH candidate AS (
            SELECT schema_id, start_offset, end_offset, s3_key,
                   byte_offset, byte_length, crc32,
                   COALESCE(SUM(byte_length) OVER (
                       ORDER BY start_offset
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ), 0) AS running
            FROM topic_batches
            WHERE topic_id = $1
              AND end_offset > $2
              AND ($3::bigint IS NULL OR start_offset < $3)
            ORDER BY start_offset
            LIMIT $4
        )
        SELECT c.schema_id, c.start_offset, c.end_offset, c.s3_key,
               c.byte_offset, c.byte_length, c.crc32,
               (SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1)
        FROM candidate c
        WHERE c.running < $5
        ORDER BY c.start_offset
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(offset.0 as i64)
    .bind(end_offset.map(|e| e.0 as i64))
    .bind(MAX_BATCHES_PER_FETCH)
    .bind(i64::try_from(max_bytes).unwrap_or(i64::MAX))
    .fetch_all(&state.pool)
    .await?;

    let high_watermark: i64 = match rows.first() {
        Some(row) => row.7,
        None => sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
        )
        .bind(topic_id.0 as i32)
        .fetch_optional(&state.pool)
        .await?
        .unwrap_or(0),
    };

    let segments: Vec<Result<reader::RawSegment, BrokerError>> =
        futures::stream::iter(rows.into_iter().map(
            |(sid, batch_start, batch_end, s3_key, byte_offset, byte_length, crc32, _)| {
                let store = state.store.clone();
                async move {
                    let payload = store
                        .get_range(&s3_key, byte_offset as u64, byte_length as u64)
                        .await?;
                    Ok(reader::RawSegment {
                        topic_id,
                        schema_id: SchemaId(sid as u32),
                        start_offset: Offset(batch_start as u64),
                        end_offset: Offset(batch_end as u64),
                        crc32: crc32 as u32,
                        payload,
                    })
                }
            },
        ))
        .buffered(SEGMENT_FETCH_CONCURRENCY)
        .collect()
        .await;

    let segments = segments.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok((segments, Offset(high_watermark as u64)))
}
