// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Shared record-fetch logic used by both direct reads and poll-based reads.

use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_wire::reader;

use crate::BrokerError;
use crate::fl::{Codec, FlReader, SegmentMeta};
use crate::object_store::ObjectStore;

use super::BrokerState;

/// Fetch records for a topic starting at `start_offset`.
///
/// If `end_offset` is `Some`, only batches whose `start_offset < end_offset` are
/// included (bounded read, used by poll). If `None`, the read is open-ended
/// (direct read).
pub(crate) async fn fetch_records<S: ObjectStore + Send + Sync>(
    topic_id: TopicId,
    start_offset: Offset,
    end_offset: Option<Offset>,
    max_bytes: usize,
    state: &BrokerState<S>,
) -> Result<(Vec<reader::TopicResult>, Offset /* high_watermark */), BrokerError> {
    let batches: Vec<(i32, i64, i64, String, i64, i64, i64)> = if let Some(end) = end_offset {
        sqlx::query_as(
            r#"
            SELECT schema_id, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32
            FROM topic_batches
            WHERE topic_id = $1
              AND end_offset > $2
              AND start_offset < $3
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(topic_id.0 as i32)
        .bind(start_offset.0 as i64)
        .bind(end.0 as i64)
        .fetch_all(&state.pool)
        .await?
    } else {
        sqlx::query_as(
            r#"
            SELECT schema_id, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32
            FROM topic_batches
            WHERE topic_id = $1
              AND end_offset > $2
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(topic_id.0 as i32)
        .bind(start_offset.0 as i64)
        .fetch_all(&state.pool)
        .await?
    };

    let high_watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM topic_offsets WHERE topic_id = $1",
    )
    .bind(topic_id.0 as i32)
    .fetch_optional(&state.pool)
    .await?
    .unwrap_or(0);

    let mut grouped_results = Vec::with_capacity(batches.len().max(1));
    let mut current_schema: Option<SchemaId> = None;
    let expected_records = batches
        .iter()
        .map(|(_, start, end, _, _, _, _)| end.saturating_sub(*start) as usize)
        .sum::<usize>();
    let mut current_records = Vec::with_capacity(expected_records.min(16_384));
    let mut total_bytes: usize = 0;
    let mut reached_limit = false;

    for (sid, batch_start, batch_end, s3_key, byte_offset, byte_length, crc32) in batches {
        let schema_id = SchemaId(sid as u32);

        let segment_bytes = state
            .store
            .get_range(&s3_key, byte_offset as u64, byte_length as u64)
            .await?;

        let meta = SegmentMeta {
            topic_id,
            schema_id,
            start_offset: Offset(batch_start as u64),
            end_offset: Offset(batch_end as u64),
            record_count: 0,
            byte_offset: 0,
            byte_length: byte_length as u64,
            ingest_time: 0,
            compression: Codec::Zstd,
            crc32: crc32 as u32,
        };

        let records = FlReader::read_segment(&segment_bytes, &meta, true)?;
        let skip = (start_offset.0 as i64 - batch_start).max(0) as usize;
        if skip >= records.len() {
            continue;
        }

        if current_schema != Some(schema_id) {
            if let Some(prev_schema) = current_schema
                && !current_records.is_empty()
            {
                grouped_results.push(reader::TopicResult {
                    topic_id,
                    schema_id: prev_schema,
                    high_watermark: Offset(high_watermark as u64),
                    records: std::mem::take(&mut current_records),
                });
            }
            current_schema = Some(schema_id);
        }

        let mut record_offset = batch_start as u64 + skip as u64;
        for record in records.into_iter().skip(skip) {
            if let Some(end) = end_offset
                && record_offset >= end.0
            {
                break;
            }
            record_offset += 1;
            total_bytes += record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
            current_records.push(record);
            if total_bytes >= max_bytes {
                reached_limit = true;
                break;
            }
        }

        if reached_limit {
            break;
        }
    }

    if let Some(schema_id) = current_schema {
        if !current_records.is_empty() {
            grouped_results.push(reader::TopicResult {
                topic_id,
                schema_id,
                high_watermark: Offset(high_watermark as u64),
                records: current_records,
            });
        }
    } else {
        grouped_results.push(reader::TopicResult {
            topic_id,
            schema_id: SchemaId(0),
            high_watermark: Offset(high_watermark as u64),
            records: vec![],
        });
    }

    Ok((grouped_results, Offset(high_watermark as u64)))
}
