// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Flush loop: buffer management, S3 writes, and database commits.
//!
//! The flush loop decouples write ingestion from flush I/O via pipelining:
//! while a flush (FL build → S3 put → DB commit → ack distribute) runs on
//! a spawned task, the loop continues accepting new writes into the buffer.
//! At most one flush is in flight at a time, preserving offset ordering and
//! dedup correctness.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bumpalo::collections::Vec as BumpVec;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::{debug, error};

use flux_common::ids::{AppendSeq, WriterId};
use flux_common::types::RecordBatch;

use crate::buffer::{BatchKey, BrokerBuffer, DrainResult};
use crate::fl::FlWriter;
use crate::metrics::{
    BACKPRESSURE_ACTIVE, BUFFER_SIZE_BYTES, DB_COMMIT_LATENCY_SECONDS, DB_COMMIT_OFFSETS_SECONDS,
    DB_COMMIT_TOPIC_BATCHES_SECONDS, DB_COMMIT_TX_COMMIT_SECONDS,
    DB_COMMIT_WRITER_STATE_PREP_SECONDS, DB_COMMIT_WRITER_STATE_UPSERT_SECONDS, ERRORS_TOTAL,
    FLUSH_ACK_DISTRIBUTE_SECONDS, FLUSH_BATCH_PENDING_WRITERS, FLUSH_BATCH_RECORDS,
    FLUSH_BATCH_SEGMENTS, FLUSH_BUFFER_RESIDENCY_SECONDS, FLUSH_FL_BUILD_SECONDS,
    FLUSH_LATENCY_SECONDS, FLUSH_QUEUE_DEPTH, FLUSH_QUEUE_WAIT_SECONDS, FLUSH_TOTAL,
    S3_PUT_LATENCY_SECONDS,
};
use crate::object_store::ObjectStore;

use super::{BrokerState, FlushCommand, WRITER_STATE_ROWS_BUMP};

/// Flush loop that buffers requests and periodically flushes to S3.
///
/// Writes accumulate in the buffer while the previous flush runs on a
/// spawned task. A completion channel signals when the flush finishes,
/// at which point backpressure is released and the next flush can start.
#[tracing::instrument(level = "debug", skip(rx, state))]
pub(crate) async fn flush_loop<S: ObjectStore + Send + Sync + 'static>(
    mut rx: mpsc::Receiver<FlushCommand>,
    state: Arc<BrokerState<S>>,
) {
    let mut buffer = BrokerBuffer::with_config(state.config.buffer.clone());
    let mut flush_interval = tokio::time::interval(state.config.flush_interval);
    let (flush_done_tx, mut flush_done_rx) = mpsc::channel::<bool>(1);
    let mut flush_in_flight = false;
    const MAX_COMMANDS_PER_DRAIN: usize = 2048;

    loop {
        tokio::select! {
            _ = state.cancel_token.cancelled() => {
                debug!("Cancel token triggered, stopping flush loop");
                break;
            }
            Some(cmd) = rx.recv() => {
                let mut should_force_flush = false;
                let mut should_shutdown = false;
                let mut processed = 0usize;
                let mut maybe_cmd = Some(cmd);

                while let Some(command) = maybe_cmd.take() {
                    processed += 1;
                    match command {
                        FlushCommand::Insert { writer_id, append_seq, batches, enqueued_at, response_tx } => {
                            let previous = state.pending_flush_commands.fetch_sub(1, Ordering::Relaxed);
                            FLUSH_QUEUE_DEPTH.set(previous.saturating_sub(1) as f64);
                            FLUSH_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());

                            // Insert into buffer
                            buffer.insert_with_sender(writer_id, append_seq, batches, response_tx);

                            // Update backpressure state
                            if buffer.should_apply_backpressure() {
                                *state.backpressure.write().await = true;
                                BACKPRESSURE_ACTIVE.set(1.0);
                            }
                        }
                        FlushCommand::ForceFlush => {
                            should_force_flush = true;
                        }
                        FlushCommand::Shutdown => {
                            should_shutdown = true;
                            break;
                        }
                    }

                    if processed >= MAX_COMMANDS_PER_DRAIN {
                        break;
                    }

                    maybe_cmd = rx.try_recv().ok();
                }

                BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);

                if (should_force_flush || buffer.should_flush()) && !flush_in_flight {
                    flush_in_flight = try_start_flush(&mut buffer, &state, &flush_done_tx);
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                }

                if should_shutdown {
                    // Wait for in-flight flush to complete
                    if flush_in_flight
                        && let Some(succeeded) = flush_done_rx.recv().await
                    {
                        handle_flush_done(succeeded, &mut buffer, &state).await;
                    }
                    // Flush any remaining buffered data synchronously
                    if !buffer.is_empty() {
                        let drain_result = buffer.drain();
                        record_drain_metrics(&drain_result);
                        execute_flush(drain_result, &state).await;
                    }
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                    break;
                }
            }
            _ = flush_interval.tick() => {
                // Periodic flush
                if !buffer.is_empty() && !flush_in_flight {
                    flush_in_flight = try_start_flush(&mut buffer, &state, &flush_done_tx);
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                }
            }
            Some(succeeded) = flush_done_rx.recv() => {
                flush_in_flight = false;
                handle_flush_done(succeeded, &mut buffer, &state).await;

                // Start next flush immediately if buffer has pending data
                if !buffer.is_empty() {
                    flush_in_flight = try_start_flush(&mut buffer, &state, &flush_done_tx);
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                }
            }
        }
    }
}

/// Release backpressure if the buffer is below the low water mark after a
/// successful flush.
async fn handle_flush_done<S: ObjectStore + Send + Sync>(
    succeeded: bool,
    buffer: &mut BrokerBuffer,
    state: &BrokerState<S>,
) {
    if succeeded && buffer.can_release_backpressure() {
        *state.backpressure.write().await = false;
        BACKPRESSURE_ACTIVE.set(0.0);
    }
}

/// Drain the buffer and spawn a flush task. Returns true if a flush was
/// actually started (false if the buffer was empty after drain).
fn try_start_flush<S: ObjectStore + Send + Sync + 'static>(
    buffer: &mut BrokerBuffer,
    state: &Arc<BrokerState<S>>,
    done_tx: &mpsc::Sender<bool>,
) -> bool {
    let drain_result = buffer.drain();
    if drain_result.batches.is_empty() {
        return false;
    }

    record_drain_metrics(&drain_result);

    let state = state.clone();
    let done_tx = done_tx.clone();
    tokio::spawn(async move {
        let succeeded = execute_flush(drain_result, &state).await;
        let _ = done_tx.send(succeeded).await;
    });

    true
}

/// Record pre-flush metrics from the drain result.
fn record_drain_metrics(drain_result: &DrainResult) {
    let total_records: usize = drain_result
        .batches
        .iter()
        .map(|batch| batch.records.len())
        .sum();
    FLUSH_BATCH_SEGMENTS.observe(drain_result.batches.len() as f64);
    FLUSH_BATCH_PENDING_WRITERS.observe(drain_result.pending_writers.len() as f64);
    FLUSH_BATCH_RECORDS.observe(total_records as f64);
    let now = Instant::now();
    for pending in &drain_result.pending_writers {
        FLUSH_BUFFER_RESIDENCY_SECONDS
            .observe(now.duration_since(pending.buffered_at).as_secs_f64());
    }
}

/// Build FL, write to S3, commit to DB, and distribute acks.
/// Returns true on success.
#[tracing::instrument(level = "debug", skip(drain_result, state))]
async fn execute_flush<S: ObjectStore + Send + Sync>(
    drain_result: DrainResult,
    state: &BrokerState<S>,
) -> bool {
    let flush_start = Instant::now();

    debug!(
        "Flushing {} batches, {} pending writers",
        drain_result.batches.len(),
        drain_result.pending_writers.len()
    );

    // Generate S3 key.
    // The counter ensures uniqueness even when orphaned flush tasks from a
    // crashed broker overlap with new broker flush tasks in the same millisecond.
    static FLUSH_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = FLUSH_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = chrono::Utc::now();
    let key = format!(
        "{}/{}/{}-{}.fl",
        state.config.key_prefix,
        now.format("%Y-%m-%d"),
        now.timestamp_millis(),
        counter
    );

    // Build FL file
    let fl_start = Instant::now();
    let (segment_metas, fl_data) = {
        let _span = tracing::debug_span!("fl_build").entered();
        let mut writer = FlWriter::new();
        for batch in &drain_result.batches {
            if let Err(e) = writer.add_segment(batch) {
                error!("Failed to add batch to FL: {}", e);
                FLUSH_TOTAL.with_label_values(&["error"]).inc();
                ERRORS_TOTAL.with_label_values(&["fl_write"]).inc();
                return false;
            }
        }
        (writer.segment_metas().to_vec(), writer.finish())
    };
    FLUSH_FL_BUILD_SECONDS.observe(fl_start.elapsed().as_secs_f64());

    // Write to S3
    let s3_start = Instant::now();
    if let Err(e) = state
        .store
        .put(&key, fl_data)
        .instrument(tracing::debug_span!("s3_put"))
        .await
    {
        error!("Failed to write to S3: {}", e);
        FLUSH_TOTAL.with_label_values(&["error"]).inc();
        ERRORS_TOTAL.with_label_values(&["s3_put"]).inc();
        return false;
    }
    S3_PUT_LATENCY_SECONDS.observe(s3_start.elapsed().as_secs_f64());

    // Commit to database
    let db_start = Instant::now();
    let ingest_time = chrono::Utc::now();
    let succeeded = match commit_batch(
        &drain_result.batches,
        &segment_metas,
        &drain_result.key_to_index,
        &drain_result.pending_writers,
        &key,
        &state.pool,
        ingest_time,
    )
    .instrument(tracing::debug_span!("db_commit_batch"))
    .await
    {
        Ok((segment_offsets, _batch_ids)) => {
            DB_COMMIT_LATENCY_SECONDS.observe(db_start.elapsed().as_secs_f64());

            // Push to Iceberg buffer if enabled (Bytes clone is O(1))
            #[cfg(feature = "iceberg")]
            if let Some(ref iceberg_buf) = state.iceberg_buffer {
                iceberg_buf
                    .push_committed_batches(
                        &drain_result.batches,
                        &segment_offsets,
                        &_batch_ids,
                        ingest_time,
                    )
                    .await;
            }

            // Distribute append_acks to writers
            let ack_start = Instant::now();
            {
                let _span = tracing::debug_span!("distribute_acks").entered();
                BrokerBuffer::distribute_acks(drain_result, &segment_offsets);
            }
            FLUSH_ACK_DISTRIBUTE_SECONDS.observe(ack_start.elapsed().as_secs_f64());

            FLUSH_TOTAL.with_label_values(&["success"]).inc();
            true
        }
        Err(e) => {
            error!("Failed to commit batch: {}", e);
            FLUSH_TOTAL.with_label_values(&["error"]).inc();
            ERRORS_TOTAL.with_label_values(&["db_commit"]).inc();
            // Acks won't be sent, writers will retry
            false
        }
    };

    FLUSH_LATENCY_SECONDS.observe(flush_start.elapsed().as_secs_f64());
    succeeded
}

/// Commit a batch of batches to the database.
/// Returns (segment_offsets, batch_ids).
async fn commit_batch(
    batches: &[RecordBatch],
    segment_metas: &[crate::fl::SegmentMeta],
    key_to_index: &std::collections::HashMap<BatchKey, usize>,
    pending_writers: &[crate::buffer::PendingWriter],
    s3_key: &str,
    pool: &PgPool,
    ingest_time: chrono::DateTime<chrono::Utc>,
) -> Result<(Vec<(u64, u64)>, Vec<i64>), sqlx::Error> {
    struct SegmentCommitInput {
        key: BatchKey,
        seg_idx: usize,
        record_count: i64,
    }

    let mut tx = pool.begin().await?;

    let mut segment_offsets = vec![(0u64, 0u64); batches.len()];
    let mut batch_ids = vec![0i64; batches.len()];
    let mut segment_inputs: Vec<SegmentCommitInput> = key_to_index
        .iter()
        .map(|(key, &seg_idx)| SegmentCommitInput {
            key: *key,
            seg_idx,
            record_count: batches[seg_idx].records.len() as i64,
        })
        .collect();
    segment_inputs.sort_by_key(|s| s.seg_idx);

    // Group segments by topic_id for offset allocation
    let mut topic_to_segment_inputs: std::collections::HashMap<i32, Vec<usize>> =
        std::collections::HashMap::with_capacity(segment_inputs.len());
    let mut topic_deltas: std::collections::HashMap<i32, i64> =
        std::collections::HashMap::with_capacity(segment_inputs.len());

    for (input_idx, input) in segment_inputs.iter().enumerate() {
        let topic_key = input.key.topic_id.0 as i32;
        topic_to_segment_inputs
            .entry(topic_key)
            .or_default()
            .push(input_idx);
        *topic_deltas.entry(topic_key).or_insert(0) += input.record_count;
    }

    if !topic_deltas.is_empty() {
        let offsets_start = std::time::Instant::now();
        let topic_delta_rows: Vec<(i32, i64)> = topic_deltas
            .iter()
            .map(|(&topic_id, &delta)| (topic_id, delta))
            .collect();

        let mut offset_qb =
            QueryBuilder::<Postgres>::new("INSERT INTO topic_offsets (topic_id, next_offset) ");
        offset_qb.push_values(topic_delta_rows, |mut b, (topic_id, delta)| {
            b.push_bind(topic_id).push_bind(delta);
        });
        offset_qb.push(
            " ON CONFLICT (topic_id) \
             DO UPDATE SET next_offset = topic_offsets.next_offset + EXCLUDED.next_offset \
             RETURNING topic_id, next_offset",
        );

        let updated_topic_offsets: Vec<(i32, i64)> =
            offset_qb.build_query_as().fetch_all(&mut *tx).await?;
        let updated_offset_by_topic: std::collections::HashMap<i32, i64> =
            updated_topic_offsets.into_iter().collect();

        for (topic_key, input_indices) in &topic_to_segment_inputs {
            let total_delta = *topic_deltas.get(topic_key).ok_or_else(|| {
                sqlx::Error::Protocol(format!("missing topic delta for topic={}", topic_key))
            })?;
            let topic_end = *updated_offset_by_topic.get(topic_key).ok_or_else(|| {
                sqlx::Error::Protocol(format!("missing updated offset for topic={}", topic_key))
            })?;

            let mut cursor = topic_end - total_delta;
            for input_idx in input_indices {
                let input = &segment_inputs[*input_idx];
                let start_offset = cursor;
                let end_offset = start_offset + input.record_count;
                segment_offsets[input.seg_idx] = (start_offset as u64, end_offset as u64);
                cursor = end_offset;
            }
        }
        DB_COMMIT_OFFSETS_SECONDS.observe(offsets_start.elapsed().as_secs_f64());
    }

    if !segment_inputs.is_empty() {
        let topic_batches_start = std::time::Instant::now();
        let mut batch_qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO topic_batches (\
                 topic_id, schema_id, \
                 start_offset, end_offset, record_count, \
                 s3_key, byte_offset, byte_length, ingest_time, crc32\
             ) ",
        );
        batch_qb.push_values(&segment_inputs, |mut b, input| {
            let meta = &segment_metas[input.seg_idx];
            let (start_offset, end_offset) = segment_offsets[input.seg_idx];
            b.push_bind(input.key.topic_id.0 as i32)
                .push_bind(input.key.schema_id.0 as i32)
                .push_bind(start_offset as i64)
                .push_bind(end_offset as i64)
                .push_bind(input.record_count as i32)
                .push_bind(s3_key)
                .push_bind(meta.byte_offset as i64)
                .push_bind(meta.byte_length as i64)
                .push_bind(ingest_time)
                .push_bind(meta.crc32 as i64);
        });
        batch_qb.push(" RETURNING batch_id");
        let returned_ids: Vec<(i64,)> = batch_qb.build_query_as().fetch_all(&mut *tx).await?;
        // Map returned batch_ids back to segment indices
        for (i, input) in segment_inputs.iter().enumerate() {
            if let Some(row) = returned_ids.get(i) {
                batch_ids[input.seg_idx] = row.0;
            }
        }
        DB_COMMIT_TOPIC_BATCHES_SECONDS.observe(topic_batches_start.elapsed().as_secs_f64());
    }

    // Persist latest writer dedup state for this flush in a single DB statement.
    // This keeps dedup durable across cache evictions, restarts, and multi-broker retries.
    if !pending_writers.is_empty() {
        let mut latest_by_writer: std::collections::HashMap<
            WriterId,
            (AppendSeq, Vec<flux_common::types::BatchAck>),
        > = std::collections::HashMap::with_capacity(pending_writers.len());

        for pending in pending_writers {
            let append_acks =
                BrokerBuffer::calculate_acks_for_pending(pending, key_to_index, &segment_offsets);
            match latest_by_writer.get(&pending.writer_id) {
                Some((existing_seq, _)) if existing_seq.0 >= pending.append_seq.0 => {}
                _ => {
                    latest_by_writer.insert(pending.writer_id, (pending.append_seq, append_acks));
                }
            }
        }

        if !latest_by_writer.is_empty() {
            let prep_start = std::time::Instant::now();
            let rows = build_writer_state_rows(latest_by_writer)?;
            DB_COMMIT_WRITER_STATE_PREP_SECONDS.observe(prep_start.elapsed().as_secs_f64());

            let upsert_start = std::time::Instant::now();
            let mut qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO writer_state (writer_id, last_seq, last_acks) ",
            );
            qb.push_values(rows, |mut b, (writer_id, append_seq, acks_json)| {
                b.push_bind(writer_id.0)
                    .push_bind(append_seq.0 as i64)
                    .push_bind(acks_json);
            });
            qb.push(
                " ON CONFLICT (writer_id) DO UPDATE SET \
                 last_seq = EXCLUDED.last_seq, \
                 last_acks = EXCLUDED.last_acks, \
                 updated_at = NOW() \
                 WHERE writer_state.last_seq < EXCLUDED.last_seq",
            );
            qb.build().execute(&mut *tx).await?;
            DB_COMMIT_WRITER_STATE_UPSERT_SECONDS.observe(upsert_start.elapsed().as_secs_f64());
        }
    }

    let tx_commit_start = std::time::Instant::now();
    tx.commit().await?;
    DB_COMMIT_TX_COMMIT_SECONDS.observe(tx_commit_start.elapsed().as_secs_f64());

    Ok((segment_offsets, batch_ids))
}

fn build_writer_state_rows(
    latest_by_writer: std::collections::HashMap<
        WriterId,
        (AppendSeq, Vec<flux_common::types::BatchAck>),
    >,
) -> Result<Vec<(WriterId, AppendSeq, serde_json::Value)>, sqlx::Error> {
    let mut bump = WRITER_STATE_ROWS_BUMP
        .lock()
        .expect("writer state bump mutex poisoned");
    bump.reset();
    let mut rows = BumpVec::with_capacity_in(latest_by_writer.len(), &bump);
    for (writer_id, (append_seq, append_acks)) in latest_by_writer {
        let acks_json = serde_json::to_value(append_acks)
            .map_err(|e| sqlx::Error::Protocol(format!("batch ack serialization: {e}")))?;
        rows.push((writer_id, append_seq, acks_json));
    }
    Ok(rows.into_iter().collect())
}
