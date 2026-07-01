// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Append request handling: dedup, enqueue, and ack awaiting.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tracing::{debug, error};

use flux_common::ids::{AppendSeq, WriterId};
use flux_common::types::BatchAck;
use flux_wire::{ERR_BACKPRESSURE, ERR_INTERNAL_ERROR, ERR_STALE_SEQUENCE, STATUS_OK, writer};

use crate::dedup::DedupResult;
use crate::metrics::{
    APPEND_BYTES_TOTAL, APPEND_ENQUEUE_TO_ACK_SECONDS, DEDUP_CHECK_LATENCY_SECONDS,
    DEDUP_RESULT_TOTAL, FLUSH_QUEUE_DEPTH, INFLIGHT_APPEND_REQUESTS,
};
use crate::object_store::ObjectStore;

use super::encoding::encode_append_response;
use super::{BrokerState, FlushCommand};

#[derive(Clone)]
pub(crate) enum InFlightOutcome {
    Success(Vec<BatchAck>),
    Error { code: u16, message: String },
}

pub(super) struct InFlightAppendEntry {
    waiters: Vec<tokio::sync::oneshot::Sender<InFlightOutcome>>,
}

#[derive(Default)]
pub(crate) struct WriterInFlightState {
    pub(super) entries: HashMap<u64, InFlightAppendEntry>,
    pub(super) seqs: BTreeSet<u64>,
}

pub(crate) enum InFlightAppendDecision {
    Proceed,
    Wait(tokio::sync::oneshot::Receiver<InFlightOutcome>),
}

/// Outcome of `enqueue_append`: the ordering-critical work (dedup + send to
/// flush channel) that runs synchronously on the connection handler.
pub(crate) enum EnqueueResult {
    /// Resolved immediately (backpressure, duplicate, stale, or enqueue error).
    Resolved(Vec<u8>),
    /// Duplicate request waiting on an in-flight original with the same seq.
    WaitInFlight {
        append_seq: AppendSeq,
        rx: tokio::sync::oneshot::Receiver<InFlightOutcome>,
    },
    /// Successfully enqueued; caller must await the flush ack.
    Pending {
        writer_id: WriterId,
        append_seq: AppendSeq,
        response_rx: tokio::sync::oneshot::Receiver<Vec<BatchAck>>,
        enqueue_started: Instant,
    },
}

pub(crate) fn total_inflight_requests(in_flight: &HashMap<WriterId, WriterInFlightState>) -> usize {
    in_flight.values().map(|state| state.entries.len()).sum()
}

pub(crate) async fn in_flight_append_decision<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
) -> InFlightAppendDecision {
    let mut in_flight = state.in_flight_append.lock().await;
    let writer_in_flight = in_flight.entry(writer_id).or_default();
    if let Some(entry) = writer_in_flight.entries.get_mut(&append_seq.0) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        entry.waiters.push(tx);
        return InFlightAppendDecision::Wait(rx);
    }

    writer_in_flight.entries.insert(
        append_seq.0,
        InFlightAppendEntry {
            waiters: Vec::new(),
        },
    );
    writer_in_flight.seqs.insert(append_seq.0);
    INFLIGHT_APPEND_REQUESTS.set(total_inflight_requests(&in_flight) as f64);
    InFlightAppendDecision::Proceed
}

pub(crate) async fn complete_in_flight_append<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
    outcome: InFlightOutcome,
) {
    let waiters = {
        let mut in_flight = state.in_flight_append.lock().await;
        let mut remove_writer = false;
        let waiters = if let Some(writer_in_flight) = in_flight.get_mut(&writer_id) {
            let waiters = writer_in_flight
                .entries
                .remove(&append_seq.0)
                .map(|entry| entry.waiters)
                .unwrap_or_default();
            writer_in_flight.seqs.remove(&append_seq.0);
            if writer_in_flight.entries.is_empty() {
                remove_writer = true;
            }
            waiters
        } else {
            Vec::new()
        };
        if remove_writer {
            in_flight.remove(&writer_id);
        }
        INFLIGHT_APPEND_REQUESTS.set(total_inflight_requests(&in_flight) as f64);
        waiters
    };

    for waiter in waiters {
        let _ = waiter.send(outcome.clone());
    }
}

pub(crate) async fn has_higher_in_flight_sequence<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
) -> bool {
    let in_flight = state.in_flight_append.lock().await;
    in_flight
        .get(&writer_id)
        .map(|writer_in_flight| {
            writer_in_flight
                .seqs
                .range((Bound::Excluded(append_seq.0), Bound::Unbounded))
                .next()
                .is_some()
        })
        .unwrap_or(false)
}

/// Dedup-check and enqueue an append request into the flush channel.
///
/// Runs synchronously on the connection handler so that pipelined appends
/// from the same writer reach the flush channel in TCP arrival order.
/// The caller spawns `await_append_ack` for non-Resolved outcomes.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(
        writer_id = ?req.writer_id,
        append_seq = req.append_seq.0,
        segment_count = req.batches.len()
    )
)]
pub(crate) async fn enqueue_append<S: ObjectStore + Send + Sync + 'static>(
    req: writer::AppendRequest,
    state: &BrokerState<S>,
) -> EnqueueResult {
    // Calculate bytes for metrics
    let total_bytes: usize = req
        .batches
        .iter()
        .flat_map(|s| &s.records)
        .map(|r| r.value.len() + r.key.as_ref().map(|k| k.len()).unwrap_or(0))
        .sum();
    APPEND_BYTES_TOTAL.inc_by(total_bytes as f64);

    match in_flight_append_decision(state, req.writer_id, req.append_seq).await {
        InFlightAppendDecision::Proceed => {}
        InFlightAppendDecision::Wait(rx) => {
            return EnqueueResult::WaitInFlight {
                append_seq: req.append_seq,
                rx,
            };
        }
    }

    // Check backpressure
    if state.is_backpressure_active().await {
        debug!(
            "Backpressure active, rejecting request from {:?}",
            req.writer_id
        );
        let result = encode_append_response(
            req.append_seq,
            false,
            ERR_BACKPRESSURE,
            "backpressure active",
            vec![],
        );
        complete_in_flight_append(
            state,
            req.writer_id,
            req.append_seq,
            InFlightOutcome::Error {
                code: ERR_BACKPRESSURE,
                message: "backpressure active".to_string(),
            },
        )
        .await;
        return EnqueueResult::Resolved(result);
    }

    // Check for duplicates
    let dedup_started = Instant::now();
    let dedup_result = state.dedup_cache.check(req.writer_id, req.append_seq).await;
    DEDUP_CHECK_LATENCY_SECONDS.observe(dedup_started.elapsed().as_secs_f64());

    match dedup_result {
        Ok(DedupResult::Duplicate(append_acks)) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["duplicate"]).inc();
            debug!(
                "Duplicate request from {:?} append_seq={}",
                req.writer_id, req.append_seq.0
            );
            let result =
                encode_append_response(req.append_seq, true, STATUS_OK, "", append_acks.clone());
            complete_in_flight_append(
                state,
                req.writer_id,
                req.append_seq,
                InFlightOutcome::Success(append_acks),
            )
            .await;
            return EnqueueResult::Resolved(result);
        }
        Ok(DedupResult::Stale) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["stale"]).inc();
            if has_higher_in_flight_sequence(state, req.writer_id, req.append_seq).await {
                debug!(
                    "Allowing out-of-order in-flight request from {:?} append_seq={}",
                    req.writer_id, req.append_seq.0
                );
            } else {
                debug!(
                    "Stale request from {:?} append_seq={}",
                    req.writer_id, req.append_seq.0
                );
                let result = encode_append_response(
                    req.append_seq,
                    false,
                    ERR_STALE_SEQUENCE,
                    "stale writer sequence",
                    vec![],
                );
                complete_in_flight_append(
                    state,
                    req.writer_id,
                    req.append_seq,
                    InFlightOutcome::Error {
                        code: ERR_STALE_SEQUENCE,
                        message: "stale writer sequence".to_string(),
                    },
                )
                .await;
                return EnqueueResult::Resolved(result);
            }
        }
        Ok(DedupResult::Accept) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["accept"]).inc();
        }
        Err(e) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["error"]).inc();
            error!("Dedup check error: {}", e);
            // Continue processing on error (fail open)
        }
    }

    // Send to flush task — this is the ordering-critical send.
    // Because enqueue_append runs synchronously per connection,
    // sequential appends from the same writer enter this channel
    // in TCP arrival order.
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let enqueue_started = Instant::now();
    let pending = state
        .pending_flush_commands
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    FLUSH_QUEUE_DEPTH.set(pending as f64);

    if state
        .flush_tx
        .send(FlushCommand::Insert {
            writer_id: req.writer_id,
            append_seq: req.append_seq,
            batches: req.batches,
            enqueued_at: Instant::now(),
            response_tx,
        })
        .await
        .is_err()
    {
        let previous = state.pending_flush_commands.fetch_sub(1, Ordering::Relaxed);
        FLUSH_QUEUE_DEPTH.set(previous.saturating_sub(1) as f64);
        error!("Failed to send to flush task");
        let result = encode_append_response(
            req.append_seq,
            false,
            ERR_INTERNAL_ERROR,
            "failed to enqueue append request",
            vec![],
        );
        complete_in_flight_append(
            state,
            req.writer_id,
            req.append_seq,
            InFlightOutcome::Error {
                code: ERR_INTERNAL_ERROR,
                message: "failed to enqueue append request".to_string(),
            },
        )
        .await;
        return EnqueueResult::Resolved(result);
    }

    EnqueueResult::Pending {
        writer_id: req.writer_id,
        append_seq: req.append_seq,
        response_rx,
        enqueue_started,
    }
}

/// Wait for the flush loop to ack an enqueued append, then update the dedup
/// cache and notify any duplicate waiters.
///
/// Runs in a spawned task so it doesn't block the connection handler.
pub(crate) async fn await_append_ack<S: ObjectStore + Send + Sync + 'static>(
    result: EnqueueResult,
    state: &BrokerState<S>,
) -> Vec<u8> {
    match result {
        EnqueueResult::Resolved(response) => response,
        EnqueueResult::WaitInFlight { append_seq, rx } => match rx.await {
            Ok(InFlightOutcome::Success(append_acks)) => {
                encode_append_response(append_seq, true, STATUS_OK, "", append_acks)
            }
            Ok(InFlightOutcome::Error { code, message }) => {
                encode_append_response(append_seq, false, code, message, vec![])
            }
            Err(_) => encode_append_response(
                append_seq,
                false,
                ERR_INTERNAL_ERROR,
                "in-flight request canceled",
                vec![],
            ),
        },
        EnqueueResult::Pending {
            writer_id,
            append_seq,
            response_rx,
            enqueue_started,
        } => match response_rx.await {
            Ok(append_acks) => {
                APPEND_ENQUEUE_TO_ACK_SECONDS.observe(enqueue_started.elapsed().as_secs_f64());
                state
                    .dedup_cache
                    .update(writer_id, append_seq, append_acks.clone())
                    .await;
                let result =
                    encode_append_response(append_seq, true, STATUS_OK, "", append_acks.clone());
                complete_in_flight_append(
                    state,
                    writer_id,
                    append_seq,
                    InFlightOutcome::Success(append_acks),
                )
                .await;
                result
            }
            Err(_) => {
                APPEND_ENQUEUE_TO_ACK_SECONDS.observe(enqueue_started.elapsed().as_secs_f64());
                error!("Response channel closed");
                let result = encode_append_response(
                    append_seq,
                    false,
                    ERR_INTERNAL_ERROR,
                    "flush response channel closed",
                    vec![],
                );
                complete_in_flight_append(
                    state,
                    writer_id,
                    append_seq,
                    InFlightOutcome::Error {
                        code: ERR_INTERNAL_ERROR,
                        message: "flush response channel closed".to_string(),
                    },
                )
                .await;
                result
            }
        },
    }
}
