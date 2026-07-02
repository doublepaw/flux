// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Read request handling.

use std::sync::Arc;

use tracing::error;

use flux_common::ids::Offset;
use flux_wire::{ERR_INTERNAL_ERROR, STATUS_OK, ServerMessage, reader};

use crate::BrokerError;
use crate::metrics::{LatencyTimer, READ_LATENCY_SECONDS, READ_REQUESTS_TOTAL};
use crate::object_store::ObjectStore;

use super::BrokerState;
use super::encoding::encode_server_message_vec;
use super::fetch::fetch_records;

/// Handle a ReadRequest (direct read from S3, no consumer group).
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(topic_id = req.topic_id.0, offset = req.offset.0, max_bytes = req.max_bytes)
)]
pub(crate) async fn handle_read_request<S: ObjectStore + Send + Sync + 'static>(
    req: reader::ReadRequest,
    state: &Arc<BrokerState<S>>,
) -> Vec<u8> {
    let _timer = LatencyTimer::new(&READ_LATENCY_SECONDS);
    READ_REQUESTS_TOTAL.inc();

    // Serve from the read-ahead cache when the previous response's prefetch
    // covered this exact (topic, offset); otherwise read from storage.
    let cache_key = (req.topic_id.0, req.offset.0);
    let result = match state.readahead.take(cache_key) {
        Some(prefetched) => Ok(prefetched.results),
        None => process_read(&req, state).await,
    };

    // Prefetch the next contiguous window while the consumer processes this
    // response.
    if let Ok(results) = &result {
        let served: u64 = results.iter().map(|r| r.records.len() as u64).sum();
        let high_watermark = results
            .iter()
            .map(|r| r.high_watermark.0)
            .max()
            .unwrap_or(0);
        let next_offset = req.offset.0 + served;
        let next_key = (req.topic_id.0, next_offset);
        if served > 0 && next_offset < high_watermark && state.readahead.try_begin(next_key) {
            let state = state.clone();
            let topic_id = req.topic_id;
            let max_bytes = req.max_bytes as usize;
            tokio::spawn(async move {
                let prefetched =
                    fetch_records(topic_id, Offset(next_offset), None, max_bytes, &state)
                        .await
                        .ok()
                        .map(|(results, high_watermark)| {
                            let bytes = results
                                .iter()
                                .flat_map(|r| &r.records)
                                .map(|rec| {
                                    rec.value.len() + rec.key.as_ref().map(|k| k.len()).unwrap_or(0)
                                })
                                .sum();
                            super::read_ahead::Prefetched {
                                results,
                                high_watermark,
                                bytes,
                            }
                        });
                state.readahead.complete(next_key, prefetched);
            });
        }
    }

    let response = match result {
        Ok(results) => reader::ReadResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            results,
        },
        Err(e) => {
            error!("Read error: {}", e);
            reader::ReadResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "read failed".to_string(),
                results: vec![],
            }
        }
    };

    let total_record_bytes: usize = response
        .results
        .iter()
        .flat_map(|r| &r.records)
        .map(|rec| rec.value.len() + rec.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32)
        .sum();
    let buf_size = (total_record_bytes + 1024).max(64 * 1024);

    encode_server_message_vec(ServerMessage::Read(response), buf_size + 16)
}

/// Process a read request for a single topic.
async fn process_read<S: ObjectStore + Send + Sync>(
    req: &reader::ReadRequest,
    state: &Arc<BrokerState<S>>,
) -> Result<Vec<reader::TopicResult>, BrokerError> {
    let (results, _high_watermark) = fetch_records(
        req.topic_id,
        Offset(req.offset.0),
        None,
        req.max_bytes as usize,
        state,
    )
    .await?;
    Ok(results)
}
