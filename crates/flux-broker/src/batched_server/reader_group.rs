// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Reader group protocol handlers: join, heartbeat, poll, leave, commit.

use tracing::{error, warn};

use flux_common::ids::{Offset, TopicId};
use flux_wire::{
    ERR_INTERNAL_ERROR, ERR_MAX_INFLIGHT, ERR_NOT_OWNER, STATUS_OK, ServerMessage, reader,
};

use crate::BrokerError;
use crate::object_store::ObjectStore;

use super::BrokerState;
use super::encoding::{RESPONSE_CAPACITY_SMALL, encode_server_message_vec};
use super::fetch::fetch_records;

/// Handle a JoinGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_count = req.topic_ids.len())
)]
pub(crate) async fn handle_join_group<S: ObjectStore + Send + Sync>(
    req: reader::JoinGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let topic_id = req.topic_ids.first().copied().unwrap_or(TopicId(0));

    let result = state
        .coordinator
        .join_group(&req.group_id, topic_id, &req.reader_id)
        .await;

    let response = match result {
        Ok(()) => reader::JoinGroupResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
        },
        Err(e) => {
            error!("JoinGroup error: {}", e);
            reader::JoinGroupResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "join group failed".to_string(),
            }
        }
    };

    encode_server_message_vec(ServerMessage::JoinGroup(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a HeartbeatRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0)
)]
pub(crate) async fn handle_heartbeat<S: ObjectStore + Send + Sync>(
    req: reader::HeartbeatRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .heartbeat(&req.group_id, req.topic_id, &req.reader_id)
        .await;

    let response = match result {
        Ok(status) => reader::HeartbeatResponseExt {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            status: match status {
                crate::coordinator::HeartbeatStatus::Ok => reader::HeartbeatStatus::Ok,
                crate::coordinator::HeartbeatStatus::UnknownMember => {
                    reader::HeartbeatStatus::UnknownMember
                }
            },
        },
        Err(e) => {
            error!("Heartbeat error: {}", e);
            reader::HeartbeatResponseExt {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "heartbeat failed".to_string(),
                status: reader::HeartbeatStatus::UnknownMember,
            }
        }
    };

    encode_server_message_vec(ServerMessage::Heartbeat(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a PollRequest: dispatch offset range and fetch records.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0)
)]
pub(crate) async fn handle_poll<S: ObjectStore + Send + Sync>(
    req: reader::PollRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = process_poll(&req, state).await;

    let response = match result {
        Ok((results, raw_segments, start_offset, end_offset, lease_deadline_ms)) => {
            reader::PollResponse {
                success: true,
                error_code: STATUS_OK,
                error_message: String::new(),
                results,
                start_offset,
                end_offset,
                lease_deadline_ms,
                raw_segments,
            }
        }
        Err(BrokerError::MaxInflight) => {
            warn!("Poll rejected: max inflight for reader {}", req.reader_id);
            reader::PollResponse {
                success: false,
                error_code: ERR_MAX_INFLIGHT,
                error_message: "max inflight ranges exceeded".to_string(),
                results: vec![],
                start_offset: Offset(0),
                end_offset: Offset(0),
                lease_deadline_ms: 0,
                raw_segments: vec![],
            }
        }
        Err(e) => {
            error!("Poll error: {}", e);
            reader::PollResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "poll failed".to_string(),
                results: vec![],
                start_offset: Offset(0),
                end_offset: Offset(0),
                lease_deadline_ms: 0,
                raw_segments: vec![],
            }
        }
    };

    let total_record_bytes: usize = response
        .results
        .iter()
        .flat_map(|r| &r.records)
        .map(|rec| rec.value.len() + rec.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32)
        .sum::<usize>()
        + response
            .raw_segments
            .iter()
            .map(|s| s.payload.len() + 64)
            .sum::<usize>();
    let buf_size = (total_record_bytes + 1024).max(64 * 1024);

    encode_server_message_vec(ServerMessage::Poll(response), buf_size + 16)
}

/// Dispatch an offset range via the coordinator and fetch the records.
type PollOutcome = (
    Vec<reader::TopicResult>,
    Vec<reader::RawSegment>,
    Offset,
    Offset,
    u64,
);

async fn process_poll<S: ObjectStore + Send + Sync>(
    req: &reader::PollRequest,
    state: &BrokerState<S>,
) -> Result<PollOutcome, BrokerError> {
    let poll_result = state
        .coordinator
        .poll(&req.group_id, req.topic_id, &req.reader_id, req.max_bytes)
        .await?;

    if poll_result.status == crate::coordinator::PollStatus::MaxInflight {
        return Err(BrokerError::MaxInflight);
    }

    let start_offset = poll_result.start_offset;
    let end_offset = poll_result.end_offset;
    let lease_deadline_ms = poll_result.lease_deadline_ms;

    if start_offset == end_offset {
        return Ok((vec![], vec![], start_offset, end_offset, lease_deadline_ms));
    }

    // Zero-copy: leases are segment-aligned by construction (the coordinator
    // accumulates whole index rows), so the raw segments in [start, end)
    // exactly cover the lease.
    if req.raw {
        let (segments, _high_watermark) = super::raw_read::fetch_raw_segments(
            req.topic_id,
            start_offset,
            Some(end_offset),
            req.max_bytes as usize,
            state,
        )
        .await?;
        return Ok((
            vec![],
            segments,
            start_offset,
            end_offset,
            lease_deadline_ms,
        ));
    }

    // Fetch the ENTIRE lease: the coordinator already budgeted it (in
    // compressed bytes), and the client commits [start, end) wholesale — a
    // second decoded-byte budget here would silently drop the tail of the
    // lease for compressible payloads (committed but never delivered).
    let (results, _high_watermark) = fetch_records(
        req.topic_id,
        start_offset,
        Some(end_offset),
        usize::MAX,
        state,
    )
    .await?;

    Ok((results, vec![], start_offset, end_offset, lease_deadline_ms))
}

/// Handle a LeaveGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0)
)]
pub(crate) async fn handle_leave_group<S: ObjectStore + Send + Sync>(
    req: reader::LeaveGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .leave_group(&req.group_id, req.topic_id, &req.reader_id)
        .await;

    let response = reader::LeaveGroupResponse {
        success: result.is_ok(),
        error_code: if result.is_ok() {
            STATUS_OK
        } else {
            ERR_INTERNAL_ERROR
        },
        error_message: if result.is_ok() {
            String::new()
        } else {
            "leave group failed".to_string()
        },
    };

    if let Err(e) = result {
        error!("LeaveGroup error: {}", e);
    }

    encode_server_message_vec(ServerMessage::LeaveGroup(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a CommitRequest (single range commit).
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(
        group_id = %req.group_id,
        reader_id = %req.reader_id,
        topic_id = req.topic_id.0,
        start_offset = req.start_offset.0,
        end_offset = req.end_offset.0,
    )
)]
pub(crate) async fn handle_commit<S: ObjectStore + Send + Sync>(
    req: reader::CommitRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .commit_range(
            &req.group_id,
            req.topic_id,
            &req.reader_id,
            req.start_offset,
            req.end_offset,
        )
        .await;

    let response = match result {
        Ok(crate::coordinator::CommitStatus::Ok) => reader::CommitResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
        },
        Ok(crate::coordinator::CommitStatus::NotOwner) => {
            warn!(
                "Commit rejected: reader {} doesn't own range [{}, {})",
                req.reader_id, req.start_offset.0, req.end_offset.0
            );
            reader::CommitResponse {
                success: false,
                error_code: ERR_NOT_OWNER,
                error_message: "commit rejected: not owner".to_string(),
            }
        }
        Err(e) => {
            error!("Commit error: {}", e);
            reader::CommitResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "commit failed".to_string(),
            }
        }
    };

    encode_server_message_vec(ServerMessage::Commit(response), RESPONSE_CAPACITY_SMALL)
}
