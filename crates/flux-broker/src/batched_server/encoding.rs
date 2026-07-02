// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Server message encoding helpers.

use std::time::Instant;

use flux_common::ids::AppendSeq;
use flux_common::types::BatchAck;
use flux_wire::{ClientMessage, ServerMessage, encode_server_message, reader, writer};

use crate::metrics::WS_ENCODE_SERVER_SECONDS;

pub(crate) const RESPONSE_CAPACITY_SMALL: usize = 512;
pub(crate) const RESPONSE_CAPACITY_PRODUCE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorResponseKind {
    Append,
    Read,
    JoinGroup,
    Heartbeat,
    Poll,
    LeaveGroup,
    Commit,
    Unknown,
}

pub(crate) fn server_message_kind(msg: &ServerMessage) -> &'static str {
    match msg {
        ServerMessage::Append(_) => "append",
        ServerMessage::Read(_) => "read",
        ServerMessage::JoinGroup(_) => "join_group",
        ServerMessage::Heartbeat(_) => "heartbeat",
        ServerMessage::Poll(_) => "poll",
        ServerMessage::LeaveGroup(_) => "leave_group",
        ServerMessage::Commit(_) => "commit",
        ServerMessage::Auth(_) => "auth",
        ServerMessage::RawRead(_) => "raw_read",
    }
}

pub(crate) fn client_message_kind(msg: &ClientMessage) -> &'static str {
    match msg {
        ClientMessage::Append(_) => "append",
        ClientMessage::Read(_) => "read",
        ClientMessage::JoinGroup(_) => "join_group",
        ClientMessage::Heartbeat(_) => "heartbeat",
        ClientMessage::Poll(_) => "poll",
        ClientMessage::LeaveGroup(_) => "leave_group",
        ClientMessage::Commit(_) => "commit",
        ClientMessage::Auth(_) => "auth",
        ClientMessage::RawRead(_) => "raw_read",
    }
}

pub(crate) fn error_kind_label(kind: ErrorResponseKind) -> &'static str {
    match kind {
        ErrorResponseKind::Append => "append",
        ErrorResponseKind::Read => "read",
        ErrorResponseKind::JoinGroup => "join_group",
        ErrorResponseKind::Heartbeat => "heartbeat",
        ErrorResponseKind::Poll => "poll",
        ErrorResponseKind::LeaveGroup => "leave_group",
        ErrorResponseKind::Commit => "commit",
        ErrorResponseKind::Unknown => "unknown",
    }
}

pub(crate) fn encode_server_message_vec(msg: ServerMessage, capacity: usize) -> Vec<u8> {
    let message_kind = server_message_kind(&msg);
    let encode_started = Instant::now();
    let mut buf_size = capacity.max(256);
    loop {
        let mut buf = vec![0u8; buf_size];
        match encode_server_message(&msg, &mut buf) {
            Ok(len) => {
                buf.truncate(len);
                WS_ENCODE_SERVER_SECONDS
                    .with_label_values(&[message_kind])
                    .observe(encode_started.elapsed().as_secs_f64());
                return buf;
            }
            Err(flux_wire::EncodeError::BufferTooSmall { needed, .. }) => {
                buf_size = needed.max(buf_size.saturating_mul(2));
            }
            Err(e) => panic!("server message encode failed: {e}"),
        }
    }
}

pub(crate) fn encode_append_response(
    append_seq: AppendSeq,
    success: bool,
    error_code: u16,
    error_message: impl Into<String>,
    append_acks: Vec<BatchAck>,
) -> Vec<u8> {
    let resp = writer::AppendResponse {
        append_seq,
        success,
        error_code,
        error_message: error_message.into(),
        append_acks,
    };
    encode_server_message_vec(ServerMessage::Append(resp), RESPONSE_CAPACITY_PRODUCE)
}

pub(crate) fn encode_error_response(
    kind: ErrorResponseKind,
    error_code: u16,
    error_message: &str,
) -> Vec<u8> {
    let small_capacity = 96 + error_message.len();
    match kind {
        ErrorResponseKind::Append | ErrorResponseKind::Unknown => {
            encode_append_response(AppendSeq(0), false, error_code, error_message, vec![])
        }
        ErrorResponseKind::Read => {
            let error_resp = reader::ReadResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                results: vec![],
            };
            encode_server_message_vec(ServerMessage::Read(error_resp), small_capacity)
        }
        ErrorResponseKind::JoinGroup => {
            let error_resp = reader::JoinGroupResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
            };
            encode_server_message_vec(ServerMessage::JoinGroup(error_resp), small_capacity)
        }
        ErrorResponseKind::Heartbeat => {
            let error_resp = reader::HeartbeatResponseExt {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                status: reader::HeartbeatStatus::UnknownMember,
            };
            encode_server_message_vec(ServerMessage::Heartbeat(error_resp), small_capacity)
        }
        ErrorResponseKind::Poll => {
            let error_resp = reader::PollResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                results: vec![],
                start_offset: flux_common::ids::Offset(0),
                end_offset: flux_common::ids::Offset(0),
                lease_deadline_ms: 0,
            };
            encode_server_message_vec(ServerMessage::Poll(error_resp), small_capacity)
        }
        ErrorResponseKind::LeaveGroup => {
            let error_resp = reader::LeaveGroupResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
            };
            encode_server_message_vec(ServerMessage::LeaveGroup(error_resp), small_capacity)
        }
        ErrorResponseKind::Commit => {
            let error_resp = reader::CommitResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
            };
            encode_server_message_vec(ServerMessage::Commit(error_resp), small_capacity)
        }
    }
}
