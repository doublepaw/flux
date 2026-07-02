// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Reader protocol messages and reader group messages using protobuf.

use flux_common::ids::{Offset, SchemaId, TopicId};
use flux_common::types::Record;

use crate::proto_conv::{
    decode_proto, encode_proto, encode_proto_checked, from_proto_record, to_proto_record,
};
use crate::{DecodeError, EncodeError, proto};

/// A direct read request (for tail/CLI, not group-based).
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub topic_id: TopicId,
    pub offset: Offset,
    pub max_bytes: u32,
}

/// A read response.
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub results: Vec<TopicResult>,
}

/// Result for a topic in a ReadResponse or PollResponse.
#[derive(Debug, Clone)]
pub struct TopicResult {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<Record>,
}

/// Reader group join request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub reader_id: String,
    pub topic_ids: Vec<TopicId>,
}

/// Reader group join response (no assignments - readers poll for work).
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
}

/// Heartbeat request to maintain reader membership.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
}

/// Heartbeat response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    Ok,
    UnknownMember,
}

/// Heartbeat response with status enum.
#[derive(Debug, Clone)]
pub struct HeartbeatResponseExt {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub status: HeartbeatStatus,
}

/// Poll request — reader asks broker for work.
#[derive(Debug, Clone)]
pub struct PollRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
    pub max_bytes: u32,
}

/// Poll response — broker hands out offset range and records.
#[derive(Debug, Clone)]
pub struct PollResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub results: Vec<TopicResult>,
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub lease_deadline_ms: u64,
}

/// Commit request — reader commits a processed offset range.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub group_id: String,
    pub reader_id: String,
    pub topic_id: TopicId,
    pub start_offset: Offset,
    pub end_offset: Offset,
}

/// Commit response.
#[derive(Debug, Clone)]
pub struct CommitResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
}

/// Leave group request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
}

/// Leave group response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
}

// ============ Encoding/Decoding ============

pub fn encode_read_request(req: &ReadRequest, buf: &mut [u8]) -> usize {
    let msg = proto::ReadRequest {
        topic_id: req.topic_id.0,
        offset: req.offset.0,
        max_bytes: req.max_bytes,
    };
    encode_proto(&msg, buf)
}

pub fn decode_read_request(buf: &[u8]) -> Result<(ReadRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::ReadRequest>(buf, "invalid protobuf read request")?;
    Ok((
        ReadRequest {
            topic_id: TopicId(msg.topic_id),
            offset: Offset(msg.offset),
            max_bytes: msg.max_bytes,
        },
        buf.len(),
    ))
}

pub fn encode_read_response(resp: &ReadResponse, buf: &mut [u8]) -> usize {
    encode_read_response_checked(resp, buf).expect("buffer too small for read response")
}

pub fn encode_read_response_checked(
    resp: &ReadResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::ReadResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        results: resp.results.iter().map(to_proto_topic_result).collect(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_read_response(buf: &[u8]) -> Result<(ReadResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::ReadResponse>(buf, "invalid protobuf read response")?;
    Ok((
        ReadResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            results: msg
                .results
                .into_iter()
                .map(from_proto_topic_result)
                .collect(),
        },
        buf.len(),
    ))
}

pub fn encode_join_request(req: &JoinGroupRequest, buf: &mut [u8]) -> usize {
    let msg = proto::JoinGroupRequest {
        group_id: req.group_id.clone(),
        reader_id: req.reader_id.clone(),
        topic_ids: req.topic_ids.iter().map(|id| id.0).collect(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_join_request(buf: &[u8]) -> Result<(JoinGroupRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::JoinGroupRequest>(buf, "invalid protobuf join request")?;
    Ok((
        JoinGroupRequest {
            group_id: msg.group_id,
            reader_id: msg.reader_id,
            topic_ids: msg.topic_ids.into_iter().map(TopicId).collect(),
        },
        buf.len(),
    ))
}

pub fn encode_join_response(resp: &JoinGroupResponse, buf: &mut [u8]) -> usize {
    encode_join_response_checked(resp, buf).expect("buffer too small for join response")
}

pub fn encode_join_response_checked(
    resp: &JoinGroupResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::JoinGroupResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_join_response(buf: &[u8]) -> Result<(JoinGroupResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::JoinGroupResponse>(buf, "invalid protobuf join response")?;
    Ok((
        JoinGroupResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
        },
        buf.len(),
    ))
}

pub fn encode_heartbeat_request(req: &HeartbeatRequest, buf: &mut [u8]) -> usize {
    let msg = proto::HeartbeatRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_heartbeat_request(buf: &[u8]) -> Result<(HeartbeatRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::HeartbeatRequest>(buf, "invalid protobuf heartbeat request")?;
    Ok((
        HeartbeatRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
        },
        buf.len(),
    ))
}

pub fn encode_heartbeat_response_ext(resp: &HeartbeatResponseExt, buf: &mut [u8]) -> usize {
    encode_heartbeat_response_ext_checked(resp, buf)
        .expect("buffer too small for heartbeat response")
}

pub fn encode_heartbeat_response_ext_checked(
    resp: &HeartbeatResponseExt,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::HeartbeatResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        status: hb_status_to_proto(resp.status) as i32,
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_heartbeat_response_ext(
    buf: &[u8],
) -> Result<(HeartbeatResponseExt, usize), DecodeError> {
    let msg = decode_proto::<proto::HeartbeatResponse>(buf, "invalid protobuf heartbeat response")?;
    Ok((
        HeartbeatResponseExt {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            status: proto_to_hb_status(msg.status),
        },
        buf.len(),
    ))
}

pub fn encode_poll_request(req: &PollRequest, buf: &mut [u8]) -> usize {
    let msg = proto::PollRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
        max_bytes: req.max_bytes,
    };
    encode_proto(&msg, buf)
}

pub fn decode_poll_request(buf: &[u8]) -> Result<(PollRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::PollRequest>(buf, "invalid protobuf poll request")?;
    Ok((
        PollRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
            max_bytes: msg.max_bytes,
        },
        buf.len(),
    ))
}

pub fn encode_poll_response(resp: &PollResponse, buf: &mut [u8]) -> usize {
    encode_poll_response_checked(resp, buf).expect("buffer too small for poll response")
}

pub fn encode_poll_response_checked(
    resp: &PollResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::PollResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        results: resp.results.iter().map(to_proto_topic_result).collect(),
        start_offset: resp.start_offset.0,
        end_offset: resp.end_offset.0,
        lease_deadline_ms: resp.lease_deadline_ms,
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_poll_response(buf: &[u8]) -> Result<(PollResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::PollResponse>(buf, "invalid protobuf poll response")?;
    Ok((
        PollResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            results: msg
                .results
                .into_iter()
                .map(from_proto_topic_result)
                .collect(),
            start_offset: Offset(msg.start_offset),
            end_offset: Offset(msg.end_offset),
            lease_deadline_ms: msg.lease_deadline_ms,
        },
        buf.len(),
    ))
}

pub fn encode_commit_request(req: &CommitRequest, buf: &mut [u8]) -> usize {
    let msg = proto::CommitRequest {
        group_id: req.group_id.clone(),
        reader_id: req.reader_id.clone(),
        topic_id: req.topic_id.0,
        start_offset: req.start_offset.0,
        end_offset: req.end_offset.0,
    };
    encode_proto(&msg, buf)
}

pub fn decode_commit_request(buf: &[u8]) -> Result<(CommitRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::CommitRequest>(buf, "invalid protobuf commit request")?;
    Ok((
        CommitRequest {
            group_id: msg.group_id,
            reader_id: msg.reader_id,
            topic_id: TopicId(msg.topic_id),
            start_offset: Offset(msg.start_offset),
            end_offset: Offset(msg.end_offset),
        },
        buf.len(),
    ))
}

pub fn encode_commit_response(resp: &CommitResponse, buf: &mut [u8]) -> usize {
    encode_commit_response_checked(resp, buf).expect("buffer too small for commit response")
}

pub fn encode_commit_response_checked(
    resp: &CommitResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::CommitResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_commit_response(buf: &[u8]) -> Result<(CommitResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::CommitResponse>(buf, "invalid protobuf commit response")?;
    Ok((
        CommitResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
        },
        buf.len(),
    ))
}

pub fn encode_leave_request(req: &LeaveGroupRequest, buf: &mut [u8]) -> usize {
    let msg = proto::LeaveGroupRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_leave_request(buf: &[u8]) -> Result<(LeaveGroupRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::LeaveGroupRequest>(buf, "invalid protobuf leave request")?;
    Ok((
        LeaveGroupRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
        },
        buf.len(),
    ))
}

pub fn encode_leave_response(resp: &LeaveGroupResponse, buf: &mut [u8]) -> usize {
    encode_leave_response_checked(resp, buf).expect("buffer too small for leave response")
}

pub fn encode_leave_response_checked(
    resp: &LeaveGroupResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::LeaveGroupResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_leave_response(buf: &[u8]) -> Result<(LeaveGroupResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::LeaveGroupResponse>(buf, "invalid protobuf leave response")?;
    Ok((
        LeaveGroupResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
        },
        buf.len(),
    ))
}

fn to_proto_topic_result(result: &TopicResult) -> proto::TopicResult {
    proto::TopicResult {
        topic_id: result.topic_id.0,
        schema_id: result.schema_id.0,
        high_watermark: result.high_watermark.0,
        records: result.records.iter().map(to_proto_record).collect(),
    }
}

fn from_proto_topic_result(result: proto::TopicResult) -> TopicResult {
    TopicResult {
        topic_id: TopicId(result.topic_id),
        schema_id: SchemaId(result.schema_id),
        high_watermark: Offset(result.high_watermark),
        records: result.records.into_iter().map(from_proto_record).collect(),
    }
}

fn hb_status_to_proto(status: HeartbeatStatus) -> proto::HeartbeatStatus {
    match status {
        HeartbeatStatus::Ok => proto::HeartbeatStatus::Ok,
        HeartbeatStatus::UnknownMember => proto::HeartbeatStatus::UnknownMember,
    }
}

fn proto_to_hb_status(status: i32) -> HeartbeatStatus {
    match proto::HeartbeatStatus::try_from(status).unwrap_or(proto::HeartbeatStatus::UnknownMember)
    {
        proto::HeartbeatStatus::Ok => HeartbeatStatus::Ok,
        proto::HeartbeatStatus::UnknownMember => HeartbeatStatus::UnknownMember,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_request_roundtrip() {
        let req = ReadRequest {
            topic_id: TopicId(1),
            offset: Offset(100),
            max_bytes: 1024,
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_read_request(&req, &mut buf);
        let (decoded, decoded_len) = decode_read_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.topic_id.0, 1);
        assert_eq!(decoded.offset.0, 100);
    }

    #[test]
    fn test_join_response_roundtrip() {
        let resp = JoinGroupResponse {
            success: true,
            error_code: 0,
            error_message: String::new(),
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_join_response(&resp, &mut buf);
        let (decoded, decoded_len) = decode_join_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert!(decoded.success);
    }

    #[test]
    fn test_poll_request_roundtrip() {
        let req = PollRequest {
            group_id: "my-group".to_string(),
            topic_id: TopicId(1),
            reader_id: "reader-1".to_string(),
            max_bytes: 1024,
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_poll_request(&req, &mut buf);
        let (decoded, decoded_len) = decode_poll_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, "my-group");
        assert_eq!(decoded.topic_id.0, 1);
        assert_eq!(decoded.max_bytes, 1024);
    }

    #[test]
    fn test_commit_request_roundtrip() {
        let req = CommitRequest {
            group_id: "my-group".to_string(),
            reader_id: "reader-1".to_string(),
            topic_id: TopicId(1),
            start_offset: Offset(100),
            end_offset: Offset(200),
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_commit_request(&req, &mut buf);
        let (decoded, decoded_len) = decode_commit_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.start_offset.0, 100);
        assert_eq!(decoded.end_offset.0, 200);
    }
}

// ---------------------------------------------------------------------------
// Raw (zero-copy) reads: compressed FL segments shipped as-is; the client
// decompresses and decodes records locally.
// ---------------------------------------------------------------------------

/// Zero-copy read request. `max_bytes` budgets *compressed* bytes.
#[derive(Debug, Clone)]
pub struct RawReadRequest {
    pub topic_id: TopicId,
    pub offset: Offset,
    pub max_bytes: u32,
}

/// One compressed FL segment plus the metadata needed to decode it.
#[derive(Debug, Clone)]
pub struct RawSegment {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub crc32: u32,
    pub payload: bytes::Bytes,
}

/// Zero-copy read response.
#[derive(Debug, Clone)]
pub struct RawReadResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub high_watermark: Offset,
    pub segments: Vec<RawSegment>,
}

pub fn encode_raw_read_request(req: &RawReadRequest, buf: &mut [u8]) -> usize {
    let msg = proto::RawReadRequest {
        topic_id: req.topic_id.0,
        offset: req.offset.0,
        max_bytes: req.max_bytes,
    };
    encode_proto(&msg, buf)
}

pub fn decode_raw_read_request(buf: &[u8]) -> Result<(RawReadRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::RawReadRequest>(buf, "invalid protobuf raw read request")?;
    Ok((
        RawReadRequest {
            topic_id: TopicId(msg.topic_id),
            offset: Offset(msg.offset),
            max_bytes: msg.max_bytes,
        },
        buf.len(),
    ))
}

pub fn encode_raw_read_response_checked(
    resp: &RawReadResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::RawReadResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        high_watermark: resp.high_watermark.0,
        segments: resp
            .segments
            .iter()
            .map(|s| proto::RawSegment {
                topic_id: s.topic_id.0,
                schema_id: s.schema_id.0,
                start_offset: s.start_offset.0,
                end_offset: s.end_offset.0,
                crc32: s.crc32,
                payload: s.payload.to_vec(),
            })
            .collect(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_raw_read_response(buf: &[u8]) -> Result<(RawReadResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::RawReadResponse>(buf, "invalid protobuf raw read response")?;
    Ok((
        RawReadResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            high_watermark: Offset(msg.high_watermark),
            segments: msg
                .segments
                .into_iter()
                .map(|s| RawSegment {
                    topic_id: TopicId(s.topic_id),
                    schema_id: SchemaId(s.schema_id),
                    start_offset: Offset(s.start_offset),
                    end_offset: Offset(s.end_offset),
                    crc32: s.crc32,
                    payload: bytes::Bytes::from(s.payload),
                })
                .collect(),
        },
        buf.len(),
    ))
}
