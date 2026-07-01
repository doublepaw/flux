// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Writer protocol messages: AppendRequest and AppendResponse.

use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{BatchAck, RecordBatch};
use prost::Message;
use uuid::Uuid;

use crate::proto_conv::{encode_proto_checked, from_proto_record, to_proto_record};
use crate::{DecodeError, EncodeError, proto};

/// A append request from a writer to an broker.
#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub writer_id: WriterId,
    pub append_seq: AppendSeq,
    pub batches: Vec<RecordBatch>,
}

/// A append response from an broker to a writer.
#[derive(Debug, Clone)]
pub struct AppendResponse {
    pub append_seq: AppendSeq,
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub append_acks: Vec<BatchAck>,
}

/// Encode a AppendRequest into the buffer, returning bytes written.
pub fn encode_request(req: &AppendRequest, buf: &mut [u8]) -> usize {
    encode_request_checked(req, buf).expect("buffer too small for append request")
}

/// Encode a AppendRequest into the buffer, returning bytes written.
pub fn encode_request_checked(req: &AppendRequest, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let msg = proto::AppendRequest {
        writer_id: req.writer_id.0.as_bytes().to_vec(),
        append_seq: req.append_seq.0,
        batches: req.batches.iter().map(to_proto_segment).collect(),
    };
    encode_proto_checked(&msg, buf)
}

/// Decode a AppendRequest from the buffer.
pub fn decode_request(buf: &[u8]) -> Result<(AppendRequest, usize), DecodeError> {
    let msg = proto::AppendRequest::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf append request",
    })?;

    if msg.writer_id.len() != 16 {
        return Err(DecodeError::InvalidData {
            msg: "writer_id must be 16 bytes",
        });
    }
    let uuid = Uuid::from_bytes(msg.writer_id.as_slice().try_into().map_err(|_| {
        DecodeError::InvalidData {
            msg: "invalid writer_id bytes",
        }
    })?);

    Ok((
        AppendRequest {
            writer_id: WriterId(uuid),
            append_seq: AppendSeq(msg.append_seq),
            batches: msg
                .batches
                .into_iter()
                .map(from_proto_segment)
                .collect::<Result<Vec<_>, _>>()?,
        },
        buf.len(),
    ))
}

/// Encode a AppendResponse into the buffer, returning bytes written.
pub fn encode_response(resp: &AppendResponse, buf: &mut [u8]) -> usize {
    encode_response_checked(resp, buf).expect("buffer too small for append response")
}

/// Encode a AppendResponse into the buffer, returning bytes written.
pub fn encode_response_checked(
    resp: &AppendResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::AppendResponse {
        append_seq: resp.append_seq.0,
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        append_acks: resp.append_acks.iter().map(to_proto_ack).collect(),
    };
    encode_proto_checked(&msg, buf)
}

/// Decode a AppendResponse from the buffer.
pub fn decode_response(buf: &[u8]) -> Result<(AppendResponse, usize), DecodeError> {
    let msg = proto::AppendResponse::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf append response",
    })?;

    Ok((
        AppendResponse {
            append_seq: AppendSeq(msg.append_seq),
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            append_acks: msg.append_acks.into_iter().map(from_proto_ack).collect(),
        },
        buf.len(),
    ))
}

fn to_proto_segment(batch: &RecordBatch) -> proto::RecordBatch {
    proto::RecordBatch {
        topic_id: batch.topic_id.0,
        schema_id: batch.schema_id.0,
        records: batch.records.iter().map(to_proto_record).collect(),
    }
}

fn from_proto_segment(msg: proto::RecordBatch) -> Result<RecordBatch, DecodeError> {
    Ok(RecordBatch {
        topic_id: TopicId(msg.topic_id),
        schema_id: SchemaId(msg.schema_id),
        records: msg.records.into_iter().map(from_proto_record).collect(),
    })
}

fn to_proto_ack(ack: &BatchAck) -> proto::BatchAck {
    proto::BatchAck {
        topic_id: ack.topic_id.0,
        schema_id: ack.schema_id.0,
        start_offset: ack.start_offset.0,
        end_offset: ack.end_offset.0,
    }
}

fn from_proto_ack(msg: proto::BatchAck) -> BatchAck {
    BatchAck {
        topic_id: TopicId(msg.topic_id),
        schema_id: SchemaId(msg.schema_id),
        start_offset: Offset(msg.start_offset),
        end_offset: Offset(msg.end_offset),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use flux_common::types::Record;

    fn sample_segment() -> RecordBatch {
        RecordBatch {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from_static(b"key1")),
                    value: Bytes::from_static(b"value1"),
                },
                Record {
                    key: None,
                    value: Bytes::from_static(b"value2"),
                },
            ],
        }
    }

    #[test]
    fn test_produce_request_roundtrip() {
        let req = AppendRequest {
            writer_id: WriterId(Uuid::new_v4()),
            append_seq: AppendSeq(42),
            batches: vec![sample_segment()],
        };

        let mut buf = [0u8; 1024];
        let encoded_len = encode_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.writer_id.0, req.writer_id.0);
        assert_eq!(decoded.append_seq.0, req.append_seq.0);
        assert_eq!(decoded.batches.len(), 1);
        assert_eq!(decoded.batches[0].records.len(), 2);
    }

    #[test]
    fn test_produce_response_roundtrip() {
        let resp = AppendResponse {
            append_seq: AppendSeq(42),
            success: false,
            error_code: 1234,
            error_message: "boom".to_string(),
            append_acks: vec![BatchAck {
                topic_id: TopicId(1),
                schema_id: SchemaId(100),
                start_offset: Offset(1000),
                end_offset: Offset(1002),
            }],
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_response(&resp, &mut buf);

        let (decoded, decoded_len) = decode_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.append_seq.0, 42);
        assert!(!decoded.success);
        assert_eq!(decoded.error_code, 1234);
        assert_eq!(decoded.error_message, "boom");
        assert_eq!(decoded.append_acks.len(), 1);
        assert_eq!(decoded.append_acks[0].start_offset.0, 1000);
        assert_eq!(decoded.append_acks[0].end_offset.0, 1002);
    }
}
