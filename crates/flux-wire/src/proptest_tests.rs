// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Property-based tests for wire protocol encoding.

use bytes::Bytes;
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{BatchAck, Record, RecordBatch};
use proptest::prelude::*;

use crate::{reader, record, varint, writer};

// ============ Varint Property Tests ============

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10000))]

    #[test]
    fn prop_varint_i64_roundtrip(value: i64) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_i64(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_i64(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_u64_roundtrip(value: u64) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_u64(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_u64(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_i32_roundtrip(value: i32) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_i32(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_i32(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_encoding_is_compact(value in 0i64..128) {
        // Values 0-127 should encode in 1 byte (zigzag doubles, so 0-63 -> 1 byte)
        let mut buf = [0u8; 10];
        let len = varint::encode_i64(value, &mut buf);
        // Zigzag: 0->0, 1->2, ..., 63->126 (1 byte), 64->128 (2 bytes)
        if value < 64 {
            prop_assert_eq!(len, 1);
        } else {
            prop_assert_eq!(len, 2);
        }
    }
}

// ============ Record Property Tests ============

fn arb_record() -> impl Strategy<Value = Record> {
    (
        prop::option::of(prop::collection::vec(any::<u8>(), 0..100)),
        prop::collection::vec(any::<u8>(), 0..1000),
    )
        .prop_map(|(key, value)| Record {
            key: key.map(Bytes::from),
            value: Bytes::from(value),
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_record_roundtrip(rec in arb_record()) {
        let mut buf = vec![0u8; 2048];
        let encoded_len = record::encode(&rec, &mut buf);
        let (decoded, decoded_len) = record::decode(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.key, rec.key);
        prop_assert_eq!(decoded.value, rec.value);
    }

    #[test]
    fn prop_record_encoded_size_accurate(rec in arb_record()) {
        let mut buf = vec![0u8; 2048];
        let actual_len = record::encode(&rec, &mut buf);
        let predicted_len = record::encoded_size(&rec);
        prop_assert_eq!(actual_len, predicted_len);
    }
}

// ============ RecordBatch Property Tests ============

fn arb_segment() -> impl Strategy<Value = RecordBatch> {
    (
        any::<u32>(),
        any::<u32>(),
        prop::collection::vec(arb_record(), 0..20),
    )
        .prop_map(|(topic_id, schema_id, records)| RecordBatch {
            topic_id: TopicId(topic_id),
            schema_id: SchemaId(schema_id),
            records,
        })
}

fn arb_segment_ack() -> impl Strategy<Value = BatchAck> {
    (any::<u32>(), any::<u32>(), any::<u64>(), any::<u64>()).prop_map(
        |(topic_id, schema_id, start_offset, end_offset)| BatchAck {
            topic_id: TopicId(topic_id),
            schema_id: SchemaId(schema_id),
            start_offset: Offset(start_offset),
            end_offset: Offset(end_offset),
        },
    )
}

// ============ AppendRequest/Response Property Tests ============

fn arb_produce_request() -> impl Strategy<Value = writer::AppendRequest> {
    (
        prop::collection::vec(any::<u8>(), 16..=16), // UUID bytes
        any::<u64>(),
        prop::collection::vec(arb_segment(), 0..5),
    )
        .prop_map(|(uuid_bytes, append_seq, batches)| {
            let uuid = uuid::Uuid::from_bytes(uuid_bytes.try_into().unwrap());
            writer::AppendRequest {
                writer_id: WriterId(uuid),
                append_seq: AppendSeq(append_seq),
                batches,
            }
        })
}

fn arb_produce_response() -> impl Strategy<Value = writer::AppendResponse> {
    (
        any::<u64>(),
        prop::collection::vec(arb_segment_ack(), 0..10),
    )
        .prop_map(|(append_seq, append_acks)| writer::AppendResponse {
            append_seq: AppendSeq(append_seq),
            success: true,
            error_code: 0,
            error_message: String::new(),
            append_acks,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_produce_request_roundtrip(req in arb_produce_request()) {
        let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer
        let encoded_len = writer::encode_request(&req, &mut buf);
        let (decoded, decoded_len) = writer::decode_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.writer_id.0, req.writer_id.0);
        prop_assert_eq!(decoded.append_seq.0, req.append_seq.0);
        prop_assert_eq!(decoded.batches.len(), req.batches.len());
        for (d, o) in decoded.batches.iter().zip(req.batches.iter()) {
            prop_assert_eq!(d.topic_id.0, o.topic_id.0);
            prop_assert_eq!(d.schema_id.0, o.schema_id.0);
            prop_assert_eq!(d.records.len(), o.records.len());
            for (dr, or) in d.records.iter().zip(o.records.iter()) {
                prop_assert_eq!(&dr.value, &or.value);
            }
        }
    }

    #[test]
    fn prop_produce_response_roundtrip(resp in arb_produce_response()) {
        let mut buf = vec![0u8; 8 * 1024];
        let encoded_len = writer::encode_response(&resp, &mut buf);
        let (decoded, decoded_len) = writer::decode_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.append_seq.0, resp.append_seq.0);
        prop_assert_eq!(decoded.success, resp.success);
        prop_assert_eq!(decoded.error_code, resp.error_code);
        prop_assert_eq!(decoded.append_acks.len(), resp.append_acks.len());
        for (d, o) in decoded.append_acks.iter().zip(resp.append_acks.iter()) {
            prop_assert_eq!(d.topic_id.0, o.topic_id.0);
            prop_assert_eq!(d.schema_id.0, o.schema_id.0);
            prop_assert_eq!(d.start_offset.0, o.start_offset.0);
            prop_assert_eq!(d.end_offset.0, o.end_offset.0);
        }
    }
}

// ============ Reader Message Property Tests ============

fn arb_read_request() -> impl Strategy<Value = reader::ReadRequest> {
    (any::<u32>(), any::<u64>(), any::<u32>()).prop_map(|(topic_id, offset, max_bytes)| {
        reader::ReadRequest {
            topic_id: TopicId(topic_id),
            offset: Offset(offset),
            max_bytes,
        }
    })
}

fn arb_topic_result() -> impl Strategy<Value = reader::TopicResult> {
    (
        any::<u32>(),
        any::<u32>(),
        any::<u64>(),
        prop::collection::vec(arb_record(), 0..10),
    )
        .prop_map(
            |(topic_id, schema_id, high_watermark, records)| reader::TopicResult {
                topic_id: TopicId(topic_id),
                schema_id: SchemaId(schema_id),
                high_watermark: Offset(high_watermark),
                records,
            },
        )
}

fn arb_read_response() -> impl Strategy<Value = reader::ReadResponse> {
    prop::collection::vec(arb_topic_result(), 0..5).prop_map(|results| reader::ReadResponse {
        success: true,
        error_code: 0,
        error_message: String::new(),
        results,
    })
}

fn arb_poll_request() -> impl Strategy<Value = reader::PollRequest> {
    ("[a-z]{1,20}", any::<u32>(), "[a-z0-9]{1,30}", any::<u32>()).prop_map(
        |(group_id, topic_id, reader_id, max_bytes)| reader::PollRequest {
            group_id,
            topic_id: TopicId(topic_id),
            reader_id,
            max_bytes,
        },
    )
}

fn arb_poll_response() -> impl Strategy<Value = reader::PollResponse> {
    (
        prop::collection::vec(arb_topic_result(), 0..5),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(results, start_offset, end_offset, lease_deadline_ms)| {
            reader::PollResponse {
                success: true,
                error_code: 0,
                error_message: String::new(),
                results,
                start_offset: Offset(start_offset),
                end_offset: Offset(end_offset),
                lease_deadline_ms,
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_read_request_roundtrip(req in arb_read_request()) {
        let mut buf = vec![0u8; 1024];
        let encoded_len = reader::encode_read_request(&req, &mut buf);
        let (decoded, decoded_len) = reader::decode_read_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.topic_id.0, req.topic_id.0);
        prop_assert_eq!(decoded.offset.0, req.offset.0);
        prop_assert_eq!(decoded.max_bytes, req.max_bytes);
    }

    #[test]
    fn prop_read_response_roundtrip(resp in arb_read_response()) {
        let mut buf = vec![0u8; 64 * 1024];
        let encoded_len = reader::encode_read_response(&resp, &mut buf);
        let (decoded, decoded_len) = reader::decode_read_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.success, resp.success);
        prop_assert_eq!(decoded.error_code, resp.error_code);
        prop_assert_eq!(decoded.results.len(), resp.results.len());
        for (d, o) in decoded.results.iter().zip(resp.results.iter()) {
            prop_assert_eq!(d.topic_id.0, o.topic_id.0);
            prop_assert_eq!(d.schema_id.0, o.schema_id.0);
            prop_assert_eq!(d.high_watermark.0, o.high_watermark.0);
            prop_assert_eq!(d.records.len(), o.records.len());
            for (dr, or) in d.records.iter().zip(o.records.iter()) {
                prop_assert_eq!(&dr.value, &or.value);
                // Note: key comparison may differ for empty vs None due to protobuf3
                // semantics. Only compare when original key is non-empty.
                match (&dr.key, &or.key) {
                    (Some(dk), Some(ok)) if !ok.is_empty() => prop_assert_eq!(dk, ok),
                    (None, None) => {}
                    (None, Some(k)) if k.is_empty() => {} // protobuf3 conflates empty with absent
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn prop_poll_request_roundtrip(req in arb_poll_request()) {
        let mut buf = vec![0u8; 1024];
        let encoded_len = reader::encode_poll_request(&req, &mut buf);
        let (decoded, decoded_len) = reader::decode_poll_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, req.group_id);
        prop_assert_eq!(decoded.topic_id.0, req.topic_id.0);
        prop_assert_eq!(decoded.reader_id, req.reader_id);
        prop_assert_eq!(decoded.max_bytes, req.max_bytes);
    }

    #[test]
    fn prop_poll_response_roundtrip(resp in arb_poll_response()) {
        let mut buf = vec![0u8; 64 * 1024];
        let encoded_len = reader::encode_poll_response(&resp, &mut buf);
        let (decoded, decoded_len) = reader::decode_poll_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.success, resp.success);
        prop_assert_eq!(decoded.error_code, resp.error_code);
        prop_assert_eq!(decoded.start_offset.0, resp.start_offset.0);
        prop_assert_eq!(decoded.end_offset.0, resp.end_offset.0);
        prop_assert_eq!(decoded.lease_deadline_ms, resp.lease_deadline_ms);
        prop_assert_eq!(decoded.results.len(), resp.results.len());
        for (d, o) in decoded.results.iter().zip(resp.results.iter()) {
            prop_assert_eq!(d.topic_id.0, o.topic_id.0);
            prop_assert_eq!(d.schema_id.0, o.schema_id.0);
            prop_assert_eq!(d.high_watermark.0, o.high_watermark.0);
            prop_assert_eq!(d.records.len(), o.records.len());
            for (dr, or) in d.records.iter().zip(o.records.iter()) {
                prop_assert_eq!(&dr.value, &or.value);
            }
        }
    }
}

// ============ Reader Group Messages ============

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_join_group_roundtrip(
        group_id in "[a-z]{1,20}",
        reader_id in "[a-z0-9]{1,30}",
        topic_ids in prop::collection::vec(any::<u32>(), 0..10)
    ) {
        let req = reader::JoinGroupRequest {
            group_id: group_id.clone(),
            reader_id: reader_id.clone(),
            topic_ids: topic_ids.iter().map(|&id| TopicId(id)).collect(),
        };

        let mut buf = vec![0u8; 1024];
        let encoded_len = reader::encode_join_request(&req, &mut buf);
        let (decoded, decoded_len) = reader::decode_join_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, group_id);
        prop_assert_eq!(decoded.reader_id, reader_id);
        prop_assert_eq!(decoded.topic_ids.len(), topic_ids.len());
    }

    #[test]
    fn prop_heartbeat_roundtrip(
        group_id in "[a-z]{1,20}",
        topic_id: u32,
        reader_id in "[a-z0-9]{1,30}",
    ) {
        let req = reader::HeartbeatRequest {
            group_id: group_id.clone(),
            topic_id: TopicId(topic_id),
            reader_id: reader_id.clone(),
        };

        let mut buf = vec![0u8; 256];
        let encoded_len = reader::encode_heartbeat_request(&req, &mut buf);
        let (decoded, decoded_len) = reader::decode_heartbeat_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, group_id);
        prop_assert_eq!(decoded.topic_id.0, topic_id);
        prop_assert_eq!(decoded.reader_id, reader_id);
    }

    #[test]
    fn prop_heartbeat_response_ext_roundtrip(
        success: bool,
        error_code: u16,
        error_message in "[a-z]{0,50}",
        status_idx in 0u32..2,
    ) {
        let status = if status_idx == 0 {
            reader::HeartbeatStatus::Ok
        } else {
            reader::HeartbeatStatus::UnknownMember
        };

        let resp = reader::HeartbeatResponseExt {
            success,
            error_code,
            error_message: error_message.clone(),
            status,
        };

        let mut buf = vec![0u8; 256];
        let encoded_len = reader::encode_heartbeat_response_ext(&resp, &mut buf);
        let (decoded, decoded_len) = reader::decode_heartbeat_response_ext(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.success, success);
        prop_assert_eq!(decoded.error_code, error_code);
        prop_assert_eq!(decoded.error_message, error_message);
        prop_assert_eq!(decoded.status, status);
    }

    #[test]
    fn prop_commit_request_roundtrip(
        group_id in "[a-z]{1,20}",
        reader_id in "[a-z0-9]{1,30}",
        topic_id: u32,
        start_offset: u64,
        end_offset: u64
    ) {
        let req = reader::CommitRequest {
            group_id: group_id.clone(),
            reader_id: reader_id.clone(),
            topic_id: TopicId(topic_id),
            start_offset: Offset(start_offset),
            end_offset: Offset(end_offset),
        };

        let mut buf = vec![0u8; 1024];
        let encoded_len = reader::encode_commit_request(&req, &mut buf);
        let (decoded, decoded_len) = reader::decode_commit_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, group_id);
        prop_assert_eq!(decoded.reader_id, reader_id);
        prop_assert_eq!(decoded.topic_id.0, topic_id);
        prop_assert_eq!(decoded.start_offset.0, start_offset);
        prop_assert_eq!(decoded.end_offset.0, end_offset);
    }
}
