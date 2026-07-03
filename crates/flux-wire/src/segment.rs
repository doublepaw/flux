// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! FL segment payload codec, shared by the broker and SDKs.
//!
//! A segment payload is the zstd-compressed concatenation of records, each
//! framed as:
//!
//! ```text
//! key:   zigzag varint i64 length (-1 = null key), then key bytes
//! value: zigzag varint i64 length, then value bytes
//! ```
//!
//! Integrity is a CRC-32 (IEEE) over the *compressed* bytes. This module is
//! the reference implementation of the format; the Java and Python SDKs
//! mirror it and must stay wire-compatible.

use bytes::BytesMut;

use flux_common::types::Record;

use crate::record;

/// ZSTD compression level used by the broker.
pub const ZSTD_LEVEL: i32 = 3;

/// Segment codec errors.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("crc32 mismatch: expected {expected}, got {actual}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("compression error: {0}")]
    Compression(String),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("record decode error at byte {at}: {msg}")]
    Decode { at: usize, msg: String },
}

/// Encode records into a compressed segment payload.
///
/// Returns the compressed bytes and their CRC-32.
pub fn encode_segment(records: &[Record]) -> Result<(Vec<u8>, u32), SegmentError> {
    let mut record_bytes = BytesMut::new();
    for rec in records {
        let key_len = rec.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let buf_size = key_len + rec.value.len() + 20;
        let mut buf = vec![0u8; buf_size];
        let len = record::encode(rec, &mut buf);
        record_bytes.extend_from_slice(&buf[..len]);
    }

    let compressed = zstd::encode_all(record_bytes.as_ref(), ZSTD_LEVEL)
        .map_err(|e| SegmentError::Compression(e.to_string()))?;
    let crc = crc32fast::hash(&compressed);
    Ok((compressed, crc))
}

/// Decode a compressed segment payload into records.
///
/// When `expected_crc` is provided, the CRC-32 of the compressed bytes is
/// verified first.
pub fn decode_segment(
    payload: &[u8],
    expected_crc: Option<u32>,
) -> Result<Vec<Record>, SegmentError> {
    if let Some(expected) = expected_crc {
        let actual = crc32fast::hash(payload);
        if actual != expected {
            return Err(SegmentError::CrcMismatch { expected, actual });
        }
    }

    let decompressed =
        zstd::decode_all(payload).map_err(|e| SegmentError::Decompression(e.to_string()))?;

    let mut records = Vec::new();
    let mut offset = 0;
    while offset < decompressed.len() {
        match record::decode(&decompressed[offset..]) {
            Ok((rec, len)) => {
                records.push(rec);
                offset += len;
            }
            Err(e) => {
                return Err(SegmentError::Decode {
                    at: offset,
                    msg: format!("{e:?}"),
                });
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_segment_round_trip() {
        let records = vec![
            Record {
                key: None,
                value: Bytes::from_static(b"hello"),
            },
            Record {
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from_static(b"world"),
            },
        ];
        let (payload, crc) = encode_segment(&records).unwrap();
        let decoded = decode_segment(&payload, Some(crc)).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, None);
        assert_eq!(decoded[0].value.as_ref(), b"hello");
        assert_eq!(decoded[1].key.as_deref(), Some(b"k".as_ref()));
        assert_eq!(decoded[1].value.as_ref(), b"world");
    }

    #[test]
    fn test_crc_mismatch_detected() {
        let records = vec![Record {
            key: None,
            value: Bytes::from_static(b"x"),
        }];
        let (payload, crc) = encode_segment(&records).unwrap();
        assert!(matches!(
            decode_segment(&payload, Some(crc ^ 1)),
            Err(SegmentError::CrcMismatch { .. })
        ));
    }
}
