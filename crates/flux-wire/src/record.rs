// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Record encoding/decoding.
//!
//! A Record contains an optional key and a value, both as bytes.
//! Encoded as: key_len (varint, -1 for null) + key_bytes + value_len (varint) + value_bytes

use bytes::Bytes;
use flux_common::types::Record;

use crate::DecodeError;
use crate::varint;

/// Encode a record into the buffer, returning bytes written.
pub fn encode(record: &Record, buf: &mut [u8]) -> usize {
    let mut offset = 0;

    // Key: -1 for null, otherwise length + bytes
    match &record.key {
        None => {
            offset += varint::encode_i64(-1, &mut buf[offset..]);
        }
        Some(key) => {
            offset += varint::encode_i64(key.len() as i64, &mut buf[offset..]);
            buf[offset..offset + key.len()].copy_from_slice(key);
            offset += key.len();
        }
    }

    // Value: length + bytes
    offset += varint::encode_i64(record.value.len() as i64, &mut buf[offset..]);
    buf[offset..offset + record.value.len()].copy_from_slice(&record.value);
    offset += record.value.len();

    offset
}

/// Decode a record from the buffer, returning (record, bytes_consumed).
pub fn decode(buf: &[u8]) -> Result<(Record, usize), DecodeError> {
    let mut offset = 0;

    // Key
    let (key_len, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let key = if key_len < 0 {
        None
    } else {
        let key_len = key_len as usize;
        if buf.len() < offset + key_len {
            return Err(DecodeError::UnexpectedEof { needed: key_len });
        }
        let key = Bytes::copy_from_slice(&buf[offset..offset + key_len]);
        offset += key_len;
        Some(key)
    };

    // Value
    let (value_len, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    if value_len < 0 {
        return Err(DecodeError::UnexpectedEof { needed: 0 });
    }
    let value_len = value_len as usize;
    if buf.len() < offset + value_len {
        return Err(DecodeError::UnexpectedEof { needed: value_len });
    }
    let value = Bytes::copy_from_slice(&buf[offset..offset + value_len]);
    offset += value_len;

    Ok((Record { key, value }, offset))
}

/// Calculate the encoded size of a record without actually encoding.
pub fn encoded_size(record: &Record) -> usize {
    let mut size = 0;

    // Key length varint + key bytes
    match &record.key {
        None => size += 1, // -1 encoded as single byte
        Some(key) => {
            size += varint_size(key.len() as i64);
            size += key.len();
        }
    }

    // Value length varint + value bytes
    size += varint_size(record.value.len() as i64);
    size += record.value.len();

    size
}

/// Calculate the encoded size of a zigzag varint.
fn varint_size(value: i64) -> usize {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    let mut v = zigzag;
    let mut size = 1;
    while v >= 0x80 {
        v >>= 7;
        size += 1;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_with_key() {
        let record = Record {
            key: Some(Bytes::from_static(b"my-key")),
            value: Bytes::from_static(b"my-value"),
        };

        let mut buf = [0u8; 64];
        let encoded_len = encode(&record, &mut buf);

        let (decoded, decoded_len) = decode(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn test_encode_decode_without_key() {
        let record = Record {
            key: None,
            value: Bytes::from_static(b"value-only"),
        };

        let mut buf = [0u8; 64];
        let encoded_len = encode(&record, &mut buf);

        let (decoded, decoded_len) = decode(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.key, None);
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn test_encode_decode_empty_value() {
        let record = Record {
            key: Some(Bytes::from_static(b"key")),
            value: Bytes::new(),
        };

        let mut buf = [0u8; 64];
        let encoded_len = encode(&record, &mut buf);

        let (decoded, decoded_len) = decode(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value.len(), 0);
    }

    #[test]
    fn test_encoded_size_matches_actual() {
        let records = [
            Record {
                key: None,
                value: Bytes::from_static(b"test"),
            },
            Record {
                key: Some(Bytes::from_static(b"key")),
                value: Bytes::from_static(b"value"),
            },
            Record {
                key: Some(Bytes::new()),
                value: Bytes::new(),
            },
        ];

        for record in records {
            let mut buf = [0u8; 64];
            let actual_len = encode(&record, &mut buf);
            let predicted_len = encoded_size(&record);
            assert_eq!(actual_len, predicted_len);
        }
    }

    #[test]
    fn test_decode_truncated_key() {
        // Encode key length of 10, but only provide 5 bytes
        let mut buf = [0u8; 10];
        let len = varint::encode_i64(10, &mut buf);
        buf[len..len + 5].copy_from_slice(b"short");

        let result = decode(&buf[..len + 5]);
        assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
    }

    #[test]
    fn test_decode_truncated_value() {
        // Key: none (-1), value length: 10, only 3 bytes of value
        let mut buf = [0u8; 10];
        let mut offset = 0;
        offset += varint::encode_i64(-1, &mut buf[offset..]);
        offset += varint::encode_i64(10, &mut buf[offset..]);
        buf[offset..offset + 3].copy_from_slice(b"abc");

        let result = decode(&buf[..offset + 3]);
        assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
    }
}
