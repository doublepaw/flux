// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Zigzag varint encoding/decoding (Avro-compatible).
//!
//! Zigzag encoding maps signed integers to unsigned:
//! 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
//!
//! Then encodes as variable-length bytes with continuation bit.

use crate::DecodeError;

/// Encode a signed 64-bit integer as zigzag varint, returning bytes written.
pub fn encode_i64(value: i64, buf: &mut [u8]) -> usize {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_u64(zigzag, buf)
}

/// Encode an unsigned 64-bit integer as varint, returning bytes written.
pub fn encode_u64(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i + 1
}

/// Decode a zigzag varint to signed 64-bit integer.
/// Returns (value, bytes_consumed).
pub fn decode_i64(buf: &[u8]) -> Result<(i64, usize), DecodeError> {
    let (zigzag, len) = decode_u64(buf)?;
    let value = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
    Ok((value, len))
}

/// Decode a varint to unsigned 64-bit integer.
/// Returns (value, bytes_consumed).
pub fn decode_u64(buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    let mut value: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 64 {
            return Err(DecodeError::InvalidVarint);
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
    }

    Err(DecodeError::UnexpectedEof { needed: 1 })
}

/// Encode a signed 32-bit integer as zigzag varint, returning bytes written.
#[inline]
pub fn encode_i32(value: i32, buf: &mut [u8]) -> usize {
    encode_i64(value as i64, buf)
}

/// Decode a zigzag varint to signed 32-bit integer.
/// Returns an error if the decoded value exceeds i32 range.
#[inline]
pub fn decode_i32(buf: &[u8]) -> Result<(i32, usize), DecodeError> {
    let (value, len) = decode_i64(buf)?;
    if value < i32::MIN as i64 || value > i32::MAX as i64 {
        return Err(DecodeError::I32Overflow { value });
    }
    Ok((value as i32, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag_encode_zero() {
        let mut buf = [0u8; 10];
        let len = encode_i64(0, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn test_zigzag_encode_negative_one() {
        let mut buf = [0u8; 10];
        let len = encode_i64(-1, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 1);
    }

    #[test]
    fn test_zigzag_encode_positive_one() {
        let mut buf = [0u8; 10];
        let len = encode_i64(1, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 2);
    }

    #[test]
    fn test_zigzag_encode_negative_two() {
        let mut buf = [0u8; 10];
        let len = encode_i64(-2, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 3);
    }

    #[test]
    fn test_zigzag_encode_64() {
        let mut buf = [0u8; 10];
        let len = encode_i64(64, &mut buf);
        // 64 zigzag -> 128, which needs 2 bytes: 0x80, 0x01
        assert_eq!(len, 2);
        assert_eq!(buf[0], 0x80);
        assert_eq!(buf[1], 0x01);
    }

    #[test]
    fn test_decode_roundtrip_small() {
        let mut buf = [0u8; 10];
        for value in -100..=100 {
            let len = encode_i64(value, &mut buf);
            let (decoded, decoded_len) = decode_i64(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(decoded_len, len);
        }
    }

    #[test]
    fn test_decode_roundtrip_large() {
        let mut buf = [0u8; 10];
        let values = [
            i64::MIN,
            i64::MIN + 1,
            i64::MAX,
            i64::MAX - 1,
            1_000_000,
            -1_000_000,
            i32::MIN as i64,
            i32::MAX as i64,
        ];
        for value in values {
            let len = encode_i64(value, &mut buf);
            let (decoded, decoded_len) = decode_i64(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(decoded_len, len);
        }
    }

    #[test]
    fn test_decode_empty_buffer() {
        let result = decode_i64(&[]);
        assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
    }

    #[test]
    fn test_decode_truncated_varint() {
        // 0x80 indicates more bytes follow, but there are none
        let result = decode_i64(&[0x80]);
        assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
    }

    #[test]
    fn test_decode_i32_overflow() {
        let mut buf = [0u8; 10];
        // Encode a value that fits in i64 but not i32
        let len = encode_i64(i32::MAX as i64 + 1, &mut buf);
        let result = decode_i32(&buf[..len]);
        assert!(matches!(result, Err(DecodeError::I32Overflow { .. })));

        let len = encode_i64(i32::MIN as i64 - 1, &mut buf);
        let result = decode_i32(&buf[..len]);
        assert!(matches!(result, Err(DecodeError::I32Overflow { .. })));

        let len = encode_i64(i64::MAX, &mut buf);
        let result = decode_i32(&buf[..len]);
        assert!(matches!(result, Err(DecodeError::I32Overflow { .. })));
    }

    #[test]
    fn test_decode_i32_boundary_values() {
        let mut buf = [0u8; 10];
        // i32::MAX and i32::MIN should succeed
        let len = encode_i32(i32::MAX, &mut buf);
        let (decoded, _) = decode_i32(&buf[..len]).unwrap();
        assert_eq!(decoded, i32::MAX);

        let len = encode_i32(i32::MIN, &mut buf);
        let (decoded, _) = decode_i32(&buf[..len]).unwrap();
        assert_eq!(decoded, i32::MIN);
    }

    #[test]
    fn test_u64_roundtrip() {
        let mut buf = [0u8; 10];
        let values = [0u64, 1, 127, 128, 255, 256, u32::MAX as u64, u64::MAX];
        for value in values {
            let len = encode_u64(value, &mut buf);
            let (decoded, decoded_len) = decode_u64(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(decoded_len, len);
        }
    }
}
