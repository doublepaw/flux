// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Wire protocol encoding/decoding errors.

use thiserror::Error;

/// Error during encoding.
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("value too large to encode: {0}")]
    ValueTooLarge(String),
}

/// Error during decoding.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unexpected end of input: need {needed} more bytes")]
    UnexpectedEof { needed: usize },

    #[error("invalid varint: too many bytes")]
    InvalidVarint,

    #[error("invalid union index: {index}")]
    InvalidUnionIndex { index: u8 },

    #[error("invalid utf-8 string")]
    InvalidUtf8,

    #[error("invalid message type: {tag}")]
    InvalidMessageType { tag: u8 },

    #[error("invalid data: {msg}")]
    InvalidData { msg: &'static str },

    #[error("varint value {value} overflows i32 range")]
    I32Overflow { value: i64 },
}
