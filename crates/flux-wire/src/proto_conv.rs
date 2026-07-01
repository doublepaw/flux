// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Shared protobuf conversion helpers used by reader, writer, and union modules.

use bytes::Bytes;
use flux_common::types::Record;
use prost::Message;

use crate::{DecodeError, EncodeError, proto};

pub(crate) fn encode_proto_checked<M: Message>(
    msg: &M,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let encoded = msg.encode_to_vec();
    if encoded.len() > buf.len() {
        return Err(EncodeError::BufferTooSmall {
            needed: encoded.len(),
            available: buf.len(),
        });
    }
    buf[..encoded.len()].copy_from_slice(&encoded);
    Ok(encoded.len())
}

pub(crate) fn encode_proto<M: Message>(msg: &M, buf: &mut [u8]) -> usize {
    encode_proto_checked(msg, buf).expect("buffer too small for protobuf message")
}

pub(crate) fn decode_proto<M: Message + Default>(
    buf: &[u8],
    err: &'static str,
) -> Result<M, DecodeError> {
    M::decode(buf).map_err(|_| DecodeError::InvalidData { msg: err })
}

pub(crate) fn to_proto_record(record: &Record) -> proto::Record {
    proto::Record {
        key: record.key.as_ref().map(|k| k.to_vec()),
        value: record.value.to_vec(),
    }
}

pub(crate) fn from_proto_record(record: proto::Record) -> Record {
    Record {
        key: record.key.map(Bytes::from),
        value: Bytes::from(record.value),
    }
}
