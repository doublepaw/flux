// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Wire-level status and error codes.

/// Successful response.
pub const STATUS_OK: u16 = 0;

/// Authorization denied.
pub const ERR_AUTHZ_DENIED: u16 = 1001;
/// Request payload decode failed.
pub const ERR_DECODE_ERROR: u16 = 1002;
/// Unknown or unsupported message type.
pub const ERR_INVALID_MESSAGE_TYPE: u16 = 1003;
/// Backpressure active; client should retry.
pub const ERR_BACKPRESSURE: u16 = 1004;
/// Internal server failure.
pub const ERR_INTERNAL_ERROR: u16 = 1005;
/// Commit rejected because reader does not own the range.
pub const ERR_NOT_OWNER: u16 = 1006;
/// Sequence number is stale.
pub const ERR_STALE_SEQUENCE: u16 = 1007;
/// Another sequence for same writer is currently in flight.
pub const ERR_SEQUENCE_IN_FLIGHT: u16 = 1008;
/// Reader has too many outstanding (uncommitted) poll ranges.
pub const ERR_MAX_INFLIGHT: u16 = 1009;
