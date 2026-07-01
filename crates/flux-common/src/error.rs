// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Error types for Flux eventbus

use crate::{TopicId, WriterId};
use thiserror::Error;

/// Error codes matching wire protocol error messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Topic not found
    TopicNotFound = 1,
    /// Schema not found
    SchemaNotFound = 3,
    /// Schema incompatible with existing schemas
    IncompatibleSchema = 4,
    /// Invalid offset requested
    InvalidOffset = 5,
    /// Reader group not found
    GroupNotFound = 6,
    /// Reader not a member of the group
    NotMember = 7,
    /// Reader does not own the requested range
    NotOwner = 9,
    /// Request rate limit exceeded
    RateLimited = 10,
    /// Internal server error
    InternalError = 11,
    /// Authentication failed
    Unauthenticated = 12,
    /// Authorization failed
    Unauthorized = 13,
    /// Duplicate writer sequence number
    DuplicateSequence = 14,
    /// Invalid sequence number (gap detected)
    InvalidSequence = 15,
}

impl ErrorCode {
    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, ErrorCode::RateLimited | ErrorCode::InternalError)
    }
}

/// Main error type for Flux operations
#[derive(Debug, Error)]
pub enum FluxError {
    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: TopicId },

    #[error("schema not found: {schema_id}")]
    SchemaNotFound { schema_id: u32 },

    #[error("incompatible schema: {message}")]
    IncompatibleSchema { message: String },

    #[error("invalid offset: {offset}")]
    InvalidOffset { offset: u64 },

    #[error("group not found: {group_id}")]
    GroupNotFound { group_id: String },

    #[error("not a member of group: {group_id}")]
    NotMember { group_id: String },

    #[error("not owner of range")]
    NotOwner,

    #[error("rate limited")]
    RateLimited,

    #[error("internal error: {message}")]
    InternalError { message: String },

    #[error("unauthenticated")]
    Unauthenticated,

    #[error("unauthorized: {action}")]
    Unauthorized { action: String },

    #[error("duplicate sequence: writer={writer_id}, append_seq={append_seq}")]
    DuplicateSequence {
        writer_id: WriterId,
        append_seq: u64,
    },

    #[error("invalid sequence: expected {expected}, got {actual}")]
    InvalidSequence { expected: u64, actual: u64 },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {message}")]
    Database { message: String },

    #[error("encoding error: {message}")]
    Encoding { message: String },
}

impl FluxError {
    /// Get the error code for this error
    pub fn code(&self) -> ErrorCode {
        match self {
            FluxError::TopicNotFound { .. } => ErrorCode::TopicNotFound,
            FluxError::SchemaNotFound { .. } => ErrorCode::SchemaNotFound,
            FluxError::IncompatibleSchema { .. } => ErrorCode::IncompatibleSchema,
            FluxError::InvalidOffset { .. } => ErrorCode::InvalidOffset,
            FluxError::GroupNotFound { .. } => ErrorCode::GroupNotFound,
            FluxError::NotMember { .. } => ErrorCode::NotMember,
            FluxError::NotOwner => ErrorCode::NotOwner,
            FluxError::RateLimited => ErrorCode::RateLimited,
            FluxError::InternalError { .. } => ErrorCode::InternalError,
            FluxError::Unauthenticated => ErrorCode::Unauthenticated,
            FluxError::Unauthorized { .. } => ErrorCode::Unauthorized,
            FluxError::DuplicateSequence { .. } => ErrorCode::DuplicateSequence,
            FluxError::InvalidSequence { .. } => ErrorCode::InvalidSequence,
            FluxError::Io(_) => ErrorCode::InternalError,
            FluxError::Database { .. } => ErrorCode::InternalError,
            FluxError::Encoding { .. } => ErrorCode::InternalError,
        }
    }

    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }
}

/// Result type for Flux operations
pub type Result<T> = std::result::Result<T, FluxError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = FluxError::TopicNotFound {
            topic_id: TopicId(42),
        };
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(
            !FluxError::TopicNotFound {
                topic_id: TopicId(1)
            }
            .is_retryable()
        );
        assert!(
            FluxError::InternalError {
                message: "oops".into()
            }
            .is_retryable()
        );
        assert!(FluxError::RateLimited.is_retryable());
    }

    #[test]
    fn test_error_code() {
        let err = FluxError::TopicNotFound {
            topic_id: TopicId(1),
        };
        assert_eq!(err.code(), ErrorCode::TopicNotFound);

        let err = FluxError::RateLimited;
        assert_eq!(err.code(), ErrorCode::RateLimited);
    }

    #[test]
    fn test_error_code_is_retryable() {
        assert!(ErrorCode::RateLimited.is_retryable());
        assert!(ErrorCode::InternalError.is_retryable());
        assert!(!ErrorCode::TopicNotFound.is_retryable());
        assert!(!ErrorCode::Unauthenticated.is_retryable());
    }
}
