// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Auth error types.

use thiserror::Error;

/// Authentication and authorization errors.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing API key")]
    MissingApiKey,

    #[error("invalid API key format")]
    InvalidKeyFormat,

    #[error("invalid API key")]
    InvalidApiKey,

    #[error("API key expired")]
    ExpiredApiKey,

    #[error("API key revoked")]
    RevokedApiKey,

    #[error("permission denied: {operation} on {resource_type}/{resource_name}")]
    PermissionDenied {
        operation: String,
        resource_type: String,
        resource_name: String,
    },

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("auth timeout")]
    Timeout,
}

impl AuthError {
    /// Returns true if this error indicates the client should retry authentication.
    pub fn is_retryable(&self) -> bool {
        matches!(self, AuthError::Database(_) | AuthError::Timeout)
    }

    /// Returns the error code for wire protocol responses.
    pub fn error_code(&self) -> u8 {
        match self {
            AuthError::MissingApiKey => 9,           // UNAUTHENTICATED
            AuthError::InvalidKeyFormat => 9,        // UNAUTHENTICATED
            AuthError::InvalidApiKey => 9,           // UNAUTHENTICATED
            AuthError::ExpiredApiKey => 9,           // UNAUTHENTICATED
            AuthError::RevokedApiKey => 9,           // UNAUTHENTICATED
            AuthError::PermissionDenied { .. } => 8, // UNAUTHORIZED
            AuthError::Database(_) => 7,             // INTERNAL
            AuthError::Timeout => 7,                 // INTERNAL
        }
    }
}
