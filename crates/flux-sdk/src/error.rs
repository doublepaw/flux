// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! SDK error types.

use thiserror::Error;

/// SDK error type.
#[derive(Debug, Error)]
pub enum SdkError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("WebSocket error: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    #[error("encoding error: {0}")]
    Encode(String),

    #[error("decoding error: {0}")]
    Decode(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("server error {code}: {message}")]
    Server { code: u16, message: String },

    #[error("timeout")]
    Timeout,

    #[error("backpressure: server is overloaded")]
    Backpressure,

    #[error("disconnected")]
    Disconnected,

    #[error("invalid response")]
    InvalidResponse,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("commit failed")]
    CommitFailed,
}

impl From<tokio_tungstenite::tungstenite::Error> for SdkError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        SdkError::WebSocket(Box::new(e))
    }
}
