// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema registry errors.

use flux_common::ids::{SchemaId, TopicId};
use thiserror::Error;

/// Schema registry error type.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("schema not found: {schema_id}")]
    SchemaNotFound { schema_id: SchemaId },

    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: TopicId },

    #[error("incompatible schema: {message}")]
    IncompatibleSchema { message: String },

    #[error("invalid schema: {message}")]
    InvalidSchema { message: String },

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("avro error: {0}")]
    Avro(String),
}

impl From<apache_avro::Error> for SchemaError {
    fn from(e: apache_avro::Error) -> Self {
        SchemaError::Avro(e.to_string())
    }
}
