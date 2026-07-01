// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FluxError {
    #[error("Avro parsing error: {0}")]
    AvroParse(Box<apache_avro::Error>),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Conversion error: {0}")]
    Conversion(String),
}

impl IntoResponse for FluxError {
    fn into_response(self) -> Response {
        let status = match &self {
            FluxError::AvroParse(_) => StatusCode::BAD_REQUEST,
            FluxError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
            FluxError::Conversion(_) => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, self.to_string()).into_response()
    }
}

impl From<apache_avro::Error> for FluxError {
    fn from(e: apache_avro::Error) -> Self {
        FluxError::AvroParse(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, FluxError>;
