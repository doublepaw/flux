// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Error types for Iceberg ingestion.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IcebergError {
    #[error("schema error: {0}")]
    Schema(String),

    #[error("conversion error: {0}")]
    Conversion(String),

    #[error("iceberg error: {0}")]
    Iceberg(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("avro error: {0}")]
    Avro(#[from] Box<apache_avro::Error>),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, IcebergError>;
