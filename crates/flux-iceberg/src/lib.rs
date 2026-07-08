// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Iceberg ingestion for Flux.
//!
//! Converts Avro-encoded records from FL flushes into Iceberg tables.
//! Two paths:
//! - **Hot path**: records are pushed from the broker flush loop into
//!   per-table buffers and flushed to Iceberg on size/time triggers.
//! - **Catch-up path**: a periodic task finds un-ingested batches via
//!   an anti-join on `iceberg_claims` and re-reads them from S3.

pub mod catchup;
pub mod committer;
pub mod config;
pub mod error;
pub mod iceberg_buffer;
pub mod iceberg_writer;
pub mod record_converter;
pub mod schema_evolution;
pub mod schema_mapping;
pub mod tracking;

pub use config::IcebergConfig;
pub use error::{IcebergError, Result};
pub use iceberg_buffer::IcebergBuffer;
pub use record_converter::RecordConverter;
