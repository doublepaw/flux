// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Fast Avro deserialization with zero-copy support.
//!
//! ## Features
//!
//! - **Zero-copy deserialization**: Strings and bytes borrow directly from input
//! - **Bump allocation**: Arena-based allocation for high-throughput batch processing
//! - **Dynamic Value types**: `BumpValue` and `Value` for schema-agnostic data handling

pub mod de;
pub mod schema;
pub mod value;

pub use schema::Schema;
pub use value::{GenericRecord, Value};
