// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema registry for Flux eventbus.
//!
//! Provides centralized schema storage with:
//! - Content-addressed deduplication (SHA-256 hash)
//! - Backward compatibility enforcement
//! - Topic-schema association
//!
//! Reserved schema IDs 1-99 are for protocol schemas.
//! User schemas start at ID 100.

pub mod api;
pub mod canonical;
pub mod compat;
pub mod error;
pub mod registry;

pub use api::{AppState, router};
pub use canonical::{canonicalize, schema_hash};
pub use compat::is_backward_compatible;
pub use error::SchemaError;
pub use registry::SchemaRegistry;
