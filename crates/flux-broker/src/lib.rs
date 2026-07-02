// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Flux broker: stateless worker that handles append/read requests.
//!
//! The broker receives WebSocket connections from writers and readers,
//! batches writes to S3, and commits metadata to Postgres.

pub mod admin;
pub mod auth;
pub mod batched_server;
pub mod buffer;
pub mod coordinator;
pub mod dedup;
pub mod error;
pub mod fl;
pub mod metrics;
pub mod object_store;
pub mod shutdown;

pub use admin::{AdminConfig, AdminState, run_with_shutdown as run_admin_with_shutdown};
pub use auth::{AclChecker, ApiKeyValidator, AuthError, Operation, Principal, ResourceType};
pub use batched_server::{BrokerConfig, BrokerState, run, run_with_shutdown};
pub use buffer::{BatchKey, BrokerBuffer, BufferConfig, DrainResult, PendingWriter};
pub use coordinator::{
    CommitStatus, Coordinator, CoordinatorConfig, HeartbeatStatus, PollResult, PollStatus,
};
pub use dedup::{DedupCache, DedupCacheConfig, DedupResult, WriterState};
pub use error::BrokerError;
pub use fl::{FlReader, FlWriter};
pub use object_store::{CloudObjectStore, LocalFsStore, ObjectStore, S3ObjectStore};
pub use shutdown::{ConnectionTracker, TrackedConnection, shutdown_signal};
pub use tokio_util::sync::CancellationToken;
