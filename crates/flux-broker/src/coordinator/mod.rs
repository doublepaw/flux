// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Reader group coordinator.
//!
//! Implements broker-side work dispatch using Postgres.
//! The broker hands out offset ranges to readers; no partition assignment.
//! All coordination state lives in the database - brokers are stateless.

mod db;

use flux_common::ids::Offset;
use std::time::Duration;

/// Coordinator configuration.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Inflight range lease duration.
    pub lease_duration: Duration,
    /// Session timeout for detecting dead readers.
    pub session_timeout: Duration,
    /// Broker ID for this coordinator instance.
    pub broker_id: uuid::Uuid,
    /// Maximum outstanding (uncommitted) poll ranges per reader.
    pub max_inflight_per_reader: u32,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(45),
            session_timeout: Duration::from_secs(30),
            broker_id: uuid::Uuid::new_v4(),
            max_inflight_per_reader: 10,
        }
    }
}

/// Heartbeat status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    Ok,
    UnknownMember,
}

/// Commit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStatus {
    Ok,
    NotOwner,
}

/// Poll status for flow control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    Ok,
    MaxInflight,
}

/// Result of a poll operation (broker-dispatched offset range).
#[derive(Debug)]
pub struct PollResult {
    pub start_offset: Offset,
    pub end_offset: Offset,
    pub lease_deadline_ms: u64,
    pub status: PollStatus,
}

pub use db::Coordinator;
