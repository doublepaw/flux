// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Configuration for Iceberg ingestion.

use std::time::Duration;

/// Iceberg ingestion configuration.
#[derive(Debug, Clone)]
pub struct IcebergConfig {
    /// Per-table flush size trigger (default 128 MB).
    pub table_flush_size: usize,
    /// Per-table flush time trigger (default 60s).
    pub table_flush_interval: Duration,
    /// Total Iceberg buffer memory budget (default 4 GB).
    pub global_memory_cap: usize,
    /// S3 warehouse location for Iceberg tables.
    pub warehouse: String,
    /// Iceberg catalog namespace (default "flux").
    pub namespace: String,
    /// Catalog flavor: "rest" (deployments; engine-visible) or "sql"
    /// (dev/tests on the flux Postgres).
    pub catalog_type: String,
    /// Catalog connection string: REST endpoint URL or Postgres URI.
    pub catalog_uri: String,
    /// Committer lease TTL; renewed on every commit attempt.
    pub committer_lease_secs: i64,
    /// Catch-up scan frequency (default 30s).
    pub catchup_interval: Duration,
    /// Claim expiry for stale pending claims (default 5 min).
    pub claim_expiry: Duration,
    /// Max batches to claim per catch-up cycle.
    pub catchup_batch_limit: i64,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            table_flush_size: 128 * 1024 * 1024, // 128 MB
            table_flush_interval: Duration::from_secs(60),
            global_memory_cap: 4 * 1024 * 1024 * 1024, // 4 GB
            warehouse: String::new(),
            namespace: "flux".to_string(),
            catalog_type: "sql".to_string(),
            catalog_uri: String::new(),
            committer_lease_secs: 30,
            catchup_interval: Duration::from_secs(30),
            claim_expiry: Duration::from_secs(300), // 5 min
            catchup_batch_limit: 100,
        }
    }
}

impl IcebergConfig {
    /// Build config from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(v) = std::env::var("ICEBERG_TABLE_FLUSH_SIZE")
            && let Ok(n) = v.parse()
        {
            config.table_flush_size = n;
        }
        if let Ok(v) = std::env::var("ICEBERG_TABLE_FLUSH_INTERVAL_SECS")
            && let Ok(n) = v.parse()
        {
            config.table_flush_interval = Duration::from_secs(n);
        }
        if let Ok(v) = std::env::var("ICEBERG_GLOBAL_MEMORY_CAP")
            && let Ok(n) = v.parse()
        {
            config.global_memory_cap = n;
        }
        if let Ok(v) = std::env::var("ICEBERG_WAREHOUSE") {
            config.warehouse = v;
        }
        if let Ok(v) = std::env::var("ICEBERG_NAMESPACE") {
            config.namespace = v;
        }
        if let Ok(v) = std::env::var("ICEBERG_CATALOG") {
            config.catalog_type = v;
        }
        if let Ok(v) = std::env::var("ICEBERG_CATALOG_URI") {
            config.catalog_uri = v;
        }
        if let Ok(v) = std::env::var("ICEBERG_COMMITTER_LEASE_SECS")
            && let Ok(n) = v.parse()
        {
            config.committer_lease_secs = n;
        }
        if let Ok(v) = std::env::var("ICEBERG_CATCHUP_INTERVAL_SECS")
            && let Ok(n) = v.parse()
        {
            config.catchup_interval = Duration::from_secs(n);
        }
        if let Ok(v) = std::env::var("ICEBERG_CLAIM_EXPIRY_SECS")
            && let Ok(n) = v.parse()
        {
            config.claim_expiry = Duration::from_secs(n);
        }
        if let Ok(v) = std::env::var("ICEBERG_CATCHUP_BATCH_LIMIT")
            && let Ok(n) = v.parse()
        {
            config.catchup_batch_limit = n;
        }

        config
    }
}
