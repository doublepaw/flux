// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Fault injection infrastructure for testing error handling and recovery.

use bytes::Bytes;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use flux_broker::buffer::BufferConfig;
use flux_broker::{BrokerConfig, BrokerState, LocalFsStore};

use super::{TestBrokerConfig, TestBrokerState};

/// Wraps an ObjectStore with fault injection capability.
#[derive(Clone)]
pub struct FaultyObjectStore {
    inner: Arc<LocalFsStore>,
    fail_next_put: Arc<AtomicBool>,
    fail_next_get: Arc<AtomicBool>,
    fail_next_get_range: Arc<AtomicBool>,
    /// 1-indexed: fail on this put number and all subsequent puts
    fail_on_put_n: Arc<AtomicU32>,
    put_count: Arc<AtomicU32>,
    /// Delay in milliseconds before each put (simulates slow S3)
    put_delay_ms: Arc<AtomicU64>,
    /// Delay in milliseconds before each get (simulates slow reads)
    get_delay_ms: Arc<AtomicU64>,
    /// Fail next N puts then auto-recover
    fail_put_remaining: Arc<AtomicU32>,
    /// Black-hole mode: puts hang until healed (simulates network partition)
    black_hole_put: Arc<AtomicBool>,
    /// Black-hole mode: gets hang until healed
    black_hole_get: Arc<AtomicBool>,
    /// Notified when partition is healed
    unpartition_notify: Arc<Notify>,
    /// Corrupt next get_range response (flip first byte)
    corrupt_next_get_range: Arc<AtomicBool>,
    /// Truncate next get_range response (return half the data)
    truncate_next_get_range: Arc<AtomicBool>,
}

impl FaultyObjectStore {
    pub fn new(store: LocalFsStore) -> Self {
        Self::new_shared(Arc::new(store))
    }

    /// Create with independent fault state but shared underlying filesystem.
    pub fn new_shared(inner: Arc<LocalFsStore>) -> Self {
        Self {
            inner,
            fail_next_put: Arc::new(AtomicBool::new(false)),
            fail_next_get: Arc::new(AtomicBool::new(false)),
            fail_next_get_range: Arc::new(AtomicBool::new(false)),
            fail_on_put_n: Arc::new(AtomicU32::new(u32::MAX)),
            put_count: Arc::new(AtomicU32::new(0)),
            put_delay_ms: Arc::new(AtomicU64::new(0)),
            get_delay_ms: Arc::new(AtomicU64::new(0)),
            fail_put_remaining: Arc::new(AtomicU32::new(0)),
            black_hole_put: Arc::new(AtomicBool::new(false)),
            black_hole_get: Arc::new(AtomicBool::new(false)),
            unpartition_notify: Arc::new(Notify::new()),
            corrupt_next_get_range: Arc::new(AtomicBool::new(false)),
            truncate_next_get_range: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn inner(&self) -> &Arc<LocalFsStore> {
        &self.inner
    }

    pub fn fail_next_put(&self) {
        self.fail_next_put.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_get(&self) {
        self.fail_next_get.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_get_range(&self) {
        self.fail_next_get_range.store(true, Ordering::SeqCst);
    }

    /// Fail on the Nth put and all subsequent puts.
    pub fn fail_on_put_n(&self, n: u32) {
        self.fail_on_put_n.store(n, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }

    /// Add delay before each put (simulates slow S3).
    pub fn set_put_delay_ms(&self, ms: u64) {
        self.put_delay_ms.store(ms, Ordering::SeqCst);
    }

    /// Add delay before each get (simulates slow reads).
    pub fn set_get_delay_ms(&self, ms: u64) {
        self.get_delay_ms.store(ms, Ordering::SeqCst);
    }

    /// Fail next N puts, then auto-recover.
    pub fn fail_put_transiently(&self, n: u32) {
        self.fail_put_remaining.store(n, Ordering::SeqCst);
    }

    /// Partition puts: all put calls hang until `heal_partition` is called.
    pub fn partition_puts(&self) {
        self.black_hole_put.store(true, Ordering::SeqCst);
    }

    /// Partition gets: all get/get_range calls hang until `heal_partition` is called.
    pub fn partition_gets(&self) {
        self.black_hole_get.store(true, Ordering::SeqCst);
    }

    /// Corrupt next get_range response (flip first byte).
    pub fn corrupt_next_get_range(&self) {
        self.corrupt_next_get_range.store(true, Ordering::SeqCst);
    }

    /// Truncate next get_range response (return half the data).
    pub fn truncate_next_get_range(&self) {
        self.truncate_next_get_range.store(true, Ordering::SeqCst);
    }

    /// Heal all partitions: unblock hanging puts and gets.
    pub fn heal_partition(&self) {
        self.black_hole_put.store(false, Ordering::SeqCst);
        self.black_hole_get.store(false, Ordering::SeqCst);
        self.unpartition_notify.notify_waiters();
    }

    pub fn reset(&self) {
        self.fail_next_put.store(false, Ordering::SeqCst);
        self.fail_next_get.store(false, Ordering::SeqCst);
        self.fail_next_get_range.store(false, Ordering::SeqCst);
        self.fail_on_put_n.store(u32::MAX, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
        self.put_delay_ms.store(0, Ordering::SeqCst);
        self.get_delay_ms.store(0, Ordering::SeqCst);
        self.fail_put_remaining.store(0, Ordering::SeqCst);
        self.black_hole_put.store(false, Ordering::SeqCst);
        self.black_hole_get.store(false, Ordering::SeqCst);
        self.corrupt_next_get_range.store(false, Ordering::SeqCst);
        self.truncate_next_get_range.store(false, Ordering::SeqCst);
        self.unpartition_notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl flux_broker::ObjectStore for FaultyObjectStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
    ) -> Result<(), flux_broker::object_store::ObjectStoreError> {
        // Black-hole: hang until healed
        if self.black_hole_put.load(Ordering::SeqCst) {
            self.unpartition_notify.notified().await;
            if self.black_hole_put.load(Ordering::SeqCst) {
                return Err(flux_broker::object_store::ObjectStoreError::Io(
                    std::io::Error::other("injected fault: put partitioned"),
                ));
            }
        }

        // Transient failure: atomically decrement remaining count, fail if was > 0
        let was_positive =
            self.fail_put_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    if n > 0 { Some(n - 1) } else { None }
                });
        if was_positive.is_ok() {
            return Err(flux_broker::object_store::ObjectStoreError::Io(
                std::io::Error::other("injected fault: transient put failure"),
            ));
        }

        // Persistent failure after N puts
        let count = self.put_count.fetch_add(1, Ordering::SeqCst);
        let put_number = count + 1;
        if put_number >= self.fail_on_put_n.load(Ordering::SeqCst) {
            return Err(flux_broker::object_store::ObjectStoreError::Io(
                std::io::Error::other("injected fault: put failed"),
            ));
        }

        // One-shot failure
        if self.fail_next_put.swap(false, Ordering::SeqCst) {
            return Err(flux_broker::object_store::ObjectStoreError::Io(
                std::io::Error::other("injected fault: put failed"),
            ));
        }

        // Delay (simulates slow S3)
        let delay_ms = self.put_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        self.inner.put(key, data).await
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Bytes, flux_broker::object_store::ObjectStoreError> {
        // Black-hole: hang until healed
        if self.black_hole_get.load(Ordering::SeqCst) {
            self.unpartition_notify.notified().await;
            if self.black_hole_get.load(Ordering::SeqCst) {
                return Err(flux_broker::object_store::ObjectStoreError::Io(
                    std::io::Error::other("injected fault: get partitioned"),
                ));
            }
        }

        // Delay (simulates slow reads)
        let delay_ms = self.get_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        if self.fail_next_get.swap(false, Ordering::SeqCst) {
            return Err(flux_broker::object_store::ObjectStoreError::Io(
                std::io::Error::other("injected fault: get failed"),
            ));
        }
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        start: u64,
        len: u64,
    ) -> Result<Bytes, flux_broker::object_store::ObjectStoreError> {
        // Black-hole: hang until healed
        if self.black_hole_get.load(Ordering::SeqCst) {
            self.unpartition_notify.notified().await;
            if self.black_hole_get.load(Ordering::SeqCst) {
                return Err(flux_broker::object_store::ObjectStoreError::Io(
                    std::io::Error::other("injected fault: get_range partitioned"),
                ));
            }
        }

        // One-shot get_range failure
        if self.fail_next_get_range.swap(false, Ordering::SeqCst) {
            return Err(flux_broker::object_store::ObjectStoreError::Io(
                std::io::Error::other("injected fault: get_range failed"),
            ));
        }

        let delay_ms = self.get_delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let result = self.inner.get_range(key, start, len).await?;

        // Corrupt: flip first byte
        if self.corrupt_next_get_range.swap(false, Ordering::SeqCst) {
            let mut data = result.to_vec();
            if !data.is_empty() {
                data[0] ^= 0xFF;
            }
            return Ok(Bytes::from(data));
        }

        // Truncate: return half the data
        if self.truncate_next_get_range.swap(false, Ordering::SeqCst) {
            let data = result.to_vec();
            let half = data.len() / 2;
            return Ok(Bytes::from(data[..half].to_vec()));
        }

        Ok(result)
    }

    async fn delete(
        &self,
        key: &str,
    ) -> Result<(), flux_broker::object_store::ObjectStoreError> {
        self.inner.delete(key).await
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, flux_broker::object_store::ObjectStoreError> {
        self.inner.list(prefix).await
    }

    async fn size(
        &self,
        key: &str,
    ) -> Result<u64, flux_broker::object_store::ObjectStoreError> {
        self.inner.size(key).await
    }
}

/// Blocks `commit_batch` by holding a `FOR UPDATE` row lock on `topic_offsets`.
pub struct DbBlocker {
    pool: PgPool,
    tx: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl DbBlocker {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, tx: None }
    }

    /// Begin transaction and lock the target topic_offsets row.
    /// This stalls `commit_batch` at the offset allocation step.
    pub async fn block_topic(&mut self, topic_id: i32) {
        // Ensure the row exists so FOR UPDATE has something to lock
        sqlx::query(
            "INSERT INTO topic_offsets (topic_id, next_offset) \
             VALUES ($1, 0) ON CONFLICT DO NOTHING",
        )
        .bind(topic_id)
        .execute(&self.pool)
        .await
        .expect("Failed to ensure topic_offsets row exists");

        let mut tx = self
            .pool
            .begin()
            .await
            .expect("Failed to begin blocking transaction");
        let _: (i64,) = sqlx::query_as(
            "SELECT next_offset FROM topic_offsets \
             WHERE topic_id = $1 FOR UPDATE",
        )
        .bind(topic_id)
        .fetch_one(&mut *tx)
        .await
        .expect("Failed to lock topic_offsets row");

        self.tx = Some(tx);
    }

    /// Roll back the transaction, releasing the lock.
    pub async fn unblock(&mut self) {
        if let Some(tx) = self.tx.take() {
            tx.rollback()
                .await
                .expect("Failed to rollback blocking transaction");
        }
    }
}

/// Simulates broker crash/restart cycle for testing recovery behavior.
/// Uses TestBrokerState (no flush loop).
pub struct CrashableBroker {
    state: Option<Arc<TestBrokerState<FaultyObjectStore>>>,
    pub pool: PgPool,
    store: FaultyObjectStore,
    config: TestBrokerConfig,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl CrashableBroker {
    pub async fn new(pool: PgPool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let config = TestBrokerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            bucket: "test-bucket".to_string(),
            key_prefix: "jepsen-test".to_string(),
        };
        let state = TestBrokerState::new(pool.clone(), store.clone(), config.clone());

        Self {
            state: Some(Arc::new(state)),
            pool,
            store,
            config,
            temp_dir,
        }
    }

    pub fn state(&self) -> &Arc<TestBrokerState<FaultyObjectStore>> {
        self.state.as_ref().expect("Broker has crashed")
    }

    pub fn crash(&mut self) {
        self.state = None;
    }

    pub fn restart(&mut self) {
        self.store.reset();
        let state =
            TestBrokerState::new(self.pool.clone(), self.store.clone(), self.config.clone());
        self.state = Some(Arc::new(state));
    }

    pub fn is_crashed(&self) -> bool {
        self.state.is_none()
    }

    pub fn faulty_store(&self) -> &FaultyObjectStore {
        &self.store
    }
}

/// Full broker with WebSocket + flush loop for testing real-world fault scenarios.
pub struct CrashableWsBroker {
    pub pool: PgPool,
    store: FaultyObjectStore,
    addr: SocketAddr,
    server_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    cancel_token: Option<flux_broker::CancellationToken>,
    coordinator_config: Option<flux_broker::CoordinatorConfig>,
    temp_dir: TempDir,
}

impl CrashableWsBroker {
    pub async fn start(pool: PgPool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let mut broker = Self {
            pool,
            store,
            addr: "127.0.0.1:0".parse().unwrap(),
            server_handle: None,
            shutdown_tx: None,
            cancel_token: None,
            coordinator_config: None,
            temp_dir,
        };
        broker.boot().await;
        broker
    }

    /// Start a broker with a shared store (for multi-broker setups).
    pub async fn start_shared(pool: PgPool, store: FaultyObjectStore, data_dir: PathBuf) -> Self {
        // Create a subdirectory for this broker's temp data
        let temp_dir = TempDir::new_in(data_dir).expect("Failed to create temp dir");
        let mut broker = Self {
            pool,
            store,
            addr: "127.0.0.1:0".parse().unwrap(),
            server_handle: None,
            shutdown_tx: None,
            cancel_token: None,
            coordinator_config: None,
            temp_dir,
        };
        broker.boot().await;
        broker
    }

    /// Start a broker with a custom buffer config (for backpressure tests).
    pub async fn start_with_buffer_config(pool: PgPool, buffer: BufferConfig) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let mut broker = Self {
            pool,
            store,
            addr: "127.0.0.1:0".parse().unwrap(),
            server_handle: None,
            shutdown_tx: None,
            cancel_token: None,
            coordinator_config: None,
            temp_dir,
        };
        broker.boot_with_config(buffer).await;
        broker
    }

    /// Start a broker with custom coordinator config (for reader group timeout tests).
    pub async fn start_with_coordinator_config(
        pool: PgPool,
        coordinator_config: flux_broker::CoordinatorConfig,
    ) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let mut broker = Self {
            pool,
            store,
            addr: "127.0.0.1:0".parse().unwrap(),
            server_handle: None,
            shutdown_tx: None,
            cancel_token: None,
            coordinator_config: Some(coordinator_config),
            temp_dir,
        };
        broker.boot().await;
        broker
    }

    async fn boot(&mut self) {
        self.boot_with_config(BufferConfig::default()).await;
    }

    async fn boot_with_config(&mut self, buffer: BufferConfig) {
        let addr = super::ws_helpers::find_available_port().await;
        let config = BrokerConfig {
            bind_addr: addr,
            bucket: "test".to_string(),
            key_prefix: "data".to_string(),
            buffer,
            flush_interval: Duration::from_millis(50),
            require_auth: false,
            auth_timeout: Duration::from_secs(10),
            #[cfg(feature = "iceberg")]
            iceberg: None,
        };
        let coordinator_config = self.coordinator_config.clone().unwrap_or_default();
        let state = BrokerState::with_coordinator_config(
            self.pool.clone(),
            self.store.clone(),
            config,
            coordinator_config,
        )
        .await;
        self.cancel_token = Some(state.cancel_token());
        let handle = tokio::spawn(async move {
            let _ = flux_broker::run(state).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.addr = addr;
        self.server_handle = Some(handle);
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn faulty_store(&self) -> &FaultyObjectStore {
        &self.store
    }

    /// Start a broker with graceful shutdown support.
    pub async fn start_with_shutdown(pool: PgPool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let mut broker = Self {
            pool,
            store,
            addr: "127.0.0.1:0".parse().unwrap(),
            server_handle: None,
            shutdown_tx: None,
            cancel_token: None,
            coordinator_config: None,
            temp_dir,
        };
        broker.boot_with_shutdown().await;
        broker
    }

    async fn boot_with_shutdown(&mut self) {
        let addr = super::ws_helpers::find_available_port().await;
        let config = BrokerConfig {
            bind_addr: addr,
            bucket: "test".to_string(),
            key_prefix: "data".to_string(),
            buffer: BufferConfig::default(),
            flush_interval: Duration::from_millis(50),
            require_auth: false,
            auth_timeout: Duration::from_secs(10),
            #[cfg(feature = "iceberg")]
            iceberg: None,
        };
        let coordinator_config = self.coordinator_config.clone().unwrap_or_default();
        let state = BrokerState::with_coordinator_config(
            self.pool.clone(),
            self.store.clone(),
            config,
            coordinator_config,
        )
        .await;
        self.cancel_token = Some(state.cancel_token());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let shutdown = async {
                rx.await.ok();
            };
            let _ = flux_broker::run_with_shutdown(state, shutdown).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.addr = addr;
        self.server_handle = Some(handle);
        self.shutdown_tx = Some(tx);
    }

    /// Send a graceful shutdown signal to the broker.
    pub fn trigger_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Wait for the server task to complete after shutdown.
    pub async fn wait_for_shutdown(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
    }

    pub fn crash(&mut self) {
        if let Some(token) = self.cancel_token.take() {
            token.cancel();
        }
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
    }

    pub async fn restart(&mut self) {
        self.crash();
        self.store.reset();
        self.boot().await;
    }
}

/// Multiple brokers sharing the same DB + S3 filesystem for multi-broker tests.
/// Each broker has independent fault injection state.
pub struct MultiBrokerCluster {
    brokers: Vec<CrashableWsBroker>,
    stores: Vec<FaultyObjectStore>,
    broker_pools: Vec<PgPool>,
    db_url: String,
    _temp_dir: TempDir,
}

impl MultiBrokerCluster {
    pub async fn start(db_url: &str, num_brokers: usize) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let inner = Arc::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let mut brokers = Vec::with_capacity(num_brokers);
        let mut stores = Vec::with_capacity(num_brokers);
        let mut broker_pools = Vec::with_capacity(num_brokers);
        for _ in 0..num_brokers {
            let store = FaultyObjectStore::new_shared(inner.clone());
            let broker_pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(db_url)
                .await
                .expect("connect broker pool");
            let broker = CrashableWsBroker::start_shared(
                broker_pool.clone(),
                store.clone(),
                temp_dir.path().to_path_buf(),
            )
            .await;
            stores.push(store);
            brokers.push(broker);
            broker_pools.push(broker_pool);
        }
        Self {
            brokers,
            stores,
            broker_pools,
            db_url: db_url.to_string(),
            _temp_dir: temp_dir,
        }
    }

    pub fn broker(&self, idx: usize) -> &CrashableWsBroker {
        &self.brokers[idx]
    }

    pub fn broker_mut(&mut self, idx: usize) -> &mut CrashableWsBroker {
        &mut self.brokers[idx]
    }

    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.brokers.iter().map(|b| b.addr()).collect()
    }

    /// Per-broker fault injection store.
    pub fn broker_store(&self, idx: usize) -> &FaultyObjectStore {
        &self.stores[idx]
    }

    /// Apply an action to all broker stores (e.g. reset all faults).
    pub fn all_stores(&self) -> &[FaultyObjectStore] {
        &self.stores
    }

    pub async fn crash_broker(&mut self, idx: usize) {
        self.brokers[idx].crash();
    }

    pub async fn restart_broker(&mut self, idx: usize) {
        self.stores[idx].reset();
        let broker = CrashableWsBroker::start_shared(
            self.broker_pools[idx].clone(),
            self.stores[idx].clone(),
            self._temp_dir.path().to_path_buf(),
        )
        .await;
        self.brokers[idx] = broker;
    }

    /// Partition a single broker from DB by closing its pool.
    pub async fn partition_broker_db(&mut self, idx: usize) {
        self.broker_pools[idx].close().await;
    }

    /// Heal a broker's DB partition: create fresh pool, restart broker.
    pub async fn heal_broker_db(&mut self, idx: usize) {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.db_url)
            .await
            .expect("reconnect to DB");
        self.broker_pools[idx] = pool.clone();
        self.brokers[idx].crash();
        let broker = CrashableWsBroker::start_shared(
            pool,
            self.stores[idx].clone(),
            self._temp_dir.path().to_path_buf(),
        )
        .await;
        self.brokers[idx] = broker;
    }

    pub fn pool(&self) -> &PgPool {
        &self.broker_pools[0]
    }

    pub fn db_url(&self) -> &str {
        &self.db_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_broker::ObjectStore;

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_n() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.fail_on_put_n(3);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_ok(), "Put 1 should succeed");

        let result = store.put("key2", Bytes::from("value2")).await;
        assert!(result.is_ok(), "Put 2 should succeed");

        let result = store.put("key3", Bytes::from("value3")).await;
        assert!(result.is_err(), "Put 3 should fail");

        let result = store.put("key4", Bytes::from("value4")).await;
        assert!(result.is_err(), "Put 4 should also fail");

        store.reset();
        let result = store.put("key5", Bytes::from("value5")).await;
        assert!(result.is_ok(), "Put after reset should succeed");
    }

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_1() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.fail_on_put_n(1);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_err(), "Put 1 should fail immediately");
    }

    #[tokio::test]
    async fn test_faulty_object_store_transient_failure() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.fail_put_transiently(2);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_err(), "Put 1 should fail (transient)");

        let result = store.put("key2", Bytes::from("value2")).await;
        assert!(result.is_err(), "Put 2 should fail (transient)");

        let result = store.put("key3", Bytes::from("value3")).await;
        assert!(result.is_ok(), "Put 3 should succeed (recovered)");
    }

    #[tokio::test]
    async fn test_faulty_object_store_put_delay() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.set_put_delay_ms(100);

        let start = std::time::Instant::now();
        let result = store.put("key1", Bytes::from("value1")).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(
            elapsed >= Duration::from_millis(90),
            "Put should be delayed by ~100ms, got {:?}",
            elapsed
        );
    }
}
