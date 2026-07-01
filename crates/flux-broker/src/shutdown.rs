// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Graceful shutdown handling for the flux broker.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tracing::info;

/// Tracks active connections for graceful shutdown.
pub struct ConnectionTracker {
    active: AtomicUsize,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl ConnectionTracker {
    /// Create a new connection tracker.
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            active: AtomicUsize::new(0),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Increment the active connection count.
    pub fn increment(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the active connection count.
    pub fn decrement(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get the current connection count.
    pub fn count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// Signal shutdown to all waiters.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Check if shutdown has been signaled.
    pub fn is_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    /// Subscribe to shutdown notifications.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Wait for all connections to drain with a timeout.
    /// Returns true if all connections drained, false if timeout was reached.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        while start.elapsed() < timeout {
            let count = self.count();
            if count == 0 {
                info!("All connections drained");
                return true;
            }
            info!("Waiting for {} connections to drain...", count);
            tokio::time::sleep(poll_interval).await;
        }

        let remaining = self.count();
        if remaining > 0 {
            info!(
                "Drain timeout reached with {} connections remaining",
                remaining
            );
        }
        remaining == 0
    }
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// A guard that tracks connection lifetime.
/// Decrements the tracker count when dropped.
pub struct TrackedConnection {
    tracker: Arc<ConnectionTracker>,
}

impl TrackedConnection {
    /// Create a new tracked connection.
    pub fn new(tracker: Arc<ConnectionTracker>) -> Self {
        tracker.increment();
        Self { tracker }
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.tracker.is_shutdown()
    }
}

impl Drop for TrackedConnection {
    fn drop(&mut self) {
        self.tracker.decrement();
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating graceful shutdown...");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating graceful shutdown...");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_tracker_count() {
        let tracker = ConnectionTracker::new();
        assert_eq!(tracker.count(), 0);

        tracker.increment();
        assert_eq!(tracker.count(), 1);

        tracker.increment();
        assert_eq!(tracker.count(), 2);

        tracker.decrement();
        assert_eq!(tracker.count(), 1);

        tracker.decrement();
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn test_connection_tracker_shutdown() {
        let tracker = ConnectionTracker::new();
        assert!(!tracker.is_shutdown());

        tracker.signal_shutdown();
        assert!(tracker.is_shutdown());
    }

    #[test]
    fn test_tracked_connection_guard() {
        let tracker = Arc::new(ConnectionTracker::new());
        assert_eq!(tracker.count(), 0);

        {
            let _conn = TrackedConnection::new(tracker.clone());
            assert_eq!(tracker.count(), 1);
        }

        assert_eq!(tracker.count(), 0);
    }
}
