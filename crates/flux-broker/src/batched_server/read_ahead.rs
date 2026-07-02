// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Server-side read-ahead for direct reads.
//!
//! After serving a read at (topic, offset), the broker prefetches the next
//! contiguous window while the consumer is busy processing the current
//! response. The next request hits the cache and skips the storage
//! round-trip entirely, pipelining consumer processing with broker I/O
//! without any wire-format change.
//!
//! Generic over the window payload: decoded records for classic reads,
//! compressed segments for raw (zero-copy) reads.
//!
//! Entries are keyed by (topic, offset), consumed on hit (serve-once, so a
//! stale high watermark is bounded to a single response), and the cache is
//! bytes-capped. `max_bytes == 0` disables read-ahead.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

pub(crate) struct ReadAheadCache<T> {
    max_bytes: usize,
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    entries: HashMap<(u32, u64), (T, usize)>,
    inflight: HashSet<(u32, u64)>,
    total_bytes: usize,
}

impl<T> ReadAheadCache<T> {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                inflight: HashSet::new(),
                total_bytes: 0,
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// Consume a prefetched window, if present.
    pub fn take(&self, key: (u32, u64)) -> Option<T> {
        if !self.enabled() {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.entries.remove(&key);
        if let Some((value, bytes)) = entry {
            inner.total_bytes -= bytes;
            Some(value)
        } else {
            None
        }
    }

    /// Reserve a prefetch slot. Returns false when disabled, already
    /// cached/in-flight, or the cache is full.
    pub fn try_begin(&self, key: (u32, u64)) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.total_bytes >= self.max_bytes
            || inner.entries.contains_key(&key)
            || inner.inflight.contains(&key)
        {
            return false;
        }
        inner.inflight.insert(key);
        true
    }

    /// Complete a reserved prefetch, inserting the (value, bytes) if it fits.
    pub fn complete(&self, key: (u32, u64), result: Option<(T, usize)>) {
        let mut inner = self.inner.lock().unwrap();
        inner.inflight.remove(&key);
        if let Some((value, bytes)) = result
            && bytes > 0
            && inner.total_bytes + bytes <= self.max_bytes
        {
            inner.total_bytes += bytes;
            inner.entries.insert(key, (value, bytes));
        }
    }
}
