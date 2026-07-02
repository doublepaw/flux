// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Server-side read-ahead for direct reads.
//!
//! After serving a read at (topic, offset), the broker prefetches the next
//! contiguous window (storage fetch + decompress) while the consumer is
//! busy decoding and processing the current response. The next request hits
//! the cache and skips the storage round-trip entirely, pipelining consumer
//! processing with broker I/O without any wire-format change.
//!
//! Entries are keyed by (topic, offset), consumed on hit (serve-once, so a
//! stale high watermark is bounded to a single response), and the cache is
//! bytes-capped. `max_bytes == 0` disables read-ahead.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use flux_common::ids::Offset;
use flux_wire::reader;

pub(crate) struct Prefetched {
    pub results: Vec<reader::TopicResult>,
    pub high_watermark: Offset,
    pub bytes: usize,
}

pub(crate) struct ReadAheadCache {
    max_bytes: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<(u32, u64), Prefetched>,
    inflight: HashSet<(u32, u64)>,
    total_bytes: usize,
}

impl ReadAheadCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// Consume a prefetched window, if present.
    pub fn take(&self, key: (u32, u64)) -> Option<Prefetched> {
        if !self.enabled() {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.entries.remove(&key);
        if let Some(ref p) = entry {
            inner.total_bytes -= p.bytes;
        }
        entry
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

    /// Complete a reserved prefetch, inserting the result if it fits.
    pub fn complete(&self, key: (u32, u64), result: Option<Prefetched>) {
        let mut inner = self.inner.lock().unwrap();
        inner.inflight.remove(&key);
        if let Some(p) = result
            && p.bytes > 0
            && inner.total_bytes + p.bytes <= self.max_bytes
        {
            inner.total_bytes += p.bytes;
            inner.entries.insert(key, p);
        }
    }
}
