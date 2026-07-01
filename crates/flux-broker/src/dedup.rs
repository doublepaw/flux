// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Writer deduplication cache.
//!
//! LRU cache for tracking writer sequence numbers. Provides fast
//! deduplication checks without hitting the database for every request.

use std::collections::HashMap;

use flux_common::ids::{AppendSeq, WriterId};
use flux_common::types::BatchAck;
use sqlx::PgPool;
use tokio::sync::RwLock;

/// Cached writer state.
#[derive(Debug, Clone)]
pub struct WriterState {
    /// Last acknowledged sequence number.
    pub last_seq: AppendSeq,
    /// Acks for the last sequence (for duplicate requests).
    pub last_acks: Vec<BatchAck>,
}

#[derive(Debug, Clone)]
enum CachedWriterState {
    Known(WriterState),
    Missing,
}

/// Result of dedup check.
#[derive(Debug)]
pub enum DedupResult {
    /// New request, should be processed.
    Accept,
    /// Duplicate request, return cached append_acks.
    Duplicate(Vec<BatchAck>),
    /// Sequence too old (already processed higher append_seq).
    Stale,
}

/// Configuration for dedup cache.
#[derive(Debug, Clone)]
pub struct DedupCacheConfig {
    /// Maximum number of writers to cache.
    pub max_entries: usize,
}

impl Default for DedupCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
        }
    }
}

/// LRU dedup cache backed by database.
pub struct DedupCache {
    /// In-memory cache.
    cache: RwLock<LruCache>,
    /// Database pool for cache misses.
    pool: PgPool,
    /// Configuration.
    config: DedupCacheConfig,
}

/// Simple LRU cache implementation.
struct LruCache {
    map: HashMap<WriterId, (CachedWriterState, u64)>,
    order: u64,
    max_entries: usize,
}

impl LruCache {
    fn new(max_entries: usize) -> Self {
        Self {
            map: HashMap::with_capacity(max_entries),
            order: 0,
            max_entries,
        }
    }

    fn get(&self, key: &WriterId) -> Option<&CachedWriterState> {
        self.map.get(key).map(|(state, _)| state)
    }

    fn insert_known(&mut self, key: WriterId, state: WriterState) {
        self.order += 1;

        // Evict oldest if at capacity
        if self.map.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.map
            .insert(key, (CachedWriterState::Known(state), self.order));
    }

    fn insert_missing(&mut self, key: WriterId) {
        self.order += 1;

        // Evict oldest if at capacity
        if self.map.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.map
            .insert(key, (CachedWriterState::Missing, self.order));
    }

    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self
            .map
            .iter()
            .min_by_key(|(_, (_, order))| *order)
            .map(|(k, _)| *k)
        {
            self.map.remove(&oldest_key);
        }
    }
}

impl DedupCache {
    /// Create a new dedup cache.
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, DedupCacheConfig::default())
    }

    /// Create a new dedup cache with custom config.
    pub fn with_config(pool: PgPool, config: DedupCacheConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(config.max_entries)),
            pool,
            config,
        }
    }

    /// Check if a request is a duplicate.
    pub async fn check(
        &self,
        writer_id: WriterId,
        append_seq: AppendSeq,
    ) -> Result<DedupResult, sqlx::Error> {
        // Fast path: check cache
        {
            let cache = self.cache.read().await;
            if let Some(cached_state) = cache.get(&writer_id) {
                match cached_state {
                    CachedWriterState::Known(state) => {
                        if append_seq.0 == state.last_seq.0 {
                            return Ok(DedupResult::Duplicate(state.last_acks.clone()));
                        } else if append_seq.0 < state.last_seq.0 {
                            return Ok(DedupResult::Stale);
                        } else {
                            return Ok(DedupResult::Accept);
                        }
                    }
                    CachedWriterState::Missing => {
                        return Ok(DedupResult::Accept);
                    }
                }
            }
        }

        // Slow path: check database
        let row: Option<(i64, serde_json::Value)> =
            sqlx::query_as("SELECT last_seq, last_acks FROM writer_state WHERE writer_id = $1")
                .bind(writer_id.0)
                .fetch_optional(&self.pool)
                .await?;

        match row {
            Some((last_seq, last_acks_json)) => {
                let state = WriterState {
                    last_seq: AppendSeq(last_seq as u64),
                    last_acks: serde_json::from_value(last_acks_json).unwrap_or_default(),
                };

                {
                    let mut cache = self.cache.write().await;
                    cache.insert_known(writer_id, state.clone());
                }

                if append_seq.0 == last_seq as u64 {
                    Ok(DedupResult::Duplicate(state.last_acks))
                } else if append_seq.0 < last_seq as u64 {
                    Ok(DedupResult::Stale)
                } else {
                    Ok(DedupResult::Accept)
                }
            }
            None => {
                // Memoize missing writers to avoid repeated DB checks before first commit.
                let mut cache = self.cache.write().await;
                cache.insert_missing(writer_id);
                Ok(DedupResult::Accept)
            }
        }
    }

    /// Update cache after successful commit.
    pub async fn update(
        &self,
        writer_id: WriterId,
        append_seq: AppendSeq,
        append_acks: Vec<BatchAck>,
    ) {
        let mut cache = self.cache.write().await;
        Self::update_locked(&mut cache, writer_id, append_seq, append_acks);
    }

    fn update_locked(
        cache: &mut LruCache,
        writer_id: WriterId,
        append_seq: AppendSeq,
        append_acks: Vec<BatchAck>,
    ) {
        if let Some((CachedWriterState::Known(existing), _)) = cache.map.get(&writer_id)
            && append_seq.0 <= existing.last_seq.0
        {
            return;
        }
        cache.insert_known(
            writer_id,
            WriterState {
                last_seq: append_seq,
                last_acks: append_acks,
            },
        );
    }

    /// Get cache stats.
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        CacheStats {
            entries: cache.map.len(),
            max_entries: self.config.max_entries,
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_common::ids::{Offset, SchemaId, TopicId};

    fn make_ack() -> BatchAck {
        BatchAck {
            topic_id: TopicId(1),
            schema_id: SchemaId(100),
            start_offset: Offset(0),
            end_offset: Offset(10),
        }
    }

    #[test]
    fn test_lru_cache_insert_get() {
        let mut cache = LruCache::new(10);
        let writer = WriterId(uuid::Uuid::new_v4());
        let state = WriterState {
            last_seq: AppendSeq(42),
            last_acks: vec![make_ack()],
        };

        cache.insert_known(writer, state.clone());

        match cache.get(&writer) {
            Some(CachedWriterState::Known(retrieved)) => assert_eq!(retrieved.last_seq.0, 42),
            _ => panic!("expected known writer state"),
        }
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LruCache::new(2);
        let p1 = WriterId(uuid::Uuid::new_v4());
        let p2 = WriterId(uuid::Uuid::new_v4());
        let p3 = WriterId(uuid::Uuid::new_v4());

        cache.insert_known(
            p1,
            WriterState {
                last_seq: AppendSeq(1),
                last_acks: vec![],
            },
        );
        cache.insert_known(
            p2,
            WriterState {
                last_seq: AppendSeq(2),
                last_acks: vec![],
            },
        );

        // Insert p3 - should evict p1 (oldest insertion)
        cache.insert_known(
            p3,
            WriterState {
                last_seq: AppendSeq(3),
                last_acks: vec![],
            },
        );

        assert!(cache.get(&p1).is_none()); // Evicted
        assert!(cache.get(&p2).is_some());
        assert!(cache.get(&p3).is_some());
    }

    #[test]
    fn test_lru_cache_insert_order_eviction() {
        let mut cache = LruCache::new(2);
        let p1 = WriterId(uuid::Uuid::new_v4());
        let p2 = WriterId(uuid::Uuid::new_v4());

        cache.insert_known(
            p1,
            WriterState {
                last_seq: AppendSeq(1),
                last_acks: vec![],
            },
        );
        cache.insert_known(
            p2,
            WriterState {
                last_seq: AppendSeq(2),
                last_acks: vec![],
            },
        );

        // p1 is oldest insertion and should be evicted next
        let p3 = WriterId(uuid::Uuid::new_v4());
        cache.insert_known(
            p3,
            WriterState {
                last_seq: AppendSeq(3),
                last_acks: vec![],
            },
        );

        assert!(cache.get(&p1).is_none());
        assert!(cache.get(&p2).is_some());
        assert!(cache.get(&p3).is_some());
    }

    #[test]
    fn test_lru_cache_missing_entry() {
        let mut cache = LruCache::new(2);
        let p1 = WriterId(uuid::Uuid::new_v4());
        cache.insert_missing(p1);
        assert!(matches!(cache.get(&p1), Some(CachedWriterState::Missing)));
    }
}
