//! Reader group coordinator benchmarks.

use criterion::{BenchmarkId, Criterion, black_box};
use flux_broker::{Coordinator, CoordinatorConfig};
use flux_common::ids::{Offset, TopicId};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Runtime;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

fn db_available() -> bool {
    std::env::var("DATABASE_URL").is_ok() || std::env::var("FLUX_BENCH_DB").is_ok()
}

/// Wrapper struct for benchmark database with automatic cleanup on drop.
#[allow(dead_code)]
pub struct BenchDb {
    pub pool: PgPool,
    db_name: String,
    base_url: String,
}

impl Drop for BenchDb {
    fn drop(&mut self) {
        drop(self.pool.close());

        let admin_url = format!("{}/postgres", self.base_url);
        let db_name = self.db_name.clone();

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                if let Ok(admin_pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin_url)
                    .await
                {
                    let _ = sqlx::query(&format!(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                        db_name
                    ))
                    .execute(&admin_pool)
                    .await;

                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
                        .execute(&admin_pool)
                        .await;
                }
            });
        })
        .join()
        .ok();
    }
}

async fn create_test_db() -> Option<BenchDb> {
    let base_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("FLUX_BENCH_DB"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());

    let admin_url = format!("{}/postgres", base_url);
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_url)
        .await
        .ok()?;

    let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let db_name = format!("flux_coord_bench_{}_{}", std::process::id(), counter);

    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(&admin_pool)
        .await
        .ok()?;

    sqlx::query(&format!("CREATE DATABASE {}", db_name))
        .execute(&admin_pool)
        .await
        .ok()?;

    let test_url = format!("{}/{}", base_url, db_name);
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&test_url)
        .await
        .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE topics (
            topic_id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_groups (
            group_id TEXT NOT NULL,
            topic_id INT NOT NULL,
            PRIMARY KEY (group_id, topic_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_members (
            group_id TEXT NOT NULL,
            topic_id INT NOT NULL,
            reader_id TEXT NOT NULL,
            broker_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (group_id, topic_id, reader_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE topic_offsets (
            topic_id INT PRIMARY KEY REFERENCES topics(topic_id) ON DELETE CASCADE,
            next_offset BIGINT NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION create_topic_offset()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO topic_offsets (topic_id, next_offset)
            VALUES (NEW.topic_id, 0);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TRIGGER trigger_create_topic_offset
        AFTER INSERT ON topics
        FOR EACH ROW
        EXECUTE FUNCTION create_topic_offset()
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_group_state (
            group_id VARCHAR(255) NOT NULL,
            topic_id INT NOT NULL,
            dispatch_cursor BIGINT NOT NULL DEFAULT 0,
            committed_watermark BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, topic_id),
            FOREIGN KEY (group_id, topic_id)
                REFERENCES reader_groups(group_id, topic_id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_inflight (
            group_id VARCHAR(255) NOT NULL,
            topic_id INT NOT NULL,
            start_offset BIGINT NOT NULL,
            end_offset BIGINT NOT NULL,
            reader_id VARCHAR(255) NOT NULL,
            lease_expires_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (group_id, topic_id, start_offset),
            FOREIGN KEY (group_id, topic_id)
                REFERENCES reader_groups(group_id, topic_id) ON DELETE CASCADE,
            CHECK (end_offset > start_offset)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        "CREATE INDEX idx_reader_inflight_lease
         ON reader_inflight (lease_expires_at)
         WHERE lease_expires_at IS NOT NULL",
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        "CREATE INDEX idx_reader_inflight_reader
         ON reader_inflight (group_id, topic_id, reader_id)",
    )
    .execute(&pool)
    .await
    .ok()?;

    // Create test topic
    sqlx::query("INSERT INTO topics (name) VALUES ('bench-topic')")
        .execute(&pool)
        .await
        .ok()?;

    Some(BenchDb {
        pool,
        db_name,
        base_url,
    })
}

/// Benchmark reader join_group operation.
pub fn bench_coordinator_join(c: &mut Criterion) {
    if !db_available() {
        eprintln!("Skipping coordinator benchmarks: DATABASE_URL not set");
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => {
            eprintln!("Failed to create test database");
            return;
        }
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let _coordinator = Coordinator::new(pool.clone(), config);

    let mut group = c.benchmark_group("coordinator_join");
    let counter = AtomicU32::new(0);

    group.bench_function("first_consumer", |b| {
        b.to_async(&rt).iter(|| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let group_id = format!("bench-group-{}", n);
            let reader_id = format!("reader-{}", n);
            let pool = pool.clone();
            let config = CoordinatorConfig {
                lease_duration: Duration::from_secs(30),
                session_timeout: Duration::from_secs(60),
                ..Default::default()
            };
            async move {
                let coord = Coordinator::new(pool, config);
                let result = coord.join_group(&group_id, TopicId(1), &reader_id).await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark heartbeat operation.
pub fn bench_coordinator_heartbeat(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let coordinator = Coordinator::new(pool.clone(), config);

    // Pre-join a reader
    rt.block_on(async {
        coordinator
            .join_group("heartbeat-bench", TopicId(1), "reader-1")
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("coordinator_heartbeat");

    group.bench_function("single_consumer", |b| {
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            async move {
                let config = CoordinatorConfig {
                    lease_duration: Duration::from_secs(30),
                    session_timeout: Duration::from_secs(60),
                    ..Default::default()
                };
                let coord = Coordinator::new(pool, config);
                let result = coord
                    .heartbeat("heartbeat-bench", TopicId(1), "reader-1")
                    .await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark commit range operation.
pub fn bench_coordinator_commit(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let coordinator = Coordinator::new(pool.clone(), config);

    // Pre-join a reader
    rt.block_on(async {
        coordinator
            .join_group("commit-bench", TopicId(1), "reader-1")
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("coordinator_commit");
    let offset_counter = AtomicU64::new(0);

    group.bench_function("single_topic", |b| {
        b.to_async(&rt).iter(|| {
            let off = offset_counter.fetch_add(100, Ordering::SeqCst);
            let pool = pool.clone();
            async move {
                let config = CoordinatorConfig {
                    lease_duration: Duration::from_secs(30),
                    session_timeout: Duration::from_secs(60),
                    ..Default::default()
                };
                let coord = Coordinator::new(pool, config);
                let result = coord
                    .commit_range(
                        "commit-bench",
                        TopicId(1),
                        "reader-1",
                        Offset(off),
                        Offset(off + 100),
                    )
                    .await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark join with multiple consumers.
pub fn bench_coordinator_reset(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let mut group = c.benchmark_group("coordinator_join_scaling");
    let iteration = AtomicU32::new(0);

    for consumer_count in &[2, 4, 8] {
        let count = *consumer_count;

        // Pre-join consumers for each iteration using unique group names
        group.bench_with_input(
            BenchmarkId::new("consumers", consumer_count),
            consumer_count,
            |b, _| {
                b.to_async(&rt).iter(|| {
                    let n = iteration.fetch_add(1, Ordering::SeqCst);
                    let group_id = format!("join-bench-{}-{}", count, n);
                    let pool = pool.clone();
                    async move {
                        let config = CoordinatorConfig {
                            lease_duration: Duration::from_secs(30),
                            session_timeout: Duration::from_secs(60),
                            ..Default::default()
                        };
                        let coord = Coordinator::new(pool.clone(), config.clone());

                        // Pre-join consumers except the last
                        for i in 0..(count - 1) {
                            let reader_id = format!("reader-{}", i);
                            coord
                                .join_group(&group_id, TopicId(1), &reader_id)
                                .await
                                .unwrap();
                        }

                        // Measure joining the Nth consumer
                        let reader_id = format!("reader-{}", count - 1);
                        let result = coord.join_group(&group_id, TopicId(1), &reader_id).await;
                        black_box(result)
                    }
                })
            },
        );
    }

    group.finish();
}
