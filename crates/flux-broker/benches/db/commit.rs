//! Batch commit benchmarks.
//!
//! These benchmarks require a running Postgres instance.
//! Set DATABASE_URL environment variable to run.

use criterion::{BenchmarkId, Criterion, black_box};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::runtime::Runtime;

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Check if database is available.
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
        // Close all connections to allow dropping (returns future but we can't await in Drop)
        drop(self.pool.close());

        // Use a new runtime for cleanup since we can't use async in Drop
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
                    // Force disconnect other sessions
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

/// Create a test database for benchmarking.
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
    let db_name = format!("flux_bench_{}_{}", std::process::id(), counter);

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

    // Run minimal schema setup
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
        CREATE TABLE topic_offsets (
            topic_id INT NOT NULL PRIMARY KEY,
            next_offset BIGINT NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE topic_batches (
            batch_id BIGSERIAL PRIMARY KEY,
            topic_id INT NOT NULL,
            schema_id INT NOT NULL,
            start_offset BIGINT NOT NULL,
            end_offset BIGINT NOT NULL,
            record_count INT NOT NULL,
            s3_key VARCHAR(512) NOT NULL,
            ingest_time TIMESTAMPTZ NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query("CREATE INDEX idx_batches ON topic_batches (topic_id, end_offset)")
        .execute(&pool)
        .await
        .ok()?;

    // Create test topics and their offsets
    for t in 1..=32 {
        let topic_id: i32 =
            sqlx::query_scalar("INSERT INTO topics (name) VALUES ($1) RETURNING topic_id")
                .bind(format!("bench-{}", t))
                .fetch_one(&pool)
                .await
                .ok()?;

        sqlx::query("INSERT INTO topic_offsets (topic_id, next_offset) VALUES ($1, 0)")
            .bind(topic_id)
            .execute(&pool)
            .await
            .ok()?;
    }

    Some(BenchDb {
        pool,
        db_name,
        base_url,
    })
}

/// Benchmark offset update + batch insert transaction.
pub fn bench_commit_batch(c: &mut Criterion) {
    if !db_available() {
        eprintln!("Skipping DB benchmarks: DATABASE_URL not set");
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => {
            eprintln!("Failed to create test database, skipping DB benchmarks");
            return;
        }
    };
    let pool = bench_db.pool.clone();

    let mut group = c.benchmark_group("db_commit_batch");

    // Single topic commit
    for record_count in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("records", record_count),
            record_count,
            |b, &count| {
                b.to_async(&rt).iter_batched(
                    || pool.clone(),
                    |pool| async move {
                        let mut tx = pool.begin().await.unwrap();

                        // Lock and update offset
                        let current: i64 = sqlx::query_scalar(
                            "SELECT next_offset FROM topic_offsets WHERE topic_id = 1 FOR UPDATE",
                        )
                        .fetch_one(&mut *tx)
                        .await
                        .unwrap();

                        let new_offset = current + count as i64;

                        sqlx::query("UPDATE topic_offsets SET next_offset = $1 WHERE topic_id = 1")
                            .bind(new_offset)
                            .execute(&mut *tx)
                            .await
                            .unwrap();

                        // Insert batch record
                        sqlx::query(
                            r#"
                            INSERT INTO topic_batches (topic_id, schema_id, start_offset, end_offset, record_count, s3_key, ingest_time)
                            VALUES (1, 100, $1, $2, $3, 'bench/test.fl', NOW())
                            "#,
                        )
                        .bind(current)
                        .bind(new_offset)
                        .bind(count)
                        .execute(&mut *tx)
                        .await
                        .unwrap();

                        tx.commit().await.unwrap();
                        black_box(new_offset)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark multi-topic commit (multiple offsets in one transaction).
pub fn bench_multi_topic_commit(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let mut group = c.benchmark_group("db_multi_topic_commit");

    for topic_count in &[1, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("topics", topic_count),
            topic_count,
            |b, &count| {
                b.to_async(&rt).iter_batched(
                    || pool.clone(),
                    |pool| async move {
                        let mut tx = pool.begin().await.unwrap();

                        for t in 1..=count {
                            let current: i64 = sqlx::query_scalar(
                                "SELECT next_offset FROM topic_offsets WHERE topic_id = $1 FOR UPDATE",
                            )
                            .bind(t)
                            .fetch_one(&mut *tx)
                            .await
                            .unwrap();

                            let new_offset = current + 100;

                            sqlx::query("UPDATE topic_offsets SET next_offset = $1 WHERE topic_id = $2")
                                .bind(new_offset)
                                .bind(t)
                                .execute(&mut *tx)
                                .await
                                .unwrap();

                            sqlx::query(
                                r#"
                                INSERT INTO topic_batches (topic_id, schema_id, start_offset, end_offset, record_count, s3_key, ingest_time)
                                VALUES ($1, 100, $2, $3, 100, 'bench/test.fl', NOW())
                                "#,
                            )
                            .bind(t)
                            .bind(current)
                            .bind(new_offset)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                        }

                        tx.commit().await.unwrap();
                        black_box(())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark batch lookup for reader read.
pub fn bench_batch_lookup(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    // Pre-populate with batches
    rt.block_on(async {
        for i in 0..1000 {
            let start = i * 100;
            let end = start + 100;
            sqlx::query(
                r#"
                INSERT INTO topic_batches (topic_id, schema_id, start_offset, end_offset, record_count, s3_key, ingest_time)
                VALUES (1, 100, $1, $2, 100, 'bench/test.fl', NOW())
                "#,
            )
            .bind(start as i64)
            .bind(end as i64)
            .execute(&pool)
            .await
            .unwrap();
        }
    });

    let mut group = c.benchmark_group("db_batch_lookup");

    group.bench_function("single_offset", |b| {
        b.to_async(&rt).iter(|| async {
            let offset = 50_000i64;
            let result: Vec<(i64, i64, String)> = sqlx::query_as(
                r#"
                SELECT start_offset, end_offset, s3_key
                FROM topic_batches
                WHERE topic_id = 1 AND end_offset > $1
                ORDER BY start_offset
                LIMIT 10
                "#,
            )
            .bind(offset)
            .fetch_all(&pool)
            .await
            .unwrap();
            black_box(result)
        })
    });

    group.bench_function("range_scan", |b| {
        b.to_async(&rt).iter(|| async {
            let start = 25_000i64;
            let end = 75_000i64;
            let result: Vec<(i64, i64, String)> = sqlx::query_as(
                r#"
                SELECT start_offset, end_offset, s3_key
                FROM topic_batches
                WHERE topic_id = 1 AND end_offset > $1 AND start_offset < $2
                ORDER BY start_offset
                "#,
            )
            .bind(start)
            .bind(end)
            .fetch_all(&pool)
            .await
            .unwrap();
            black_box(result)
        })
    });

    group.finish();
}
