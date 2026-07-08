// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Exactly-once and committer-election tests for Iceberg ingestion.
//!
//! The P0 correctness properties (DESIGN_ICEBERG.md):
//! - A crash between the Iceberg commit and Postgres `mark_completed`
//!   re-claims batches; the snapshot watermark makes the retry a no-op —
//!   table row count equals produced count, never more.
//! - One committer per table: a second writer cannot commit while the
//!   lease is held; takeover after expiry increments the epoch.
//!
//! Needs Postgres (DATABASE_URL, no database name — a fresh DB is
//! created per test) and a local-fs warehouse.

use std::collections::HashMap;

use apache_avro::{Schema as AvroSchema, to_avro_datum, types::Value as AvroValue};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use sqlx::PgPool;

use flux_common::ids::{SchemaId, TopicId};
use flux_common::types::Record;
use flux_iceberg::iceberg_buffer::CommittedSegment;
use flux_iceberg::iceberg_writer::TableWriter;
use flux_iceberg::{IcebergConfig, IcebergError, committer};

const SCHEMA_JSON: &str = r#"{
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "body", "type": "string"}
    ]
}"#;

struct TestEnv {
    pool: PgPool,
    config: IcebergConfig,
    topic_id: TopicId,
    schema_id: SchemaId,
    _warehouse: tempfile::TempDir,
}

async fn setup(test_name: &str) -> TestEnv {
    let base = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());
    let db_name = format!(
        "flux_iceberg_{}_{}",
        test_name,
        uuid::Uuid::new_v4().simple()
    );
    let admin = PgPool::connect(&format!("{base}/postgres")).await.unwrap();
    sqlx::query(&format!(r#"CREATE DATABASE "{db_name}""#))
        .execute(&admin)
        .await
        .unwrap();
    let pool = PgPool::connect(&format!("{base}/{db_name}")).await.unwrap();
    for migration in [
        include_str!("../../../migrations/001_init.sql"),
        include_str!("../../../migrations/002_iceberg.sql"),
        include_str!("../../../migrations/003_iceberg_committer.sql"),
    ] {
        sqlx::raw_sql(migration).execute(&pool).await.unwrap();
    }

    let topic_id: i32 =
        sqlx::query_scalar("INSERT INTO topics (name) VALUES ($1) RETURNING topic_id")
            .bind(format!("t-{test_name}"))
            .fetch_one(&pool)
            .await
            .unwrap();
    let schema_json: serde_json::Value = serde_json::from_str(SCHEMA_JSON).unwrap();
    let schema_id: i32 = sqlx::query_scalar(
        "INSERT INTO schemas (schema_hash, schema_json) VALUES ($1, $2) RETURNING schema_id",
    )
    .bind(uuid::Uuid::new_v4().as_bytes().to_vec())
    .bind(&schema_json)
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO topic_schemas (topic_id, schema_id) VALUES ($1, $2)")
        .bind(topic_id)
        .bind(schema_id)
        .execute(&pool)
        .await
        .unwrap();

    let warehouse = tempfile::tempdir().unwrap();
    let config = IcebergConfig {
        warehouse: warehouse.path().to_string_lossy().to_string(),
        catalog_uri: format!("{base}/{db_name}"),
        catalog_type: "sql".to_string(),
        ..IcebergConfig::default()
    };

    TestEnv {
        pool,
        config,
        topic_id: TopicId(topic_id as u32),
        schema_id: SchemaId(schema_id as u32),
        _warehouse: warehouse,
    }
}

fn make_segment(env: &TestEnv, batch_id: i64, offsets: std::ops::Range<u64>) -> CommittedSegment {
    let avro_schema = AvroSchema::parse_str(SCHEMA_JSON).unwrap();
    let records = offsets
        .clone()
        .map(|i| {
            let rec = AvroValue::Record(vec![
                ("id".into(), AvroValue::Long(i as i64)),
                ("body".into(), AvroValue::String(format!("r{i}"))),
            ]);
            Record::new(Bytes::from(to_avro_datum(&avro_schema, rec).unwrap()))
        })
        .collect();
    CommittedSegment {
        topic_id: env.topic_id,
        schema_id: env.schema_id,
        records,
        start_offset: offsets.start,
        end_offset: offsets.end,
        batch_id,
        ingest_time: Utc::now(),
    }
}

async fn table_row_count(env: &TestEnv, table_name: &str) -> usize {
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
    use iceberg_catalog_sql::SqlCatalogBuilder;
    let catalog = SqlCatalogBuilder::default()
        .uri(env.config.catalog_uri.clone())
        .warehouse_location(env.config.warehouse.clone())
        .with_storage_factory(std::sync::Arc::new(
            iceberg_storage_opendal::OpenDalStorageFactory::Fs,
        ))
        .load("flux", HashMap::new())
        .await
        .unwrap();
    let table = catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new(env.config.namespace.clone()),
            table_name.to_string(),
        ))
        .await
        .unwrap();
    let batches: Vec<_> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Crash between Iceberg commit and mark_completed: the re-claimed
/// batches must not produce duplicate rows.
#[tokio::test]
async fn test_reingestion_after_crash_is_deduped_by_watermark() {
    let env = setup("dedupe").await;
    let writer = TableWriter::new(env.config.clone(), env.pool.clone())
        .await
        .unwrap();

    let seg = make_segment(&env, 1, 0..100);
    writer
        .write_segments(env.topic_id, vec![seg])
        .await
        .unwrap();
    assert_eq!(table_row_count(&env, "t-dedupe").await, 100);

    // Simulate the crash: wipe the Postgres claims (as if mark_completed
    // never ran) and re-ingest the same batch, as catch-up would.
    sqlx::query("DELETE FROM iceberg_claims")
        .execute(&env.pool)
        .await
        .unwrap();
    let seg_again = make_segment(&env, 1, 0..100);
    writer
        .write_segments(env.topic_id, vec![seg_again])
        .await
        .unwrap();

    assert_eq!(
        table_row_count(&env, "t-dedupe").await,
        100,
        "re-ingesting a batch at or below the snapshot watermark must be a no-op"
    );

    // New data past the watermark still lands.
    let seg2 = make_segment(&env, 2, 100..150);
    writer
        .write_segments(env.topic_id, vec![seg2])
        .await
        .unwrap();
    assert_eq!(table_row_count(&env, "t-dedupe").await, 150);
}

/// A mixed retry (already-ingested batch + new batch in one write) keeps
/// exactly-once: only the new rows land.
#[tokio::test]
async fn test_partial_retry_writes_only_new_batches() {
    let env = setup("partial").await;
    let writer = TableWriter::new(env.config.clone(), env.pool.clone())
        .await
        .unwrap();

    writer
        .write_segments(env.topic_id, vec![make_segment(&env, 1, 0..40)])
        .await
        .unwrap();
    writer
        .write_segments(
            env.topic_id,
            vec![make_segment(&env, 1, 0..40), make_segment(&env, 2, 40..70)],
        )
        .await
        .unwrap();
    assert_eq!(table_row_count(&env, "t-partial").await, 70);
}

/// Two writers, one table: the second cannot commit while the first
/// holds the lease; after expiry it takes over with a higher epoch.
#[tokio::test]
async fn test_single_committer_lease_excludes_and_epoch_advances() {
    let env = setup("lease").await;
    let writer_a = TableWriter::new(env.config.clone(), env.pool.clone())
        .await
        .unwrap();
    let writer_b = TableWriter::new(env.config.clone(), env.pool.clone())
        .await
        .unwrap();

    writer_a
        .write_segments(env.topic_id, vec![make_segment(&env, 1, 0..10)])
        .await
        .unwrap();

    // B is excluded while A's lease is live.
    let err = writer_b
        .write_segments(env.topic_id, vec![make_segment(&env, 2, 10..20)])
        .await
        .unwrap_err();
    assert!(
        matches!(err, IcebergError::NotCommitter(_)),
        "expected NotCommitter, got: {err}"
    );
    assert_eq!(table_row_count(&env, "t-lease").await, 10);

    // Expire A's lease; B takes over with a higher epoch and commits.
    sqlx::query("UPDATE iceberg_committers SET lease_expiry = NOW() - INTERVAL '1 second'")
        .execute(&env.pool)
        .await
        .unwrap();
    writer_b
        .write_segments(env.topic_id, vec![make_segment(&env, 2, 10..20)])
        .await
        .unwrap();
    assert_eq!(table_row_count(&env, "t-lease").await, 20);

    let epoch: i64 = sqlx::query_scalar("SELECT epoch FROM iceberg_committers LIMIT 1")
        .fetch_one(&env.pool)
        .await
        .unwrap();
    assert_eq!(epoch, 2, "takeover must increment the epoch");
}

/// Direct lease-primitive semantics: renewal keeps the epoch, contention
/// returns None, takeover after expiry increments.
#[tokio::test]
async fn test_lease_primitive_semantics() {
    let env = setup("leaseprim").await;
    let t = "ns.table";

    assert_eq!(
        committer::acquire(&env.pool, t, "a", 60).await.unwrap(),
        Some(1)
    );
    // Renewal by the same holder keeps the epoch.
    assert_eq!(
        committer::acquire(&env.pool, t, "a", 60).await.unwrap(),
        Some(1)
    );
    // A live lease excludes other holders.
    assert_eq!(
        committer::acquire(&env.pool, t, "b", 60).await.unwrap(),
        None
    );
    // Release lets the next holder in, with a bumped epoch.
    committer::release(&env.pool, t, "a").await.unwrap();
    assert_eq!(
        committer::acquire(&env.pool, t, "b", 60).await.unwrap(),
        Some(2)
    );
}
