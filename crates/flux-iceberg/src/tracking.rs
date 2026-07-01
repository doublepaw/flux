// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Iceberg ingestion tracking via the `iceberg_claims` table.
//!
//! Uses a claim-then-process pattern for safe multi-broker catch-up:
//! 1. `claim_batches` — INSERT pending claims (anti-join + SKIP LOCKED)
//! 2. Process (S3 read → Iceberg write)
//! 3. `mark_completed` — UPDATE status to 'completed'
//! 4. `expire_stale_claims` — periodic reaper for crashed workers

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::Result;

/// Row type returned from the unclaimed-batch query.
type UnclaimedRow = (
    i64,
    i32,
    i32,
    i64,
    i64,
    i32,
    String,
    i64,
    i64,
    DateTime<Utc>,
    i64,
);

/// Batch info returned from the claim query.
#[derive(Debug, Clone)]
pub struct UnclaimedBatch {
    pub batch_id: i64,
    pub topic_id: i32,
    pub schema_id: i32,
    pub start_offset: i64,
    pub end_offset: i64,
    pub record_count: i32,
    pub s3_key: String,
    pub byte_offset: i64,
    pub byte_length: i64,
    pub ingest_time: DateTime<Utc>,
    pub crc32: i64,
}

/// Find and claim un-ingested batches for a set of topics.
///
/// Atomically: SELECT un-ingested batches → INSERT pending claims.
/// Uses `FOR UPDATE SKIP LOCKED` on `topic_batches` so concurrent
/// catch-up workers grab different batches.
///
/// Returns the claimed batches (now locked by this worker).
pub async fn claim_batches(
    pool: &PgPool,
    topic_ids: &[i32],
    limit: i64,
) -> Result<Vec<UnclaimedBatch>> {
    let mut tx = pool.begin().await?;

    // Step 1: Find unclaimed batches
    let unclaimed: Vec<UnclaimedRow> = sqlx::query_as(
        r#"
            SELECT tb.batch_id, tb.topic_id, tb.schema_id,
                   tb.start_offset, tb.end_offset, tb.record_count,
                   tb.s3_key, tb.byte_offset, tb.byte_length,
                   tb.ingest_time, tb.crc32
            FROM topic_batches tb
            WHERE NOT EXISTS (
                SELECT 1 FROM iceberg_claims ic
                WHERE ic.batch_id = tb.batch_id
                  AND ic.ingest_time = tb.ingest_time
            )
            AND tb.topic_id = ANY($1)
            ORDER BY tb.start_offset
            LIMIT $2
            FOR UPDATE OF tb SKIP LOCKED
            "#,
    )
    .bind(topic_ids)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;

    if unclaimed.is_empty() {
        tx.commit().await?;
        return Ok(Vec::new());
    }

    // Step 2: Insert claims
    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO iceberg_claims (batch_id, ingest_time, status, claimed_at) ",
    );
    qb.push_values(&unclaimed, |mut b, row| {
        b.push_bind(row.0) // batch_id
            .push_bind(row.9) // ingest_time
            .push_bind("pending")
            .push("NOW()");
    });
    qb.push(" ON CONFLICT (batch_id, ingest_time) DO NOTHING");
    qb.build().execute(&mut *tx).await?;

    tx.commit().await?;

    Ok(unclaimed
        .into_iter()
        .map(|r| UnclaimedBatch {
            batch_id: r.0,
            topic_id: r.1,
            schema_id: r.2,
            start_offset: r.3,
            end_offset: r.4,
            record_count: r.5,
            s3_key: r.6,
            byte_offset: r.7,
            byte_length: r.8,
            ingest_time: r.9,
            crc32: r.10,
        })
        .collect())
}

/// Mark batches as completed after successful Iceberg commit.
///
/// Uses UPSERT so it works for both:
/// - Hot path: inserts new row with 'completed' status
/// - Catch-up path: updates existing 'pending' claim to 'completed'
pub async fn mark_completed(pool: &PgPool, batch_ids: &[(i64, DateTime<Utc>)]) -> Result<()> {
    if batch_ids.is_empty() {
        return Ok(());
    }

    for (batch_id, ingest_time) in batch_ids {
        sqlx::query(
            "INSERT INTO iceberg_claims (batch_id, ingest_time, status, completed_at) \
             VALUES ($1, $2, 'completed', NOW()) \
             ON CONFLICT (batch_id, ingest_time) \
             DO UPDATE SET status = 'completed', completed_at = NOW()",
        )
        .bind(batch_id)
        .bind(ingest_time)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Expire stale pending claims older than `expiry_secs`.
///
/// Deletes them so another catch-up worker can re-claim.
pub async fn expire_stale_claims(pool: &PgPool, expiry_secs: i64) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM iceberg_claims \
         WHERE status = 'pending' \
           AND claimed_at < NOW() - make_interval(secs => $1)",
    )
    .bind(expiry_secs as f64)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
