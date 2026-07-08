// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Single-committer-per-table election (Postgres lease + epoch fencing).
//!
//! Exactly one holder commits to a given Iceberg table at a time. The
//! lease renews while held and expires like reader-group leases; every
//! takeover increments the epoch. Committers stamp their epoch into each
//! snapshot (`flux.committer-epoch`) and abort if the table's current
//! snapshot carries a newer epoch — a paused or partitioned ex-committer
//! is thereby harmless even after its lease row changes hands.

use sqlx::PgPool;

use crate::error::Result;

/// Try to acquire or renew the committer lease for `table_ident`.
///
/// Returns `Some(epoch)` when this holder owns the lease (renewal keeps
/// the epoch; takeover of an expired lease increments it), `None` when
/// another holder's unexpired lease exists.
pub async fn acquire(
    pool: &PgPool,
    table_ident: &str,
    holder: &str,
    ttl_secs: i64,
) -> Result<Option<i64>> {
    let epoch: Option<i64> = sqlx::query_scalar(
        r#"
        INSERT INTO iceberg_committers (table_ident, holder, epoch, lease_expiry)
        VALUES ($1, $2, 1, NOW() + make_interval(secs => $3))
        ON CONFLICT (table_ident) DO UPDATE SET
            holder = EXCLUDED.holder,
            epoch = CASE
                WHEN iceberg_committers.holder = EXCLUDED.holder
                     AND iceberg_committers.lease_expiry > NOW()
                THEN iceberg_committers.epoch
                ELSE iceberg_committers.epoch + 1
            END,
            lease_expiry = EXCLUDED.lease_expiry
        WHERE iceberg_committers.holder = EXCLUDED.holder
           OR iceberg_committers.lease_expiry <= NOW()
        RETURNING epoch
        "#,
    )
    .bind(table_ident)
    .bind(holder)
    .bind(ttl_secs as f64)
    .fetch_optional(pool)
    .await?;
    Ok(epoch)
}

/// Release a held lease (shutdown path); lets the next committer take
/// over without waiting out the TTL. No-op if not the holder.
pub async fn release(pool: &PgPool, table_ident: &str, holder: &str) -> Result<()> {
    sqlx::query(
        "UPDATE iceberg_committers SET lease_expiry = NOW() \
         WHERE table_ident = $1 AND holder = $2",
    )
    .bind(table_ident)
    .bind(holder)
    .execute(pool)
    .await?;
    Ok(())
}
