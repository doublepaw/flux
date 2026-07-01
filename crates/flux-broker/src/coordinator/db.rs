// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! SQL operations for reader group coordination.

use flux_common::ids::{Offset, TopicId};
use sqlx::PgPool;

use super::{CommitStatus, CoordinatorConfig, HeartbeatStatus, PollResult, PollStatus};

/// Reader group coordinator.
#[derive(Clone)]
pub struct Coordinator {
    pool: PgPool,
    config: CoordinatorConfig,
    broker_id: uuid::Uuid,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(pool: PgPool, config: CoordinatorConfig) -> Self {
        let broker_id = config.broker_id;
        Self {
            pool,
            config,
            broker_id,
        }
    }

    /// Lock the group's state row for the rest of the transaction.
    ///
    /// Every transaction that deletes inflight rows and recomputes
    /// committed_watermark from MIN(start_offset) must take this lock first:
    /// under READ COMMITTED, concurrent recomputes each see the other's
    /// not-yet-committed DELETE and pin the watermark below the true
    /// committed prefix. Taking this lock before touching reader_inflight or
    /// reader_members also keeps lock ordering consistent with poll and
    /// join_group (group state first), preventing deadlocks.
    async fn lock_group_state(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        group_id: &str,
        topic_id: TopicId,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            SELECT 1 FROM reader_group_state
            WHERE group_id = $1 AND topic_id = $2
            FOR UPDATE
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Handle a JoinGroup request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id)
    )]
    pub async fn join_group(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Initialize group if first reader
        self.initialize_group(&mut tx, group_id, topic_id).await?;

        // 2. Register member (upsert)
        sqlx::query(
            r#"
            INSERT INTO reader_members (group_id, topic_id, reader_id, broker_id, last_heartbeat)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (group_id, topic_id, reader_id)
            DO UPDATE SET last_heartbeat = NOW(), broker_id = $4
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .bind(self.broker_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Handle a Heartbeat request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id)
    )]
    pub async fn heartbeat(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<HeartbeatStatus, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        Self::lock_group_state(&mut tx, group_id, topic_id).await?;

        // 1. Update member heartbeat
        let updated = sqlx::query(
            r#"
            UPDATE reader_members
            SET last_heartbeat = NOW()
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(HeartbeatStatus::UnknownMember);
        }

        // 2. Renew leases on this reader's inflight ranges
        let lease_secs = self.config.lease_duration.as_secs_f64();
        sqlx::query(
            r#"
            UPDATE reader_inflight
            SET lease_expires_at = NOW() + make_interval(secs => $1)
            WHERE group_id = $2 AND topic_id = $3 AND reader_id = $4
            "#,
        )
        .bind(lease_secs)
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // 3. Detect and remove expired members
        let session_timeout_secs = self.config.session_timeout.as_secs_f64();
        let expired: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT reader_id FROM reader_members
            WHERE group_id = $1 AND topic_id = $2
            AND last_heartbeat < NOW() - make_interval(secs => $3)
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(session_timeout_secs)
        .fetch_all(&mut *tx)
        .await?;

        if !expired.is_empty() {
            // Delete expired members
            sqlx::query(
                r#"
                DELETE FROM reader_members
                WHERE group_id = $1 AND topic_id = $2
                AND reader_id = ANY($3)
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&expired)
            .execute(&mut *tx)
            .await?;

            // Release expired members' inflight ranges and roll back
            // dispatch_cursor so those ranges get re-dispatched.
            sqlx::query(
                r#"
                WITH deleted AS (
                    DELETE FROM reader_inflight
                    WHERE group_id = $1 AND topic_id = $2
                    AND reader_id = ANY($3)
                    RETURNING start_offset
                )
                UPDATE reader_group_state
                SET dispatch_cursor = LEAST(
                    dispatch_cursor,
                    COALESCE((SELECT MIN(start_offset) FROM deleted), dispatch_cursor)
                )
                WHERE group_id = $1 AND topic_id = $2
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&expired)
            .execute(&mut *tx)
            .await?;

            // Update committed_watermark
            sqlx::query(
                r#"
                UPDATE reader_group_state
                SET committed_watermark = COALESCE(
                    (SELECT MIN(start_offset) FROM reader_inflight
                     WHERE group_id = $1 AND topic_id = $2),
                    dispatch_cursor
                )
                WHERE group_id = $1 AND topic_id = $2
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(HeartbeatStatus::Ok)
    }

    /// Handle a LeaveGroup request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id)
    )]
    pub async fn leave_group(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        Self::lock_group_state(&mut tx, group_id, topic_id).await?;

        // 1. Release this reader's inflight ranges and roll back
        //    dispatch_cursor so those ranges get re-dispatched to other readers.
        sqlx::query(
            r#"
            WITH deleted AS (
                DELETE FROM reader_inflight
                WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
                RETURNING start_offset
            )
            UPDATE reader_group_state
            SET dispatch_cursor = LEAST(
                dispatch_cursor,
                COALESCE((SELECT MIN(start_offset) FROM deleted), dispatch_cursor)
            )
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // 2. Update committed_watermark
        sqlx::query(
            r#"
            UPDATE reader_group_state
            SET committed_watermark = COALESCE(
                (SELECT MIN(start_offset) FROM reader_inflight
                 WHERE group_id = $1 AND topic_id = $2),
                dispatch_cursor
            )
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        // 3. Remove member
        sqlx::query(
            r#"
            DELETE FROM reader_members
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Handle a CommitRange request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(
            group_id = %group_id,
            topic_id = topic_id.0,
            reader_id = %reader_id,
            start_offset = start_offset.0,
            end_offset = end_offset.0
        )
    )]
    pub async fn commit_range(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
        start_offset: Offset,
        end_offset: Offset,
    ) -> Result<CommitStatus, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        Self::lock_group_state(&mut tx, group_id, topic_id).await?;

        // 1. Delete the inflight range owned by this reader
        let result = sqlx::query(
            r#"
            DELETE FROM reader_inflight
            WHERE group_id = $1 AND topic_id = $2
              AND start_offset = $3 AND end_offset = $4
              AND reader_id = $5
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(start_offset.0 as i64)
        .bind(end_offset.0 as i64)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(CommitStatus::NotOwner);
        }

        // 2. Advance committed_watermark
        sqlx::query(
            r#"
            UPDATE reader_group_state
            SET committed_watermark = COALESCE(
                (SELECT MIN(start_offset) FROM reader_inflight
                 WHERE group_id = $1 AND topic_id = $2),
                dispatch_cursor
            )
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(CommitStatus::Ok)
    }

    /// Handle a Poll request: dispatch the next offset range to the reader.
    ///
    /// Supports pipelined polling: a reader can have multiple outstanding
    /// (uncommitted) ranges, up to `max_inflight_per_reader`. Expired leases
    /// are re-dispatched to the requesting reader before advancing the cursor.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(
            group_id = %group_id,
            topic_id = topic_id.0,
            reader_id = %reader_id,
            max_bytes = max_bytes,
        )
    )]
    pub async fn poll(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
        max_bytes: u32,
    ) -> Result<PollResult, sqlx::Error> {
        let lease_secs = self.config.lease_duration.as_secs_f64();
        let lease_duration_ms = self.config.lease_duration.as_millis() as u64;
        let mut tx = self.pool.begin().await?;

        // 1. Verify member exists
        let member_exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM reader_members
                WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            )
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .fetch_one(&mut *tx)
        .await?;

        if !member_exists {
            tx.commit().await?;
            return Ok(PollResult {
                start_offset: Offset(0),
                end_offset: Offset(0),
                lease_deadline_ms: 0,
                status: PollStatus::Ok,
            });
        }

        // 2. Check max inflight per reader
        let inflight_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM reader_inflight
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .fetch_one(&mut *tx)
        .await?;

        if inflight_count >= self.config.max_inflight_per_reader as i64 {
            tx.commit().await?;
            return Ok(PollResult {
                start_offset: Offset(0),
                end_offset: Offset(0),
                lease_deadline_ms: 0,
                status: PollStatus::MaxInflight,
            });
        }

        // 4. Try to steal an expired inflight range before advancing the cursor
        let stolen: Option<(i64, i64)> = sqlx::query_as(
            r#"
            UPDATE reader_inflight
            SET reader_id = $3,
                lease_expires_at = NOW() + make_interval(secs => $4)
            WHERE ctid = (
                SELECT ctid FROM reader_inflight
                WHERE group_id = $1 AND topic_id = $2 AND lease_expires_at < NOW()
                ORDER BY start_offset
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING start_offset, end_offset
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .bind(lease_secs)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((start, end)) = stolen {
            // Compute lease deadline
            let now_ms: i64 =
                sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint")
                    .fetch_one(&mut *tx)
                    .await?;
            let deadline = now_ms as u64 + lease_duration_ms;

            tx.commit().await?;
            return Ok(PollResult {
                start_offset: Offset(start as u64),
                end_offset: Offset(end as u64),
                lease_deadline_ms: deadline,
                status: PollStatus::Ok,
            });
        }

        // 5. Read dispatch cursor (FOR UPDATE serializes concurrent polls)
        //    and high watermark in a single round-trip
        let (dispatch_cursor, high_watermark): (i64, i64) = sqlx::query_as(
            r#"
            WITH cursor AS (
                SELECT dispatch_cursor
                FROM reader_group_state
                WHERE group_id = $1 AND topic_id = $2
                FOR UPDATE
            ),
            hw AS (
                SELECT COALESCE(next_offset, 0) AS high_watermark
                FROM topic_offsets
                WHERE topic_id = $3
            )
            SELECT cursor.dispatch_cursor, COALESCE(hw.high_watermark, 0)
            FROM cursor
            LEFT JOIN hw ON true
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(topic_id.0 as i32)
        .fetch_one(&mut *tx)
        .await?;

        if dispatch_cursor >= high_watermark {
            tx.commit().await?;
            return Ok(PollResult {
                start_offset: Offset(dispatch_cursor as u64),
                end_offset: Offset(dispatch_cursor as u64),
                lease_deadline_ms: 0,
                status: PollStatus::Ok,
            });
        }

        // 6. Find batches from dispatch_cursor, respecting max_bytes
        let batches: Vec<(i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT start_offset, end_offset, byte_length
            FROM topic_batches
            WHERE topic_id = $1
              AND end_offset > $2
              AND start_offset < $3
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(topic_id.0 as i32)
        .bind(dispatch_cursor)
        .bind(high_watermark)
        .fetch_all(&mut *tx)
        .await?;

        if batches.is_empty() {
            tx.commit().await?;
            return Ok(PollResult {
                start_offset: Offset(dispatch_cursor as u64),
                end_offset: Offset(dispatch_cursor as u64),
                lease_deadline_ms: 0,
                status: PollStatus::Ok,
            });
        }

        // Determine the range to dispatch, accumulating byte_length
        let start_offset = dispatch_cursor;
        let mut end_offset = start_offset;
        let mut cumulative_bytes: i64 = 0;
        for (_, batch_end, byte_length) in &batches {
            cumulative_bytes += byte_length;
            end_offset = *batch_end;
            if cumulative_bytes >= max_bytes as i64 {
                break;
            }
        }

        // 7. Insert inflight range with lease
        sqlx::query(
            r#"
            INSERT INTO reader_inflight
            (group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + make_interval(secs => $6))
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(start_offset)
        .bind(end_offset)
        .bind(reader_id)
        .bind(lease_secs)
        .execute(&mut *tx)
        .await?;

        // 8. Advance dispatch cursor
        sqlx::query(
            r#"
            UPDATE reader_group_state
            SET dispatch_cursor = $3
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(end_offset)
        .execute(&mut *tx)
        .await?;

        // 9. Compute lease deadline
        let now_ms: i64 = sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint")
            .fetch_one(&mut *tx)
            .await?;
        let deadline = now_ms as u64 + lease_duration_ms;

        tx.commit().await?;

        Ok(PollResult {
            start_offset: Offset(start_offset as u64),
            end_offset: Offset(end_offset as u64),
            lease_deadline_ms: deadline,
            status: PollStatus::Ok,
        })
    }

    /// Initialize a reader group and its dispatch state.
    async fn initialize_group(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        group_id: &str,
        topic_id: TopicId,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO reader_groups (group_id, topic_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO reader_group_state
            (group_id, topic_id, dispatch_cursor, committed_watermark)
            VALUES ($1, $2, 0, 0)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// BREAK-GLASS: Reset a consumer group by evicting all members, clearing
    /// inflight ranges, and rolling back the dispatch cursor so uncommitted
    /// ranges are re-dispatched when new readers join.
    pub async fn force_reset(&self, group_id: &str, topic_id: TopicId) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM reader_inflight WHERE group_id = $1 AND topic_id = $2")
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM reader_members WHERE group_id = $1 AND topic_id = $2")
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .execute(&mut *tx)
            .await?;

        // Roll back dispatch_cursor to committed_watermark so uncommitted
        // ranges are re-dispatched when new readers join.
        sqlx::query(
            r#"
            UPDATE reader_group_state
            SET dispatch_cursor = committed_watermark
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// BREAK-GLASS: Force remove a member from the group.
    pub async fn force_remove_member(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        Self::lock_group_state(&mut tx, group_id, topic_id).await?;

        // Release all inflight ranges owned by this reader
        sqlx::query(
            r#"
            DELETE FROM reader_inflight
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // Advance committed_watermark
        sqlx::query(
            r#"
            UPDATE reader_group_state
            SET committed_watermark = COALESCE(
                (SELECT MIN(start_offset) FROM reader_inflight
                 WHERE group_id = $1 AND topic_id = $2),
                dispatch_cursor
            )
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        // Remove from members
        sqlx::query(
            r#"
            DELETE FROM reader_members
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }
}
