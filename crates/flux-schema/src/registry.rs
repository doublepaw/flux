// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema registry with database operations.
//!
//! Handles schema storage, deduplication, and topic association.

use flux_common::ids::{SchemaId, TopicId};
use serde_json::Value;
use sqlx::PgPool;

use crate::SchemaError;
use crate::canonical::schema_hash;
use crate::compat::is_backward_compatible;

/// Schema registry backed by Postgres.
#[derive(Clone)]
pub struct SchemaRegistry {
    pool: PgPool,
}

/// Schema with its metadata.
#[derive(Debug, Clone)]
pub struct SchemaRecord {
    pub schema_id: SchemaId,
    pub schema_json: Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Topic-schema association.
#[derive(Debug, Clone)]
pub struct TopicSchema {
    pub topic_id: TopicId,
    pub schema_id: SchemaId,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl SchemaRegistry {
    /// Create a new schema registry with the given database pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Register a schema for a topic.
    ///
    /// Returns the schema ID (existing or newly assigned).
    /// Checks backward compatibility with the latest schema for the topic.
    pub async fn register(
        &self,
        topic_id: TopicId,
        schema: &Value,
    ) -> Result<SchemaId, SchemaError> {
        let hash = schema_hash(schema);

        let mut tx = self.pool.begin().await?;

        // 1. Check if topic exists
        let topic_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM topics WHERE topic_id = $1)")
                .bind(topic_id.0 as i32)
                .fetch_one(&mut *tx)
                .await?;

        if !topic_exists {
            return Err(SchemaError::TopicNotFound { topic_id });
        }

        // 2. Get or create schema
        let schema_id: i32 = match sqlx::query_scalar::<_, i32>(
            "SELECT schema_id FROM schemas WHERE schema_hash = $1"
        )
        .bind(&hash[..])
        .fetch_optional(&mut *tx)
        .await?
        {
            Some(id) => id,
            None => {
                sqlx::query_scalar(
                    "INSERT INTO schemas (schema_hash, schema_json) VALUES ($1, $2) RETURNING schema_id"
                )
                .bind(&hash[..])
                .bind(schema)
                .fetch_one(&mut *tx)
                .await?
            }
        };

        // 3. Check if already registered for topic (idempotent)
        let already_registered: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM topic_schemas WHERE topic_id = $1 AND schema_id = $2)",
        )
        .bind(topic_id.0 as i32)
        .bind(schema_id)
        .fetch_one(&mut *tx)
        .await?;

        if already_registered {
            tx.commit().await?;
            return Ok(SchemaId(schema_id as u32));
        }

        // 4. Get latest schema for topic (with lock)
        let latest: Option<(i32, Value)> = sqlx::query_as(
            r#"
            SELECT s.schema_id, s.schema_json
            FROM topic_schemas ts
            JOIN schemas s USING (schema_id)
            WHERE ts.topic_id = $1
            ORDER BY ts.created_at DESC
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(topic_id.0 as i32)
        .fetch_optional(&mut *tx)
        .await?;

        // 5. Check compatibility if there's a previous schema
        if let Some((_, old_schema)) = latest
            && !is_backward_compatible(schema, &old_schema)?
        {
            return Err(SchemaError::IncompatibleSchema {
                message: "new schema is not backward compatible with the previous schema".into(),
            });
        }

        // 6. Register for topic
        sqlx::query("INSERT INTO topic_schemas (topic_id, schema_id) VALUES ($1, $2)")
            .bind(topic_id.0 as i32)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(SchemaId(schema_id as u32))
    }

    /// Get a schema by ID.
    pub async fn get_schema(&self, schema_id: SchemaId) -> Result<SchemaRecord, SchemaError> {
        let row: Option<(i32, Value, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT schema_id, schema_json, created_at FROM schemas WHERE schema_id = $1",
        )
        .bind(schema_id.0 as i32)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((id, json, created_at)) => Ok(SchemaRecord {
                schema_id: SchemaId(id as u32),
                schema_json: json,
                created_at,
            }),
            None => Err(SchemaError::SchemaNotFound { schema_id }),
        }
    }

    /// List all schemas registered for a topic, ordered by registration time (newest first).
    pub async fn list_schemas_for_topic(
        &self,
        topic_id: TopicId,
    ) -> Result<Vec<TopicSchema>, SchemaError> {
        let rows: Vec<(i32, i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            r#"
            SELECT topic_id, schema_id, created_at
            FROM topic_schemas
            WHERE topic_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(topic_id.0 as i32)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(tid, sid, created_at)| TopicSchema {
                topic_id: TopicId(tid as u32),
                schema_id: SchemaId(sid as u32),
                created_at,
            })
            .collect())
    }

    /// Get the latest schema for a topic.
    pub async fn get_latest_schema_for_topic(
        &self,
        topic_id: TopicId,
    ) -> Result<Option<SchemaRecord>, SchemaError> {
        let row: Option<(i32, Value, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            r#"
            SELECT s.schema_id, s.schema_json, s.created_at
            FROM topic_schemas ts
            JOIN schemas s USING (schema_id)
            WHERE ts.topic_id = $1
            ORDER BY ts.created_at DESC
            LIMIT 1
            "#,
        )
        .bind(topic_id.0 as i32)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(id, json, created_at)| SchemaRecord {
            schema_id: SchemaId(id as u32),
            schema_json: json,
            created_at,
        }))
    }

    /// Check if a schema is compatible with a topic (dry-run, no registration).
    pub async fn check_compatibility(
        &self,
        topic_id: TopicId,
        schema: &Value,
    ) -> Result<bool, SchemaError> {
        let latest = self.get_latest_schema_for_topic(topic_id).await?;

        match latest {
            Some(record) => is_backward_compatible(schema, &record.schema_json),
            None => Ok(true), // No existing schema, any valid schema is compatible
        }
    }

    /// Get schema ID by hash (for deduplication lookup).
    pub async fn get_schema_id_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<SchemaId>, SchemaError> {
        let id: Option<i32> =
            sqlx::query_scalar("SELECT schema_id FROM schemas WHERE schema_hash = $1")
                .bind(&hash[..])
                .fetch_optional(&self.pool)
                .await?;

        Ok(id.map(|id| SchemaId(id as u32)))
    }
}

#[cfg(test)]
mod tests {
    // Integration tests require a database connection
    // These would be run against a test database
}
