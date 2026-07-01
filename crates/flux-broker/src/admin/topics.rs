// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Topic management endpoints.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};

use super::AdminState;

/// Topic information.
#[derive(Debug, Serialize)]
pub struct TopicInfo {
    pub topic_id: i32,
    pub name: String,
    pub retention_hours: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Request to create a topic.
#[derive(Debug, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    #[serde(default = "default_retention")]
    pub retention_hours: i32,
}

fn default_retention() -> i32 {
    168 // 7 days
}

/// Request to update a topic.
#[derive(Debug, Deserialize)]
pub struct UpdateTopicRequest {
    pub retention_hours: Option<i32>,
}

/// Response with created topic ID.
#[derive(Debug, Serialize)]
pub struct CreateTopicResponse {
    pub topic_id: i32,
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/", get(list_topics).post(create_topic))
        .route(
            "/:id",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
}

async fn list_topics(State(state): State<AdminState>) -> Result<Json<Vec<TopicInfo>>, StatusCode> {
    let rows: Vec<TopicRow> = sqlx::query_as(
        r#"
        SELECT topic_id, name, retention_hours, created_at
        FROM topics
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let topics: Vec<TopicInfo> = rows
        .into_iter()
        .map(|r| TopicInfo {
            topic_id: r.topic_id,
            name: r.name,
            retention_hours: r.retention_hours,
            created_at: r.created_at,
        })
        .collect();

    Ok(Json(topics))
}

async fn create_topic(
    State(state): State<AdminState>,
    Json(req): Json<CreateTopicRequest>,
) -> Result<(StatusCode, Json<CreateTopicResponse>), StatusCode> {
    // Insert topic. Topic offset is created by DB trigger.
    let topic_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO topics (name, retention_hours)
        VALUES ($1, $2)
        RETURNING topic_id
        "#,
    )
    .bind(&req.name)
    .bind(req.retention_hours)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        if let Some(db_err) = e.as_database_error()
            && db_err.is_unique_violation()
        {
            return StatusCode::CONFLICT;
        }
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((StatusCode::CREATED, Json(CreateTopicResponse { topic_id })))
}

async fn get_topic(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
) -> Result<Json<TopicInfo>, StatusCode> {
    let row: Option<TopicRow> = sqlx::query_as(
        r#"
        SELECT topic_id, name, retention_hours, created_at
        FROM topics
        WHERE topic_id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let row = row.ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(TopicInfo {
        topic_id: row.topic_id,
        name: row.name,
        retention_hours: row.retention_hours,
        created_at: row.created_at,
    }))
}

async fn update_topic(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
    Json(req): Json<UpdateTopicRequest>,
) -> Result<StatusCode, StatusCode> {
    if let Some(retention) = req.retention_hours {
        let result = sqlx::query(
            r#"
            UPDATE topics
            SET retention_hours = $1
            WHERE topic_id = $2
            "#,
        )
        .bind(retention)
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if result.rows_affected() == 0 {
            return Err(StatusCode::NOT_FOUND);
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_topic(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
) -> Result<StatusCode, StatusCode> {
    let result = sqlx::query("DELETE FROM topics WHERE topic_id = $1")
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if result.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(sqlx::FromRow)]
struct TopicRow {
    topic_id: i32,
    name: String,
    retention_hours: i32,
    created_at: chrono::DateTime<chrono::Utc>,
}
