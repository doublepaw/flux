// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Reader group management endpoints with break-glass operations.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use flux_common::ids::TopicId;
use serde::Serialize;

use super::AdminState;

/// Reader group summary.
#[derive(Debug, Serialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub topic_id: i32,
    pub member_count: i32,
}

/// Reader group member.
#[derive(Debug, Serialize)]
pub struct GroupMember {
    pub reader_id: String,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

/// Reader group details.
#[derive(Debug, Serialize)]
pub struct GroupDetails {
    pub group_id: String,
    pub topic_id: i32,
    pub members: Vec<GroupMember>,
}

/// Response from force reset.
#[derive(Debug, Serialize)]
pub struct ResetResponse {
    pub success: bool,
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/", get(list_groups))
        .route("/:group_id/topics/:topic_id", get(get_group))
        .route("/:group_id/topics/:topic_id/reset", post(force_reset))
        .route(
            "/:group_id/topics/:topic_id/members/:member_id",
            delete(force_remove_member),
        )
}

async fn list_groups(
    State(state): State<AdminState>,
) -> Result<Json<Vec<GroupSummary>>, StatusCode> {
    let rows: Vec<GroupRow> = sqlx::query_as(
        r#"
        SELECT
            group_id,
            topic_id,
            (SELECT COUNT(*)::int FROM reader_members cm
             WHERE cm.group_id = cg.group_id AND cm.topic_id = cg.topic_id) as member_count
        FROM reader_groups cg
        ORDER BY group_id, topic_id
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let groups: Vec<GroupSummary> = rows
        .into_iter()
        .map(|r| GroupSummary {
            group_id: r.group_id,
            topic_id: r.topic_id,
            member_count: r.member_count,
        })
        .collect();

    Ok(Json(groups))
}

async fn get_group(
    State(state): State<AdminState>,
    Path((group_id, topic_id)): Path<(String, i32)>,
) -> Result<Json<GroupDetails>, StatusCode> {
    // Verify group exists
    let exists: Option<(i32,)> = sqlx::query_as(
        r#"
        SELECT 1
        FROM reader_groups
        WHERE group_id = $1 AND topic_id = $2
        "#,
    )
    .bind(&group_id)
    .bind(topic_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if exists.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Get members
    let member_rows: Vec<MemberRow> = sqlx::query_as(
        r#"
        SELECT reader_id, last_heartbeat
        FROM reader_members
        WHERE group_id = $1 AND topic_id = $2
        ORDER BY reader_id
        "#,
    )
    .bind(&group_id)
    .bind(topic_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let members: Vec<GroupMember> = member_rows
        .into_iter()
        .map(|m| GroupMember {
            reader_id: m.reader_id,
            last_heartbeat: m.last_heartbeat,
        })
        .collect();

    Ok(Json(GroupDetails {
        group_id,
        topic_id,
        members,
    }))
}

/// BREAK-GLASS: Reset a consumer group by evicting all members and clearing inflight.
async fn force_reset(
    State(state): State<AdminState>,
    Path((group_id, topic_id)): Path<(String, i32)>,
) -> Result<Json<ResetResponse>, StatusCode> {
    state
        .coordinator
        .force_reset(&group_id, TopicId(topic_id as u32))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ResetResponse { success: true }))
}

/// BREAK-GLASS: Force remove a member from the group.
async fn force_remove_member(
    State(state): State<AdminState>,
    Path((group_id, topic_id, member_id)): Path<(String, i32, String)>,
) -> Result<StatusCode, StatusCode> {
    // Use coordinator to remove member
    state
        .coordinator
        .force_remove_member(&group_id, TopicId(topic_id as u32), &member_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, sqlx::FromRow)]
struct GroupRow {
    group_id: String,
    topic_id: i32,
    member_count: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct MemberRow {
    reader_id: String,
    last_heartbeat: chrono::DateTime<chrono::Utc>,
}
