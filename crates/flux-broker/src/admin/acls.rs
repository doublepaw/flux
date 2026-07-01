// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! ACL management endpoints.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get},
};
use serde::{Deserialize, Serialize};

use super::AdminState;
use crate::auth::{Operation, ResourceType};

/// ACL information.
#[derive(Debug, Serialize)]
pub struct AclInfo {
    pub acl_id: i32,
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub operation: String,
    pub permission: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Request to create an ACL.
#[derive(Debug, Deserialize)]
pub struct CreateAclRequest {
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub operation: String,
    #[serde(default = "default_allow")]
    pub allow: bool,
}

fn default_allow() -> bool {
    true
}

/// Response with created ACL ID.
#[derive(Debug, Serialize)]
pub struct CreateAclResponse {
    pub acl_id: i32,
}

/// Query parameters for listing ACLs.
#[derive(Debug, Deserialize)]
pub struct ListAclsQuery {
    pub principal: Option<String>,
    pub resource_type: Option<String>,
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/", get(list_acls).post(create_acl))
        .route("/:id", delete(delete_acl))
}

async fn list_acls(
    State(state): State<AdminState>,
    Query(query): Query<ListAclsQuery>,
) -> Result<Json<Vec<AclInfo>>, StatusCode> {
    let resource_type = query
        .resource_type
        .as_ref()
        .and_then(|rt| match rt.as_str() {
            "topic" => Some(ResourceType::Topic),
            "group" => Some(ResourceType::Group),
            "cluster" => Some(ResourceType::Cluster),
            _ => None,
        });

    let acls = state
        .acl_checker
        .list_acls(query.principal.as_deref(), resource_type)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result: Vec<AclInfo> = acls
        .into_iter()
        .map(|a| AclInfo {
            acl_id: a.acl_id,
            principal: a.principal,
            resource_type: a.resource_type,
            resource_name: a.resource_name,
            operation: a.operation,
            permission: a.permission,
            created_at: a.created_at,
        })
        .collect();

    Ok(Json(result))
}

async fn create_acl(
    State(state): State<AdminState>,
    Json(req): Json<CreateAclRequest>,
) -> Result<(StatusCode, Json<CreateAclResponse>), StatusCode> {
    let resource_type = match req.resource_type.as_str() {
        "topic" => ResourceType::Topic,
        "group" => ResourceType::Group,
        "cluster" => ResourceType::Cluster,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let operation = match req.operation.as_str() {
        "append" => Operation::Append,
        "consume" => Operation::Consume,
        "admin" => Operation::Admin,
        "describe" => Operation::Describe,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let acl_id = state
        .acl_checker
        .create_acl(
            &req.principal,
            resource_type,
            &req.resource_name,
            operation,
            req.allow,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((StatusCode::CREATED, Json(CreateAclResponse { acl_id })))
}

async fn delete_acl(
    State(state): State<AdminState>,
    Path(id): Path<i32>,
) -> Result<StatusCode, StatusCode> {
    let deleted = state
        .acl_checker
        .delete_acl(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if deleted.is_some() {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
