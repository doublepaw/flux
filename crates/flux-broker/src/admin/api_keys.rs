// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! API key management endpoints.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::AdminState;

/// API key information (without secret).
#[derive(Debug, Serialize)]
pub struct ApiKeyInfo {
    pub key_id: Uuid,
    pub name: String,
    pub principal: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Request to create an API key.
#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub principal: String,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Response with created API key (secret shown only once).
#[derive(Debug, Serialize)]
pub struct CreateApiKeyResponse {
    pub key_id: Uuid,
    pub api_key: String,
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/", get(list_api_keys).post(create_api_key))
        .route("/:id", get(get_api_key).delete(revoke_api_key))
}

async fn list_api_keys(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ApiKeyInfo>>, StatusCode> {
    let keys = state
        .api_key_validator
        .list_keys()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result: Vec<ApiKeyInfo> = keys
        .into_iter()
        .map(|k| ApiKeyInfo {
            key_id: k.key_id,
            name: k.name,
            principal: k.principal,
            created_at: k.created_at,
            expires_at: k.expires_at,
            revoked_at: k.revoked_at,
            last_used_at: k.last_used_at,
        })
        .collect();

    Ok(Json(result))
}

async fn create_api_key(
    State(state): State<AdminState>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<CreateApiKeyResponse>), StatusCode> {
    let (api_key, key_id) = state
        .api_key_validator
        .create_key(&req.name, &req.principal, req.expires_at)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((
        StatusCode::CREATED,
        Json(CreateApiKeyResponse { key_id, api_key }),
    ))
}

async fn get_api_key(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
) -> Result<Json<ApiKeyInfo>, StatusCode> {
    let key = state
        .api_key_validator
        .get_key(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ApiKeyInfo {
        key_id: key.key_id,
        name: key.name,
        principal: key.principal,
        created_at: key.created_at,
        expires_at: key.expires_at,
        revoked_at: key.revoked_at,
        last_used_at: key.last_used_at,
    }))
}

async fn revoke_api_key(
    State(state): State<AdminState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let revoked = state
        .api_key_validator
        .revoke_key(id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if revoked {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
