// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! HTTP API for schema registry.
//!
//! Provides REST endpoints for schema management:
//! - POST /schemas - Register a schema for a topic
//! - GET /schemas/:id - Get schema by ID
//! - GET /topics/:topic_id/schemas - List schemas for topic
//! - GET /topics/:topic_id/schemas/latest - Get latest schema for topic
//! - POST /topics/:topic_id/schemas/check - Check compatibility

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use flux_common::ids::{SchemaId, TopicId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::{SchemaError, SchemaRegistry};

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub registry: SchemaRegistry,
}

/// Request to register a schema.
#[derive(Debug, Deserialize)]
pub struct RegisterSchemaRequest {
    pub topic_id: u32,
    pub schema: Value,
}

/// Response after registering a schema.
#[derive(Debug, Serialize)]
pub struct RegisterSchemaResponse {
    pub schema_id: u32,
}

/// Schema details response.
#[derive(Debug, Serialize)]
pub struct SchemaResponse {
    pub schema_id: u32,
    pub schema: Value,
    pub created_at: String,
}

/// Topic-schema association response.
#[derive(Debug, Serialize)]
pub struct TopicSchemaResponse {
    pub topic_id: u32,
    pub schema_id: u32,
    pub created_at: String,
}

/// Request to check compatibility.
#[derive(Debug, Deserialize)]
pub struct CompatibilityCheckRequest {
    pub schema: Value,
}

/// Compatibility check response.
#[derive(Debug, Serialize)]
pub struct CompatibilityResponse {
    pub is_compatible: bool,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error_code: u32,
    pub message: String,
}

/// Build the schema registry router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/schemas", post(register_schema))
        .route("/schemas/:id", get(get_schema))
        .route("/topics/:topic_id/schemas", get(list_topic_schemas))
        .route("/topics/:topic_id/schemas/latest", get(get_latest_schema))
        .route("/topics/:topic_id/schemas/check", post(check_compatibility))
        .route("/health", get(health_check))
        .with_state(Arc::new(state))
}

/// Health check endpoint.
async fn health_check() -> &'static str {
    "ok"
}

/// Register a schema for a topic.
async fn register_schema(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSchemaRequest>,
) -> Result<Json<RegisterSchemaResponse>, ApiError> {
    let schema_id = state
        .registry
        .register(TopicId(req.topic_id), &req.schema)
        .await?;

    Ok(Json(RegisterSchemaResponse {
        schema_id: schema_id.0,
    }))
}

/// Get a schema by ID.
async fn get_schema(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u32>,
) -> Result<Json<SchemaResponse>, ApiError> {
    let record = state.registry.get_schema(SchemaId(id)).await?;

    Ok(Json(SchemaResponse {
        schema_id: record.schema_id.0,
        schema: record.schema_json,
        created_at: record.created_at.to_rfc3339(),
    }))
}

/// List all schemas for a topic.
async fn list_topic_schemas(
    State(state): State<Arc<AppState>>,
    Path(topic_id): Path<u32>,
) -> Result<Json<Vec<TopicSchemaResponse>>, ApiError> {
    let schemas = state
        .registry
        .list_schemas_for_topic(TopicId(topic_id))
        .await?;

    Ok(Json(
        schemas
            .into_iter()
            .map(|s| TopicSchemaResponse {
                topic_id: s.topic_id.0,
                schema_id: s.schema_id.0,
                created_at: s.created_at.to_rfc3339(),
            })
            .collect(),
    ))
}

/// Get the latest schema for a topic.
async fn get_latest_schema(
    State(state): State<Arc<AppState>>,
    Path(topic_id): Path<u32>,
) -> Result<Json<SchemaResponse>, ApiError> {
    let record = state
        .registry
        .get_latest_schema_for_topic(TopicId(topic_id))
        .await?
        .ok_or(SchemaError::TopicNotFound {
            topic_id: TopicId(topic_id),
        })?;

    Ok(Json(SchemaResponse {
        schema_id: record.schema_id.0,
        schema: record.schema_json,
        created_at: record.created_at.to_rfc3339(),
    }))
}

/// Check if a schema is compatible with the latest version.
async fn check_compatibility(
    State(state): State<Arc<AppState>>,
    Path(topic_id): Path<u32>,
    Json(req): Json<CompatibilityCheckRequest>,
) -> Result<Json<CompatibilityResponse>, ApiError> {
    let is_compatible = state
        .registry
        .check_compatibility(TopicId(topic_id), &req.schema)
        .await?;

    Ok(Json(CompatibilityResponse { is_compatible }))
}

/// API error type that converts to HTTP responses.
pub struct ApiError(SchemaError);

impl From<SchemaError> for ApiError {
    fn from(e: SchemaError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_code, message) = match &self.0 {
            SchemaError::SchemaNotFound { .. } => {
                (StatusCode::NOT_FOUND, 40401, self.0.to_string())
            }
            SchemaError::TopicNotFound { .. } => (StatusCode::NOT_FOUND, 40402, self.0.to_string()),
            SchemaError::IncompatibleSchema { .. } => {
                (StatusCode::CONFLICT, 40901, self.0.to_string())
            }
            SchemaError::InvalidSchema { .. } => {
                (StatusCode::UNPROCESSABLE_ENTITY, 42201, self.0.to_string())
            }
            SchemaError::Database(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                50001,
                "database error".to_string(),
            ),
            SchemaError::Avro(_) => (StatusCode::UNPROCESSABLE_ENTITY, 42202, self.0.to_string()),
        };

        let body = Json(ErrorResponse {
            error_code,
            message,
        });

        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_schema_request_deserialize() {
        let json = r#"{"topic_id": 1, "schema": {"type": "record", "name": "Test", "fields": []}}"#;
        let req: RegisterSchemaRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topic_id, 1);
    }

    #[test]
    fn test_schema_response_serialize() {
        let resp = SchemaResponse {
            schema_id: 100,
            schema: serde_json::json!({"type": "string"}),
            created_at: "2024-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"schema_id\":100"));
    }

    #[test]
    fn test_error_response_serialize() {
        let resp = ErrorResponse {
            error_code: 40401,
            message: "schema not found".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("40401"));
    }
}
