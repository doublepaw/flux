// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Admin API authentication middleware.

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

use super::AdminState;
use crate::auth::{AuthError, Principal};

/// Extension type for the authenticated principal.
#[derive(Clone)]
pub struct AuthenticatedPrincipal(pub Principal);

/// Authentication middleware for admin endpoints.
///
/// Extracts the API key from the Authorization header, validates it,
/// and checks for cluster:admin permission.
pub async fn auth_middleware(
    State(state): State<AdminState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Extract API key from Authorization header
    let api_key = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Validate the API key
    let principal = state
        .api_key_validator
        .validate(api_key)
        .await
        .map_err(|e| match e {
            AuthError::MissingApiKey
            | AuthError::InvalidKeyFormat
            | AuthError::InvalidApiKey
            | AuthError::ExpiredApiKey
            | AuthError::RevokedApiKey => StatusCode::UNAUTHORIZED,
            AuthError::PermissionDenied { .. } => StatusCode::FORBIDDEN,
            AuthError::Database(_) | AuthError::Timeout => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    // Check for cluster:admin permission
    let has_admin = state
        .acl_checker
        .check(
            &principal,
            crate::auth::ResourceType::Cluster,
            "*",
            crate::auth::Operation::Admin,
        )
        .await;

    if !has_admin {
        return Err(StatusCode::FORBIDDEN);
    }

    // Store principal in request extensions for handlers
    request
        .extensions_mut()
        .insert(AuthenticatedPrincipal(principal));

    Ok(next.run(request).await)
}
