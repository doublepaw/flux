// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Admin HTTP API for management operations.
//!
//! Provides REST endpoints for:
//! - Topic management
//! - API key management
//! - ACL management
//! - Reader group management
//! - Schema registry

pub mod acls;
pub mod api_keys;
pub mod groups;
pub mod middleware;
pub mod topics;

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use axum::routing::get;
use flux_schema::SchemaRegistry;
use sqlx::PgPool;

use crate::auth::{AclChecker, ApiKeyValidator};
use crate::coordinator::Coordinator;
use crate::metrics;

/// Admin server configuration.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Address to bind the admin HTTP server to.
    pub bind_addr: SocketAddr,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9001".parse().unwrap(),
        }
    }
}

/// Shared state for admin endpoints.
#[derive(Clone)]
pub struct AdminState {
    pub pool: PgPool,
    pub api_key_validator: ApiKeyValidator,
    pub acl_checker: AclChecker,
    pub coordinator: Coordinator,
}

impl AdminState {
    pub fn new(pool: PgPool, coordinator: Coordinator) -> Self {
        Self {
            api_key_validator: ApiKeyValidator::new(pool.clone()),
            acl_checker: AclChecker::new(pool.clone()),
            pool,
            coordinator,
        }
    }
}

/// Create the admin API router.
pub fn create_router(state: AdminState) -> Router {
    // Schema registry (self-contained with its own state)
    let schema_state = flux_schema::AppState {
        registry: SchemaRegistry::new(state.pool.clone()),
    };
    let schema_router = flux_schema::router(schema_state);

    // Authenticated routes
    let authenticated = Router::new()
        .nest("/topics", topics::router())
        .nest("/api-keys", api_keys::router())
        .nest("/acls", acls::router())
        .nest("/groups", groups::router())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            middleware::auth_middleware,
        ))
        .with_state(state)
        .merge(schema_router);

    // Unauthenticated routes (metrics for Prometheus scraping)
    Router::new()
        .route("/metrics", get(metrics::metrics_handler))
        .merge(authenticated)
}

/// Run the admin HTTP server.
pub async fn run(config: AdminConfig, state: AdminState) -> Result<(), std::io::Error> {
    run_with_shutdown(config, state, std::future::pending()).await
}

/// Run the admin HTTP server with graceful shutdown support.
pub async fn run_with_shutdown<F>(
    config: AdminConfig,
    state: AdminState,
    shutdown: F,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    tracing::info!("Admin API listening on {}", config.bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}
