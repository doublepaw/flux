// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Flux broker binary.
//!
//! Runs both the WebSocket server (for writers/readers) and the Admin HTTP API
//! with graceful shutdown support.

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use flux_broker::{
    AdminConfig, AdminState, BrokerConfig, BrokerState, Coordinator, CoordinatorConfig,
    LocalFsStore, admin, metrics, run_with_shutdown, shutdown_signal,
};

fn init_tracing() -> Result<Option<SdkTracerProvider>> {
    let env_filter = EnvFilter::from_default_env().add_directive("flux_broker=info".parse()?);

    // OTEL export is opt-in to keep local development lightweight.
    let endpoint = match env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
            return Ok(None);
        }
    };

    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "flux-broker".into());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(3))
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer("flux-broker");
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    Ok(Some(provider))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging and optional OpenTelemetry export.
    let otel_provider = init_tracing()?;

    // Register Prometheus metrics
    metrics::register_metrics();

    // Parse configuration from environment
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let ws_addr: SocketAddr = env::var("WS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9000".to_string())
        .parse()
        .expect("Invalid WS_ADDR");
    let admin_addr: SocketAddr = env::var("ADMIN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9001".to_string())
        .parse()
        .expect("Invalid ADMIN_ADDR");
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "/tmp/flux".to_string());

    info!("Starting flux-broker");
    info!("WebSocket server: {}", ws_addr);
    info!("Admin API: {}", admin_addr);
    info!("Data directory: {}", data_dir);

    // Create database pool
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    info!("Connected to database");

    // Create object store (local filesystem for now)
    let store = LocalFsStore::new(&data_dir);
    std::fs::create_dir_all(&data_dir)?;

    // Create broker configuration
    let broker_config = BrokerConfig {
        bind_addr: ws_addr,
        bucket: "flux".to_string(),
        key_prefix: "data".to_string(),
        ..Default::default()
    };

    // Create broker state
    let broker_state = BrokerState::with_coordinator_config(
        pool.clone(),
        store,
        broker_config,
        CoordinatorConfig::default(),
    )
    .await;

    // Create admin configuration and state
    let admin_config = AdminConfig {
        bind_addr: admin_addr,
    };
    let admin_state = AdminState::new(
        pool.clone(),
        Coordinator::new(pool.clone(), CoordinatorConfig::default()),
    );

    // Create shutdown signal channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    // Create shutdown futures for each server
    let ws_shutdown_rx = shutdown_rx.clone();
    let ws_shutdown = async move {
        let mut rx = ws_shutdown_rx;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    let admin_shutdown_rx = shutdown_rx.clone();
    let admin_shutdown = async move {
        let mut rx = admin_shutdown_rx;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    // Run both servers concurrently
    let ws_server = run_with_shutdown(broker_state, ws_shutdown);
    let admin_server = admin::run_with_shutdown(admin_config, admin_state, admin_shutdown);

    info!("Starting servers...");

    tokio::select! {
        result = ws_server => {
            if let Err(e) = result {
                error!("WebSocket server error: {}", e);
            }
        }
        result = admin_server => {
            if let Err(e) = result {
                error!("Admin server error: {}", e);
            }
        }
    }

    info!("Flux broker stopped");

    if let Some(provider) = otel_provider
        && let Err(e) = provider.shutdown()
    {
        error!("Failed to flush OpenTelemetry provider: {}", e);
    }

    Ok(())
}
