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

use flux_broker::buffer::BufferConfig;
use flux_broker::{
    AdminConfig, AdminState, BrokerConfig, BrokerState, CloudObjectStore, Coordinator,
    CoordinatorConfig, LocalFsStore, ObjectStore, S3ObjectStore, admin, metrics, run_with_shutdown,
    shutdown_signal,
};

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

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

    // Object store backend: fs (default), s3, gcs, azure.
    let backend = env::var("OBJECT_STORE").unwrap_or_else(|_| "fs".to_string());
    let bucket = env::var("OBJECT_STORE_BUCKET").unwrap_or_else(|_| "flux".to_string());
    let key_prefix = env::var("OBJECT_STORE_PREFIX").unwrap_or_else(|_| "data".to_string());

    info!("Starting flux-broker");
    info!("WebSocket server: {}", ws_addr);
    info!("Admin API: {}", admin_addr);
    info!("Object store: {} (bucket={})", backend, bucket);

    // Create database pool
    let pool = PgPoolOptions::new()
        .max_connections(env_or("PG_POOL_SIZE", 10u32))
        .connect(&database_url)
        .await?;

    info!("Connected to database");

    // Broker configuration (env-tunable for benchmarks and deployment)
    let buffer = BufferConfig {
        max_size_bytes: env_or("BUFFER_MAX_BYTES", 256 * 1024 * 1024usize),
        max_wait: Duration::from_millis(env_or("BUFFER_MAX_WAIT_MS", 200u64)),
        high_water_bytes: env_or("BUFFER_HIGH_WATER_BYTES", 384 * 1024 * 1024usize),
        low_water_bytes: env_or("BUFFER_LOW_WATER_BYTES", 128 * 1024 * 1024usize),
        segment_max_bytes: env_or("SEGMENT_MAX_BYTES", 256 * 1024 * 1024usize),
    };
    let broker_config = BrokerConfig {
        bind_addr: ws_addr,
        bucket: bucket.clone(),
        key_prefix,
        buffer,
        flush_interval: Duration::from_millis(env_or("FLUSH_INTERVAL_MS", 50u64)),
        readahead_max_bytes: env_or("READAHEAD_MAX_BYTES", 0usize),
        flush_pipeline_depth: env_or("FLUSH_PIPELINE_DEPTH", 4usize),
        ..Default::default()
    };

    match backend.as_str() {
        "fs" => {
            std::fs::create_dir_all(&data_dir)?;
            info!("Data directory: {}", data_dir);
            serve(
                pool,
                LocalFsStore::new(&data_dir),
                broker_config,
                admin_addr,
                otel_provider,
            )
            .await
        }
        "s3" => {
            let store = match env::var("S3_ENDPOINT") {
                Ok(endpoint) if !endpoint.trim().is_empty() => {
                    S3ObjectStore::new_with_endpoint(&bucket, endpoint).await
                }
                _ => S3ObjectStore::new(&bucket).await,
            };
            serve(pool, store, broker_config, admin_addr, otel_provider).await
        }
        "gcs" => {
            let store = CloudObjectStore::gcs(&bucket)
                .map_err(|e| anyhow::anyhow!("gcs store init failed: {e}"))?;
            serve(pool, store, broker_config, admin_addr, otel_provider).await
        }
        "azure" => {
            let store = CloudObjectStore::azure(&bucket)
                .map_err(|e| anyhow::anyhow!("azure store init failed: {e}"))?;
            serve(pool, store, broker_config, admin_addr, otel_provider).await
        }
        other => anyhow::bail!("unknown OBJECT_STORE backend: {other}"),
    }
}

async fn serve<S: ObjectStore + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    store: S,
    broker_config: BrokerConfig,
    admin_addr: SocketAddr,
    otel_provider: Option<SdkTracerProvider>,
) -> Result<()> {
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
