// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::{Level, info};
use tracing_subscriber::EnvFilter;

use flux_core::buffer::{AppState, ParquetWriterConfig, build_router};

#[derive(Parser, Debug)]
#[command(name = "flux")]
#[command(about = "High-performance Avro to Parquet conversion service")]
struct Args {
    /// Host to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Output directory for Parquet files
    #[arg(short, long, default_value = "./output")]
    output_dir: String,

    /// Row group size for Parquet files
    #[arg(long, default_value_t = 1_000_000)]
    row_group_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args = Args::parse();

    // Configure Parquet writer
    let config = ParquetWriterConfig::default()
        .with_output_dir(&args.output_dir)
        .with_row_group_size(args.row_group_size);

    // Create application state
    let state = Arc::new(AppState::new(config)?);

    // Build router with middleware
    let app = build_router(state)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new());

    // Start server
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!(address = %addr, "Starting Flux server");
    info!(output_dir = %args.output_dir, "Parquet files will be written to");

    axum::serve(listener, app).await?;

    Ok(())
}
