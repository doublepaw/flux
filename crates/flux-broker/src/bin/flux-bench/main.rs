// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Flux E2E benchmark CLI.
//!
//! Run writer/reader throughput and latency benchmarks.
//!
//! Usage:
//!   flux-bench writer --scenario baseline
//!   flux-bench writer --writers 10 --record-size 1024 --duration 30
//!   flux-bench report --compare baseline.json --current latest.json

mod remote;

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "flux-bench")]
#[command(about = "Flux eventbus benchmark CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run writer benchmark
    Writer {
        /// Number of concurrent writers
        #[arg(short, long, default_value = "1")]
        writers: usize,

        /// Record payload size in bytes
        #[arg(long, default_value = "1024")]
        record_size: usize,

        /// Duration in seconds
        #[arg(short, long, default_value = "10")]
        duration: u64,

        /// Use a predefined scenario
        #[arg(long)]
        scenario: Option<String>,

        /// Output format: table, json
        #[arg(long, default_value = "table")]
        output: String,

        /// Save results to file
        #[arg(long)]
        save: Option<String>,
    },

    /// Benchmark a running broker over the network (produce + fetch)
    Remote {
        /// Broker WebSocket URL, e.g. ws://flux-broker:9000
        #[arg(long)]
        url: String,

        /// Postgres URL (used once, to resolve/create the topic).
        /// Falls back to $DATABASE_URL.
        #[arg(long)]
        database_url: Option<String>,

        /// Topic name (created if missing)
        #[arg(long, default_value = "bench")]
        topic: String,

        /// Number of concurrent writer connections
        #[arg(long, default_value = "40")]
        writers: usize,

        /// Append requests per writer
        #[arg(long, default_value = "2500")]
        requests_per_writer: u64,

        /// Records per append request
        #[arg(long, default_value = "128")]
        records_per_batch: u32,

        /// Record payload size in bytes
        #[arg(long, default_value = "1024")]
        record_size: usize,

        /// Max in-flight appends per writer
        #[arg(long, default_value = "64")]
        max_in_flight: usize,

        /// Parallel fetch reader connections
        #[arg(long, default_value = "4")]
        fetch_readers: usize,

        /// Max bytes per read request (should equal the broker's
        /// SEGMENT_MAX_BYTES so one read returns one whole segment)
        #[arg(long, default_value = "268435456")]
        max_bytes: u32,

        /// Skip the fetch phase
        #[arg(long, default_value = "false")]
        skip_fetch: bool,
    },

    /// Generate comparison report
    Report {
        /// Baseline results file (JSON)
        #[arg(long)]
        compare: String,

        /// Current results file (JSON)
        #[arg(long)]
        current: String,
    },
}

/// Metrics collected during benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkMetrics {
    name: String,
    git_commit: Option<String>,
    timestamp: String,
    config: BenchmarkConfig,
    throughput: ThroughputMetrics,
    latency: LatencyMetrics,
    batching: BatchingMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkConfig {
    writers: usize,
    record_size: usize,
    duration_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThroughputMetrics {
    records_per_sec: f64,
    bytes_per_sec: f64,
    total_records: u64,
    total_bytes: u64,
    duration_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LatencyMetrics {
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
    min_us: u64,
    max_us: u64,
    mean_us: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchingMetrics {
    records_per_batch: f64,
    compression_ratio: f64,
}

/// Simulated writer for benchmarking.
struct BenchProducer {
    record_size: usize,
    records_sent: AtomicU64,
    bytes_sent: AtomicU64,
}

impl BenchProducer {
    fn new(record_size: usize) -> Self {
        Self {
            record_size,
            records_sent: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
        }
    }

    async fn run(&self, stop: Arc<AtomicBool>, latency_tx: mpsc::UnboundedSender<u64>) {
        let payload = vec![0xABu8; self.record_size];

        while !stop.load(Ordering::Relaxed) {
            let start = Instant::now();

            // Simulate producing a record (in a real benchmark, this would send to the broker)
            // For now, we simulate the work with a small delay
            tokio::time::sleep(Duration::from_micros(100)).await;

            let elapsed_us = start.elapsed().as_micros() as u64;
            let _ = latency_tx.send(elapsed_us);

            self.records_sent.fetch_add(1, Ordering::Relaxed);
            self.bytes_sent
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
        }
    }
}

fn get_git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        })
}

async fn run_producer_benchmark(
    writers: usize,
    record_size: usize,
    duration: Duration,
) -> BenchmarkMetrics {
    let stop = Arc::new(AtomicBool::new(false));
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<u64>();

    // Create writers
    let bench_producers: Vec<_> = (0..writers)
        .map(|_| Arc::new(BenchProducer::new(record_size)))
        .collect();

    // Spawn writer tasks
    let mut handles = Vec::new();
    for writer in &bench_producers {
        let p = Arc::clone(writer);
        let s = Arc::clone(&stop);
        let tx = latency_tx.clone();
        handles.push(tokio::spawn(async move {
            p.run(s, tx).await;
        }));
    }
    drop(latency_tx); // Close sender so receiver can complete

    // Collect latencies in background
    let latency_handle = tokio::spawn(async move {
        let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
        while let Some(latency_us) = latency_rx.recv().await {
            let _ = hist.record(latency_us);
        }
        hist
    });

    // Wait for duration
    let start = Instant::now();
    eprintln!(
        "Running benchmark: {} writers, {}B records, {}s",
        writers,
        record_size,
        duration.as_secs()
    );

    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    // Wait for writers to stop
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();

    // Collect metrics
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;
    for writer in &bench_producers {
        total_records += writer.records_sent.load(Ordering::Relaxed);
        total_bytes += writer.bytes_sent.load(Ordering::Relaxed);
    }

    let hist = latency_handle.await.unwrap();

    let throughput = ThroughputMetrics {
        records_per_sec: total_records as f64 / elapsed.as_secs_f64(),
        bytes_per_sec: total_bytes as f64 / elapsed.as_secs_f64(),
        total_records,
        total_bytes,
        duration_secs: elapsed.as_secs_f64(),
    };

    let latency = LatencyMetrics {
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
        min_us: hist.min(),
        max_us: hist.max(),
        mean_us: hist.mean(),
    };

    let batching = BatchingMetrics {
        records_per_batch: 1.0, // Placeholder - would be collected from actual broker
        compression_ratio: 1.0, // Placeholder
    };

    BenchmarkMetrics {
        name: format!("producer_{}w_{}B", writers, record_size),
        git_commit: get_git_commit(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        config: BenchmarkConfig {
            writers,
            record_size,
            duration_secs: duration.as_secs(),
        },
        throughput,
        latency,
        batching,
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

fn print_table(metrics: &BenchmarkMetrics) {
    println!();
    println!("PRODUCER THROUGHPUT");
    println!(
        "  Records/sec:     {}",
        format_number(metrics.throughput.records_per_sec as u64)
    );
    println!(
        "  Bytes/sec:       {}/s",
        format_bytes(metrics.throughput.bytes_per_sec as u64)
    );
    println!();
    println!("PRODUCER LATENCY (microseconds)");
    println!(
        "  p50: {}    p99: {}    p999: {}",
        format_number(metrics.latency.p50_us),
        format_number(metrics.latency.p99_us),
        format_number(metrics.latency.p999_us)
    );
    println!(
        "  min: {}    max: {}    mean: {:.0}",
        format_number(metrics.latency.min_us),
        format_number(metrics.latency.max_us),
        metrics.latency.mean_us
    );
    println!();
    println!("BATCHING EFFICIENCY");
    println!(
        "  Records/batch:     {:.1}",
        metrics.batching.records_per_batch
    );
    println!(
        "  Compression ratio: {:.1}x",
        metrics.batching.compression_ratio
    );
    println!();
}

fn compare_reports(
    baseline_path: &str,
    current_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline_str = std::fs::read_to_string(baseline_path)?;
    let current_str = std::fs::read_to_string(current_path)?;

    let baseline: BenchmarkMetrics = serde_json::from_str(&baseline_str)?;
    let current: BenchmarkMetrics = serde_json::from_str(&current_str)?;

    fn delta_pct(baseline: f64, current: f64) -> String {
        let pct = ((current - baseline) / baseline) * 100.0;
        if pct >= 0.0 {
            format!("+{:.1}%", pct)
        } else {
            format!("{:.1}%", pct)
        }
    }

    println!();
    println!("COMPARISON: {} vs {}", baseline.name, current.name);
    println!("{}", "─".repeat(60));
    println!();
    println!("THROUGHPUT");
    println!(
        "  Records/sec:  {} → {} ({})",
        format_number(baseline.throughput.records_per_sec as u64),
        format_number(current.throughput.records_per_sec as u64),
        delta_pct(
            baseline.throughput.records_per_sec,
            current.throughput.records_per_sec
        )
    );
    println!(
        "  Bytes/sec:    {}/s → {}/s ({})",
        format_bytes(baseline.throughput.bytes_per_sec as u64),
        format_bytes(current.throughput.bytes_per_sec as u64),
        delta_pct(
            baseline.throughput.bytes_per_sec,
            current.throughput.bytes_per_sec
        )
    );
    println!();
    println!("LATENCY");
    println!(
        "  p50:  {} → {} ({})",
        format_number(baseline.latency.p50_us),
        format_number(current.latency.p50_us),
        delta_pct(
            baseline.latency.p50_us as f64,
            current.latency.p50_us as f64
        )
    );
    println!(
        "  p99:  {} → {} ({})",
        format_number(baseline.latency.p99_us),
        format_number(current.latency.p99_us),
        delta_pct(
            baseline.latency.p99_us as f64,
            current.latency.p99_us as f64
        )
    );
    println!(
        "  p999: {} → {} ({})",
        format_number(baseline.latency.p999_us),
        format_number(current.latency.p999_us),
        delta_pct(
            baseline.latency.p999_us as f64,
            current.latency.p999_us as f64
        )
    );
    println!();

    Ok(())
}

fn get_scenario_config(name: &str) -> Option<(usize, usize, u64)> {
    match name {
        "baseline" => Some((1, 1024, 10)),
        "small_messages" => Some((10, 100, 30)),
        "large_messages" => Some((5, 100_000, 30)),
        "high_fanout" => Some((50, 1024, 30)),
        "batching_stress" => Some((100, 1024, 30)),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Writer {
            writers,
            record_size,
            duration,
            scenario,
            output,
            save,
        } => {
            let (w, size, dur) = if let Some(ref scenario_name) = scenario {
                match get_scenario_config(scenario_name) {
                    Some(config) => config,
                    None => {
                        eprintln!("Unknown scenario: {}", scenario_name);
                        eprintln!(
                            "Available: baseline, small_messages, large_messages, high_fanout, batching_stress"
                        );
                        std::process::exit(1);
                    }
                }
            } else {
                (writers, record_size, duration)
            };

            let metrics = run_producer_benchmark(w, size, Duration::from_secs(dur)).await;

            match output.as_str() {
                "json" => {
                    let json = serde_json::to_string_pretty(&metrics).unwrap();
                    println!("{}", json);
                }
                _ => {
                    print_table(&metrics);
                }
            }

            if let Some(path) = save {
                let json = serde_json::to_string_pretty(&metrics).unwrap();
                let mut file = std::fs::File::create(&path).expect("Failed to create file");
                file.write_all(json.as_bytes()).expect("Failed to write");
                eprintln!("Results saved to {}", path);
            }
        }

        Commands::Remote {
            url,
            database_url,
            topic,
            writers,
            requests_per_writer,
            records_per_batch,
            record_size,
            max_in_flight,
            fetch_readers,
            max_bytes,
            skip_fetch,
        } => {
            let database_url = database_url
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .expect("--database-url or DATABASE_URL required");
            let report = remote::run(remote::RemoteConfig {
                url,
                database_url,
                topic,
                writers,
                requests_per_writer,
                records_per_batch,
                record_size,
                max_in_flight,
                fetch_readers,
                max_bytes,
                skip_fetch,
            })
            .await
            .expect("remote benchmark failed");
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }

        Commands::Report { compare, current } => {
            if let Err(e) = compare_reports(&compare, &current) {
                eprintln!("Error comparing reports: {}", e);
                std::process::exit(1);
            }
        }
    }
}
