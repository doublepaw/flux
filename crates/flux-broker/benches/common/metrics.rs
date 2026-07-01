//! Benchmark metrics collection and reporting.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Throughput metrics for writer/reader benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputMetrics {
    pub records_per_sec: f64,
    pub bytes_per_sec: f64,
    pub total_records: u64,
    pub total_bytes: u64,
    pub duration_secs: f64,
}

impl ThroughputMetrics {
    pub fn new(records: u64, bytes: u64, duration: Duration) -> Self {
        let secs = duration.as_secs_f64();
        Self {
            records_per_sec: records as f64 / secs,
            bytes_per_sec: bytes as f64 / secs,
            total_records: records,
            total_bytes: bytes,
            duration_secs: secs,
        }
    }
}

/// Latency metrics with percentiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetrics {
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: f64,
    pub sample_count: u64,
}

impl LatencyMetrics {
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        Self {
            p50_us: hist.value_at_quantile(0.50),
            p99_us: hist.value_at_quantile(0.99),
            p999_us: hist.value_at_quantile(0.999),
            min_us: hist.min(),
            max_us: hist.max(),
            mean_us: hist.mean(),
            sample_count: hist.len(),
        }
    }
}

/// Batching efficiency metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchingMetrics {
    pub records_per_batch: f64,
    pub compression_ratio: f64,
    pub batches_flushed: u64,
    pub total_records: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

/// Complete benchmark results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub name: String,
    pub git_commit: Option<String>,
    pub timestamp: String,
    pub throughput: Option<ThroughputMetrics>,
    pub latency: Option<LatencyMetrics>,
    pub batching: Option<BatchingMetrics>,
}

impl BenchmarkResults {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            git_commit: get_git_commit(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            throughput: None,
            latency: None,
            batching: None,
        }
    }

    pub fn with_throughput(mut self, throughput: ThroughputMetrics) -> Self {
        self.throughput = Some(throughput);
        self
    }

    pub fn with_latency(mut self, latency: LatencyMetrics) -> Self {
        self.latency = Some(latency);
        self
    }

    pub fn with_batching(mut self, batching: BatchingMetrics) -> Self {
        self.batching = Some(batching);
        self
    }
}

/// Latency tracker using HDR histogram.
pub struct LatencyTracker {
    histogram: Histogram<u64>,
}

impl LatencyTracker {
    /// Create a new latency tracker.
    /// Range is 1 microsecond to 10 seconds with 3 significant digits.
    pub fn new() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(1, 10_000_000, 3).unwrap(),
        }
    }

    /// Record a latency sample.
    pub fn record(&mut self, start: Instant) {
        let elapsed = start.elapsed().as_micros() as u64;
        let _ = self.histogram.record(elapsed);
    }

    /// Record a latency value directly in microseconds.
    pub fn record_us(&mut self, us: u64) {
        let _ = self.histogram.record(us);
    }

    /// Get metrics from the histogram.
    pub fn metrics(&self) -> LatencyMetrics {
        LatencyMetrics::from_histogram(&self.histogram)
    }

    /// Reset the histogram.
    pub fn reset(&mut self) {
        self.histogram.reset();
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
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

/// Format bytes for human-readable output.
pub fn format_bytes(bytes: u64) -> String {
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

/// Format a number with thousands separators.
pub fn format_number(n: u64) -> String {
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
