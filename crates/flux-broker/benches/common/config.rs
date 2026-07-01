//! Benchmark configuration.

use std::time::Duration;

/// Configuration for benchmark runs.
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of records to generate for benchmarks.
    pub record_count: usize,
    /// Size of record values in bytes.
    pub record_size: usize,
    /// Number of batches per FL file.
    pub segments_per_file: usize,
    /// Whether to include keys in records.
    pub include_keys: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            record_count: 1000,
            record_size: 1024,
            segments_per_file: 10,
            include_keys: true,
        }
    }
}

impl BenchmarkConfig {
    /// Small messages benchmark profile.
    pub fn small_messages() -> Self {
        Self {
            record_count: 10_000,
            record_size: 100,
            segments_per_file: 10,
            include_keys: true,
        }
    }

    /// Large messages benchmark profile.
    pub fn large_messages() -> Self {
        Self {
            record_count: 100,
            record_size: 100_000,
            segments_per_file: 4,
            include_keys: true,
        }
    }

    /// High record count benchmark profile.
    pub fn high_count() -> Self {
        Self {
            record_count: 100_000,
            record_size: 256,
            segments_per_file: 8,
            include_keys: false,
        }
    }
}

/// E2E benchmark scenario configuration.
#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub name: &'static str,
    pub producers: usize,
    pub record_size: usize,
    pub duration: Duration,
}

impl ScenarioConfig {
    /// Single writer baseline.
    pub fn baseline() -> Self {
        Self {
            name: "baseline",
            producers: 1,
            record_size: 1024,
            duration: Duration::from_secs(10),
        }
    }

    /// High-rate small messages.
    pub fn small_messages() -> Self {
        Self {
            name: "small_messages",
            producers: 10,
            record_size: 100,
            duration: Duration::from_secs(30),
        }
    }

    /// Large payload throughput.
    pub fn large_messages() -> Self {
        Self {
            name: "large_messages",
            producers: 5,
            record_size: 100_000,
            duration: Duration::from_secs(30),
        }
    }

    /// Many producers.
    pub fn high_fanout() -> Self {
        Self {
            name: "high_fanout",
            producers: 50,
            record_size: 1024,
            duration: Duration::from_secs(30),
        }
    }

    /// Force batch merging.
    pub fn batching_stress() -> Self {
        Self {
            name: "batching_stress",
            producers: 100,
            record_size: 1024,
            duration: Duration::from_secs(30),
        }
    }
}
