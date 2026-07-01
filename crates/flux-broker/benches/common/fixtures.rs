//! Benchmark data fixtures.

use bytes::Bytes;
use flux_common::ids::{SchemaId, TopicId};
use flux_common::types::{Record, RecordBatch};

use super::config::BenchmarkConfig;

/// Generate random bytes of specified length.
pub fn random_bytes(len: usize) -> Bytes {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = vec![0u8; len];
    rng.fill_bytes(&mut bytes);
    Bytes::from(bytes)
}

/// Generate a single record with random data.
pub fn generate_record(config: &BenchmarkConfig) -> Record {
    let key = if config.include_keys {
        Some(random_bytes(16))
    } else {
        None
    };
    Record {
        key,
        value: random_bytes(config.record_size),
    }
}

/// Generate a vector of records.
pub fn generate_records(config: &BenchmarkConfig) -> Vec<Record> {
    (0..config.record_count)
        .map(|_| generate_record(config))
        .collect()
}

/// Generate a single batch with random data.
pub fn generate_segment(
    topic_id: TopicId,
    schema_id: SchemaId,
    config: &BenchmarkConfig,
) -> RecordBatch {
    RecordBatch {
        topic_id,
        schema_id,
        records: generate_records(config),
    }
}

/// Generate multiple batches for a FL file.
pub fn generate_segments(config: &BenchmarkConfig) -> Vec<RecordBatch> {
    (0..config.segments_per_file)
        .map(|i| generate_segment(TopicId(1 + i as u32 % 8), SchemaId(100), config))
        .collect()
}

/// Pre-generate record data for deterministic benchmarks.
pub struct RecordFixture {
    pub records_1k: Vec<Record>,
    pub records_10k: Vec<Record>,
    pub small_records: Vec<Record>,
    pub large_records: Vec<Record>,
}

impl RecordFixture {
    pub fn new() -> Self {
        Self {
            records_1k: generate_records(&BenchmarkConfig {
                record_count: 1000,
                record_size: 1024,
                include_keys: true,
                segments_per_file: 1,
            }),
            records_10k: generate_records(&BenchmarkConfig {
                record_count: 10_000,
                record_size: 256,
                include_keys: true,
                segments_per_file: 1,
            }),
            small_records: generate_records(&BenchmarkConfig {
                record_count: 1000,
                record_size: 100,
                include_keys: false,
                segments_per_file: 1,
            }),
            large_records: generate_records(&BenchmarkConfig {
                record_count: 100,
                record_size: 100_000,
                include_keys: true,
                segments_per_file: 1,
            }),
        }
    }
}

impl Default for RecordFixture {
    fn default() -> Self {
        Self::new()
    }
}
