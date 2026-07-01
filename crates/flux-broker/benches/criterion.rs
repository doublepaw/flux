//! Criterion benchmark entry point.
//!
//! Run with: cargo bench --bench criterion
//!
//! For database benchmarks, set DATABASE_URL environment variable:
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo bench --bench criterion

mod common;
mod db;
mod micro;

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

// Configure criterion for micro-benchmarks
fn micro_config() -> Criterion {
    Criterion::default()
        .sample_size(100)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1))
}

// Configure criterion for DB benchmarks (longer measurement time)
fn db_config() -> Criterion {
    Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(2))
}

// Micro-benchmark groups
criterion_group! {
    name = fl_benches;
    config = micro_config();
    targets =
        micro::fl::bench_fl_encode,
        micro::fl::bench_fl_decode,
        micro::fl::bench_fl_footer,
        micro::fl::bench_compression_ratio
}

criterion_group! {
    name = buffer_benches;
    config = micro_config();
    targets =
        micro::buffer::bench_buffer_insert,
        micro::buffer::bench_buffer_merge,
        micro::buffer::bench_buffer_drain,
        micro::buffer::bench_distribute_acks,
        micro::buffer::bench_multi_topic_insert
}

criterion_group! {
    name = wire_benches;
    config = micro_config();
    targets =
        micro::wire::bench_request_encode,
        micro::wire::bench_request_decode,
        micro::wire::bench_response_encode,
        micro::wire::bench_response_decode,
        micro::wire::bench_varint
}

// Database benchmark groups
criterion_group! {
    name = db_benches;
    config = db_config();
    targets =
        db::commit::bench_commit_batch,
        db::commit::bench_multi_topic_commit,
        db::commit::bench_batch_lookup
}

criterion_group! {
    name = coordinator_benches;
    config = db_config();
    targets =
        db::coordinator::bench_coordinator_join,
        db::coordinator::bench_coordinator_heartbeat,
        db::coordinator::bench_coordinator_commit,
        db::coordinator::bench_coordinator_reset
}

// Run all benchmarks
criterion_main!(
    fl_benches,
    buffer_benches,
    wire_benches,
    db_benches,
    coordinator_benches
);
