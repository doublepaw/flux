//! BrokerBuffer benchmarks.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box};
use flux_broker::{BrokerBuffer, BufferConfig};
use flux_common::ids::{AppendSeq, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use std::time::Duration;
use uuid::Uuid;

/// Create a batch with specified record count and value size.
fn make_segment(topic_id: u32, record_count: usize, value_size: usize) -> RecordBatch {
    RecordBatch {
        topic_id: TopicId(topic_id),
        schema_id: SchemaId(100),
        records: (0..record_count)
            .map(|_| Record {
                key: Some(Bytes::from(format!("{:016x}", rand::random::<u64>()))),
                value: Bytes::from(vec![0xABu8; value_size]),
            })
            .collect(),
    }
}

/// Benchmark single writer insert.
pub fn bench_buffer_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_insert");

    for record_count in &[1, 10, 100, 1000] {
        let batch = make_segment(1, *record_count, 256);
        let bytes: usize = batch.records.iter().map(|r| r.value.len() + 16).sum();

        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("records", record_count),
            &batch,
            |b, seg| {
                b.iter_batched(
                    || {
                        let config = BufferConfig {
                            max_size_bytes: 1024 * 1024 * 1024, // 1GB to avoid flush
                            max_wait: Duration::from_secs(3600),
                            high_water_bytes: 1024 * 1024 * 1024,
                            low_water_bytes: 512 * 1024 * 1024,
                        };
                        (BrokerBuffer::with_config(config), seg.clone())
                    },
                    |(mut buffer, seg)| {
                        let writer_id = WriterId(Uuid::new_v4());
                        drop(black_box(buffer.insert(writer_id, AppendSeq(1), vec![seg])));
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark batch merging (multiple producers to same topic).
pub fn bench_buffer_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_merge");

    for producer_count in &[2, 5, 10, 50] {
        let record_count = 10;
        let value_size = 256;

        group.bench_with_input(
            BenchmarkId::new("producers", producer_count),
            producer_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let config = BufferConfig {
                            max_size_bytes: 1024 * 1024 * 1024,
                            max_wait: Duration::from_secs(3600),
                            high_water_bytes: 1024 * 1024 * 1024,
                            low_water_bytes: 512 * 1024 * 1024,
                        };
                        let mut buffer = BrokerBuffer::with_config(config);

                        // Pre-insert producers except the last one
                        for i in 0..(count - 1) {
                            let writer_id = WriterId(Uuid::from_u128(i as u128));
                            let batch = make_segment(1, record_count, value_size);
                            drop(buffer.insert(writer_id, AppendSeq(1), vec![batch]));
                        }

                        let last_segment = make_segment(1, record_count, value_size);
                        (buffer, last_segment)
                    },
                    |(mut buffer, seg)| {
                        let writer_id = WriterId(Uuid::new_v4());
                        drop(black_box(buffer.insert(writer_id, AppendSeq(1), vec![seg])));
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark buffer drain operation.
pub fn bench_buffer_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_drain");

    for (producers, topics) in &[(10, 1), (10, 4), (50, 8), (100, 16)] {
        group.bench_with_input(
            BenchmarkId::new("config", format!("{}p_{}t", producers, topics)),
            &(*producers, *topics),
            |b, &(producer_count, topic_count)| {
                b.iter_batched(
                    || {
                        let config = BufferConfig {
                            max_size_bytes: 1024 * 1024 * 1024,
                            max_wait: Duration::from_secs(3600),
                            high_water_bytes: 1024 * 1024 * 1024,
                            low_water_bytes: 512 * 1024 * 1024,
                        };
                        let mut buffer = BrokerBuffer::with_config(config);

                        // Insert data from multiple producers
                        for i in 0..producer_count {
                            let writer_id = WriterId(Uuid::from_u128(i as u128));
                            let topic = (i % topic_count) as u32 + 1;
                            let batch = make_segment(topic, 10, 256);
                            drop(buffer.insert(writer_id, AppendSeq(1), vec![batch]));
                        }

                        buffer
                    },
                    |mut buffer| black_box(buffer.drain()),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark ack distribution after drain.
pub fn bench_distribute_acks(c: &mut Criterion) {
    let mut group = c.benchmark_group("distribute_acks");

    for producer_count in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("producers", producer_count),
            producer_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let config = BufferConfig {
                            max_size_bytes: 1024 * 1024 * 1024,
                            max_wait: Duration::from_secs(3600),
                            high_water_bytes: 1024 * 1024 * 1024,
                            low_water_bytes: 512 * 1024 * 1024,
                        };
                        let mut buffer = BrokerBuffer::with_config(config);
                        let mut receivers = Vec::with_capacity(count);

                        // Insert data from multiple producers (all to topic 1)
                        for i in 0..count {
                            let writer_id = WriterId(Uuid::from_u128(i as u128));
                            let batch = make_segment(1, 10, 256);
                            let rx = buffer.insert(writer_id, AppendSeq(1), vec![batch]);
                            receivers.push(rx);
                        }

                        let drain_result = buffer.drain();
                        // Fake offsets for the merged batch
                        let segment_offsets: Vec<(u64, u64)> = drain_result
                            .batches
                            .iter()
                            .scan(0u64, |offset, seg| {
                                let start = *offset;
                                let end = start + seg.records.len() as u64;
                                *offset = end;
                                Some((start, end))
                            })
                            .collect();

                        (drain_result, segment_offsets, receivers)
                    },
                    |(drain_result, segment_offsets, _receivers)| {
                        BrokerBuffer::distribute_acks(drain_result, &segment_offsets);
                        black_box(())
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark multi-topic insert (writer sends to multiple topics).
pub fn bench_multi_topic_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_topic_insert");

    for topic_count in &[1, 4, 8, 16] {
        let batches: Vec<RecordBatch> = (0..*topic_count)
            .map(|t| make_segment(t as u32 + 1, 10, 256))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("topics", topic_count),
            &batches,
            |b, segs| {
                b.iter_batched(
                    || {
                        let config = BufferConfig {
                            max_size_bytes: 1024 * 1024 * 1024,
                            max_wait: Duration::from_secs(3600),
                            high_water_bytes: 1024 * 1024 * 1024,
                            low_water_bytes: 512 * 1024 * 1024,
                        };
                        (BrokerBuffer::with_config(config), segs.clone())
                    },
                    |(mut buffer, segs)| {
                        let writer_id = WriterId(Uuid::new_v4());
                        black_box(buffer.insert(writer_id, AppendSeq(1), segs))
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}
