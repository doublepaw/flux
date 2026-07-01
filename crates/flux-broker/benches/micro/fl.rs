//! FL encode/decode benchmarks.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box};
use flux_broker::{FlReader, FlWriter};
use flux_common::ids::{SchemaId, TopicId};
use flux_common::types::{Record, RecordBatch};

/// Create a batch with specified record count and value size.
fn make_segment(record_count: usize, value_size: usize) -> RecordBatch {
    RecordBatch {
        topic_id: TopicId(1),
        schema_id: SchemaId(100),
        records: (0..record_count)
            .map(|i| Record {
                key: Some(Bytes::from(format!("key-{:08}", i))),
                value: Bytes::from(vec![0xABu8; value_size]),
            })
            .collect(),
    }
}

/// Benchmark FL encoding with compression.
pub fn bench_fl_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("fl_encode");

    // Various record sizes
    for (record_count, value_size) in &[(100, 100), (100, 1024), (1000, 256), (100, 10_000)] {
        let batch = make_segment(*record_count, *value_size);
        let total_bytes = batch.records.iter().map(|r| r.value.len()).sum::<usize>();

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", format!("{}r_{}b", record_count, value_size)),
            &batch,
            |b, seg| {
                b.iter(|| {
                    let mut writer = FlWriter::new();
                    writer.add_segment(black_box(seg)).unwrap();
                    black_box(writer.finish())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark FL decoding (decompression + parsing).
pub fn bench_fl_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("fl_decode");

    for (record_count, value_size) in &[(100, 100), (100, 1024), (1000, 256), (100, 10_000)] {
        let batch = make_segment(*record_count, *value_size);

        // Pre-encode the batch
        let mut writer = FlWriter::new();
        writer.add_segment(&batch).unwrap();
        let file_bytes = writer.finish();

        // Parse footer first
        let metas = FlReader::read_footer(&file_bytes).unwrap();

        group.throughput(Throughput::Elements(*record_count as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", format!("{}r_{}b", record_count, value_size)),
            &(file_bytes, metas),
            |b, (data, metas)| {
                b.iter(|| {
                    black_box(FlReader::read_segment(black_box(data), &metas[0], true).unwrap())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark footer parsing only.
pub fn bench_fl_footer(c: &mut Criterion) {
    let mut group = c.benchmark_group("fl_footer");

    // Create file with multiple batches
    for segment_count in &[1, 10, 50] {
        let mut writer = FlWriter::new();
        for _i in 0..*segment_count {
            let batch = RecordBatch {
                topic_id: TopicId(1),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: None,
                    value: Bytes::from(vec![0u8; 100]),
                }],
            };
            writer.add_segment(&batch).unwrap();
        }
        let file_bytes = writer.finish();

        group.bench_with_input(
            BenchmarkId::new("batches", segment_count),
            &file_bytes,
            |b, data| b.iter(|| black_box(FlReader::read_footer(black_box(data)).unwrap())),
        );
    }

    group.finish();
}

/// Benchmark compression ratio across different data patterns.
pub fn bench_compression_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratio");

    // Repetitive data (compresses well)
    let repetitive_segment = RecordBatch {
        topic_id: TopicId(1),
        schema_id: SchemaId(100),
        records: (0..100)
            .map(|_| Record {
                key: None,
                value: Bytes::from("this is repetitive data ".repeat(40)),
            })
            .collect(),
    };

    // Random data (compresses poorly)
    let random_segment = RecordBatch {
        topic_id: TopicId(1),
        schema_id: SchemaId(100),
        records: (0..100)
            .map(|i| {
                // Pseudo-random but deterministic
                let seed = i as u8;
                Record {
                    key: None,
                    value: Bytes::from(
                        (0..1000)
                            .map(|j| seed.wrapping_add(j as u8))
                            .collect::<Vec<u8>>(),
                    ),
                }
            })
            .collect(),
    };

    let uncompressed_rep: usize = repetitive_segment
        .records
        .iter()
        .map(|r| r.value.len())
        .sum();
    let uncompressed_rand: usize = random_segment.records.iter().map(|r| r.value.len()).sum();

    group.throughput(Throughput::Bytes(uncompressed_rep as u64));
    group.bench_function("repetitive", |b| {
        b.iter(|| {
            let mut writer = FlWriter::new();
            writer.add_segment(black_box(&repetitive_segment)).unwrap();
            black_box(writer.finish())
        })
    });

    group.throughput(Throughput::Bytes(uncompressed_rand as u64));
    group.bench_function("random", |b| {
        b.iter(|| {
            let mut writer = FlWriter::new();
            writer.add_segment(black_box(&random_segment)).unwrap();
            black_box(writer.finish())
        })
    });

    group.finish();
}
