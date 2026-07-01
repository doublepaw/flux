//! Wire protocol encode/decode benchmarks.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box};
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{BatchAck, Record, RecordBatch};
use flux_wire::writer::{
    AppendRequest, AppendResponse, decode_request, decode_response, encode_request, encode_response,
};
use uuid::Uuid;

/// Create a AppendRequest with specified batch/record counts.
fn make_request(
    segment_count: usize,
    records_per_segment: usize,
    value_size: usize,
) -> AppendRequest {
    AppendRequest {
        writer_id: WriterId(Uuid::new_v4()),
        append_seq: AppendSeq(42),
        batches: (0..segment_count)
            .map(|i| RecordBatch {
                topic_id: TopicId(i as u32 + 1),
                schema_id: SchemaId(100),
                records: (0..records_per_segment)
                    .map(|j| Record {
                        key: Some(Bytes::from(format!("key-{}-{}", i, j))),
                        value: Bytes::from(vec![0xABu8; value_size]),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Create a AppendResponse with specified ack count.
fn make_response(ack_count: usize) -> AppendResponse {
    AppendResponse {
        append_seq: AppendSeq(42),
        success: true,
        error_code: 0,
        error_message: String::new(),
        append_acks: (0..ack_count)
            .map(|i| BatchAck {
                topic_id: TopicId(i as u32 + 1),
                schema_id: SchemaId(100),
                start_offset: Offset(i as u64 * 100),
                end_offset: Offset(i as u64 * 100 + 99),
            })
            .collect(),
    }
}

/// Benchmark AppendRequest encoding.
pub fn bench_request_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_request_encode");

    // Small request: 1 batch, 10 records, 100B values
    let small_req = make_request(1, 10, 100);
    let small_size: usize = small_req
        .batches
        .iter()
        .flat_map(|s| s.records.iter())
        .map(|r| r.value.len())
        .sum();

    // Medium request: 4 batches, 100 records each, 256B values
    let medium_req = make_request(4, 100, 256);
    let medium_size: usize = medium_req
        .batches
        .iter()
        .flat_map(|s| s.records.iter())
        .map(|r| r.value.len())
        .sum();

    // Large request: 8 batches, 500 records each, 1KB values
    let large_req = make_request(8, 500, 1024);
    let large_size: usize = large_req
        .batches
        .iter()
        .flat_map(|s| s.records.iter())
        .map(|r| r.value.len())
        .sum();

    group.throughput(Throughput::Bytes(small_size as u64));
    group.bench_with_input(BenchmarkId::new("size", "small"), &small_req, |b, req| {
        let mut buf = vec![0u8; 64 * 1024];
        b.iter(|| black_box(encode_request(black_box(req), &mut buf)))
    });

    group.throughput(Throughput::Bytes(medium_size as u64));
    group.bench_with_input(BenchmarkId::new("size", "medium"), &medium_req, |b, req| {
        let mut buf = vec![0u8; 512 * 1024];
        b.iter(|| black_box(encode_request(black_box(req), &mut buf)))
    });

    group.throughput(Throughput::Bytes(large_size as u64));
    group.bench_with_input(BenchmarkId::new("size", "large"), &large_req, |b, req| {
        let mut buf = vec![0u8; 8 * 1024 * 1024];
        b.iter(|| black_box(encode_request(black_box(req), &mut buf)))
    });

    group.finish();
}

/// Benchmark AppendRequest decoding.
pub fn bench_request_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_request_decode");

    for (label, segment_count, records_per_segment, value_size) in &[
        ("small", 1, 10, 100),
        ("medium", 4, 100, 256),
        ("large", 8, 500, 1024),
    ] {
        let req = make_request(*segment_count, *records_per_segment, *value_size);
        let total_records: usize = req.batches.iter().map(|s| s.records.len()).sum();

        // Encode it
        let mut buf = vec![0u8; 16 * 1024 * 1024];
        let len = encode_request(&req, &mut buf);
        let encoded = &buf[..len];

        group.throughput(Throughput::Elements(total_records as u64));
        group.bench_with_input(BenchmarkId::new("size", label), &encoded, |b, data| {
            b.iter(|| black_box(decode_request(black_box(data)).unwrap()))
        });
    }

    group.finish();
}

/// Benchmark AppendResponse encoding.
pub fn bench_response_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_response_encode");

    for ack_count in &[1, 8, 32, 128] {
        let resp = make_response(*ack_count);

        group.throughput(Throughput::Elements(*ack_count as u64));
        group.bench_with_input(
            BenchmarkId::new("append_acks", ack_count),
            &resp,
            |b, resp| {
                let mut buf = vec![0u8; 16 * 1024];
                b.iter(|| black_box(encode_response(black_box(resp), &mut buf)))
            },
        );
    }

    group.finish();
}

/// Benchmark AppendResponse decoding.
pub fn bench_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_response_decode");

    for ack_count in &[1, 8, 32, 128] {
        let resp = make_response(*ack_count);

        // Encode it
        let mut buf = vec![0u8; 16 * 1024];
        let len = encode_response(&resp, &mut buf);
        let encoded = &buf[..len];

        group.throughput(Throughput::Elements(*ack_count as u64));
        group.bench_with_input(
            BenchmarkId::new("append_acks", ack_count),
            &encoded,
            |b, data| b.iter(|| black_box(decode_response(black_box(data)).unwrap())),
        );
    }

    group.finish();
}

/// Benchmark varint encoding specifically.
pub fn bench_varint(c: &mut Criterion) {
    let mut group = c.benchmark_group("wire_varint");

    // Small values (1-byte varint)
    group.bench_function("encode_small", |b| {
        let mut buf = [0u8; 10];
        b.iter(|| black_box(flux_wire::varint::encode_u64(black_box(127), &mut buf)))
    });

    // Medium values (2-4 byte varint)
    group.bench_function("encode_medium", |b| {
        let mut buf = [0u8; 10];
        b.iter(|| {
            black_box(flux_wire::varint::encode_u64(
                black_box(100_000),
                &mut buf,
            ))
        })
    });

    // Large values (8+ byte varint)
    group.bench_function("encode_large", |b| {
        let mut buf = [0u8; 10];
        b.iter(|| {
            black_box(flux_wire::varint::encode_u64(
                black_box(u64::MAX),
                &mut buf,
            ))
        })
    });

    // Decode small
    group.bench_function("decode_small", |b| {
        let mut buf = [0u8; 10];
        let len = flux_wire::varint::encode_u64(127, &mut buf);
        let encoded = &buf[..len];
        b.iter(|| black_box(flux_wire::varint::decode_u64(black_box(encoded)).unwrap()))
    });

    // Decode large
    group.bench_function("decode_large", |b| {
        let mut buf = [0u8; 10];
        let len = flux_wire::varint::encode_u64(u64::MAX, &mut buf);
        let encoded = &buf[..len];
        b.iter(|| black_box(flux_wire::varint::decode_u64(black_box(encoded)).unwrap()))
    });

    group.finish();
}
