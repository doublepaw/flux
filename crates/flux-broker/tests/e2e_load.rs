// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! End-to-end load tests against a real flux-broker instance.
//!
//! These tests exercise the full WebSocket append/read path (no simulation).
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_load
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_load -- --ignored
//! ```

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{
    InMemorySpanExporter, InMemorySpanExporterBuilder, Sampler, SdkTracerProvider,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use flux_broker::buffer::BufferConfig;
use flux_broker::{BrokerConfig, BrokerState, LocalFsStore};
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{
    ClientMessage, ERR_BACKPRESSURE, ServerMessage, decode_server_message, encode_client_message,
    reader, writer,
};

use common::TestDb;

#[derive(Debug, Clone, Copy)]
struct LoadScenario {
    writers: usize,
    batches_per_producer: u32,
    records_per_batch: u32,
    payload_bytes: usize,
    max_in_flight: usize,
    fetch_timeout: Duration,
}

#[derive(Debug)]
struct WorkerResult {
    request_count: u64,
    record_count: u64,
    payload_bytes: u64,
    request_latency: Histogram<u64>,
}

#[derive(Debug, Clone)]
struct LoadRunResult {
    request_count: u64,
    record_count: u64,
    payload_bytes: u64,
    produce_elapsed: Duration,
    fetch_elapsed: Duration,
    total_elapsed: Duration,
    request_latency: Histogram<u64>,
}

#[derive(Debug, Clone)]
struct SpanStat {
    name: String,
    count: u64,
    total_ms: f64,
    avg_ms: f64,
    max_ms: f64,
}

#[derive(Clone)]
struct OTelCapture {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

static OTEL_CAPTURE: OnceLock<OTelCapture> = OnceLock::new();
static METRICS_REGISTERED: OnceLock<()> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default)]
struct HistogramSample {
    count: u64,
    sum_seconds: f64,
}

fn ensure_metrics_registered() {
    let _ = METRICS_REGISTERED.get_or_init(|| {
        flux_broker::metrics::register_metrics();
    });
}

fn hot_histogram_snapshot() -> HashMap<String, HistogramSample> {
    const EXTRA_HISTOGRAMS: &[&str] = &[
        "flux_append_enqueue_to_ack_seconds",
        "flux_dedup_check_latency_seconds",
        "flux_flush_queue_wait_seconds",
        "flux_flush_buffer_residency_seconds",
        "flux_flush_latency_seconds",
        "flux_flush_fl_build_seconds",
        "flux_flush_ack_distribute_seconds",
        "flux_db_commit_latency_seconds",
        "flux_db_commit_offsets_seconds",
        "flux_db_commit_topic_batches_seconds",
        "flux_db_commit_writer_state_prep_seconds",
        "flux_db_commit_writer_state_upsert_seconds",
        "flux_db_commit_tx_commit_seconds",
    ];

    let mut snapshot = HashMap::new();

    for family in flux_broker::metrics::REGISTRY.gather() {
        let name = family.get_name();
        if !name.starts_with("flux_ws_") && !EXTRA_HISTOGRAMS.contains(&name) {
            continue;
        }

        for metric in family.get_metric() {
            let histogram = metric.get_histogram();
            let count = histogram.get_sample_count();
            let sum_seconds = histogram.get_sample_sum();

            let labels = metric
                .get_label()
                .iter()
                .map(|label| format!("{}={}", label.get_name(), label.get_value()))
                .collect::<Vec<_>>()
                .join(",");
            let key = if labels.is_empty() {
                name.to_string()
            } else {
                format!("{name}{{{labels}}}")
            };

            snapshot.insert(key, HistogramSample { count, sum_seconds });
        }
    }

    snapshot
}

fn print_ws_hot_path_summary(before: &HashMap<String, HistogramSample>, top_n: usize) {
    let after = hot_histogram_snapshot();
    let mut rows = Vec::new();

    for (key, after_sample) in after {
        let before_sample = before.get(&key).copied().unwrap_or_default();
        let count_delta = after_sample.count.saturating_sub(before_sample.count);
        let sum_delta_seconds = (after_sample.sum_seconds - before_sample.sum_seconds).max(0.0);
        if count_delta == 0 || sum_delta_seconds <= 0.0 {
            continue;
        }

        let total_ms = sum_delta_seconds * 1000.0;
        let avg_ms = total_ms / count_delta as f64;
        rows.push((key, count_delta, total_ms, avg_ms));
    }

    rows.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    eprintln!("hot path histogram summary (top by total_ms):");
    for (key, count, total_ms, avg_ms) in rows.into_iter().take(top_n) {
        eprintln!(
            "  metric={} count={} total_ms={:.2} avg_ms={:.6}",
            key, count, total_ms, avg_ms
        );
    }
}

fn encode_client_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf_size = capacity.max(256);
    loop {
        let mut buf = vec![0u8; buf_size];
        match encode_client_message(&msg, &mut buf) {
            Ok(len) => {
                buf.truncate(len);
                return buf;
            }
            Err(flux_wire::EncodeError::BufferTooSmall { needed, .. }) => {
                buf_size = needed.max(buf_size.saturating_mul(2));
            }
            Err(e) => panic!("encode client frame: {e}"),
        }
    }
}

fn decode_server_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
}

fn decode_produce_response(data: &[u8]) -> writer::AppendResponse {
    match decode_server_frame(data) {
        ServerMessage::Append(resp) => resp,
        _ => panic!("expected append response"),
    }
}

fn decode_read_response(data: &[u8]) -> reader::ReadResponse {
    match decode_server_frame(data) {
        ServerMessage::Read(resp) => resp,
        _ => panic!("expected read response"),
    }
}

fn websocket_client_config() -> WebSocketConfig {
    WebSocketConfig {
        max_frame_size: Some(64 << 20),
        max_message_size: Some(64 << 20),
        ..Default::default()
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn init_otel_capture(sample_ratio: f64) -> &'static OTelCapture {
    OTEL_CAPTURE.get_or_init(|| {
        let exporter = InMemorySpanExporterBuilder::new().build();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::TraceIdRatioBased(sample_ratio))
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("flux-broker-e2e-load");

        // If another test already initialized a subscriber, continue without failing.
        let _ = tracing_subscriber::registry()
            .with(EnvFilter::new("off,flux_broker=debug"))
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init();

        OTelCapture { provider, exporter }
    })
}

fn summarize_spans(exporter: &InMemorySpanExporter, top_n: usize) -> Vec<SpanStat> {
    let spans = exporter
        .get_finished_spans()
        .expect("failed to retrieve finished spans");

    let mut agg: HashMap<String, (u64, u128, u128)> = HashMap::new();
    for span in spans {
        let elapsed = span
            .end_time
            .duration_since(span.start_time)
            .unwrap_or_default()
            .as_micros();
        let entry = agg.entry(span.name.into_owned()).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += elapsed;
        if elapsed > entry.2 {
            entry.2 = elapsed;
        }
    }

    let mut stats: Vec<SpanStat> = agg
        .into_iter()
        .map(|(name, (count, total_us, max_us))| SpanStat {
            name,
            count,
            total_ms: total_us as f64 / 1000.0,
            avg_ms: (total_us as f64 / count as f64) / 1000.0,
            max_ms: max_us as f64 / 1000.0,
        })
        .collect();

    stats.sort_by(|a, b| {
        b.total_ms
            .partial_cmp(&a.total_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    stats.truncate(top_n);
    stats
}

async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
    flush_interval: Duration,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval,
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
        #[cfg(feature = "iceberg")]
        iceberg: None,
    };

    let state = BrokerState::new(pool, store, config).await;
    let handle = tokio::spawn(async move {
        if let Err(e) = flux_broker::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    (addr, handle)
}

async fn run_producer_worker(
    url: &str,
    topic_id: TopicId,
    _producer_idx: usize,
    batches_per_producer: u32,
    records_per_batch: u32,
    payload_bytes: usize,
    max_in_flight: usize,
) -> WorkerResult {
    const MAX_PRODUCE_BATCH_BYTES: usize = 16 * 1024 * 1024;
    const APPROX_RECORD_WIRE_OVERHEAD_BYTES: usize = 8;

    let (mut ws, _) = connect_async_with_config(url, Some(websocket_client_config()), false)
        .await
        .expect("writer connect");
    let writer_id = WriterId::new();
    let payload = Bytes::from(vec![b'x'; payload_bytes]);
    let effective_record_bytes = payload_bytes
        .saturating_add(APPROX_RECORD_WIRE_OVERHEAD_BYTES)
        .max(1);
    let max_records_per_batch = (MAX_PRODUCE_BATCH_BYTES / effective_record_bytes).max(1) as u32;
    let records_per_batch = if records_per_batch > max_records_per_batch {
        eprintln!(
            "clamping records_per_batch from {} to {} to respect 16MiB batch budget",
            records_per_batch, max_records_per_batch
        );
        max_records_per_batch
    } else {
        records_per_batch
    };
    let template_records: Vec<Record> = (0..records_per_batch)
        .map(|_| Record {
            key: None,
            value: payload.clone(),
        })
        .collect();
    let window = max_in_flight.max(1);
    let max_retries = 8u32;

    #[derive(Clone)]
    struct InFlightProduce {
        frame: Vec<u8>,
        started_at: Instant,
        retries: u32,
    }

    let mut request_latency = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut in_flight = HashMap::<u64, InFlightProduce>::with_capacity(window.saturating_mul(2));
    let mut next_seq = 1u64;
    let mut acked = 0u64;

    while acked < batches_per_producer as u64 {
        while next_seq <= batches_per_producer as u64 && in_flight.len() < window {
            let append_seq = next_seq;
            let req = writer::AppendRequest {
                writer_id,
                append_seq: AppendSeq(append_seq),
                batches: vec![RecordBatch {
                    topic_id,
                    schema_id: SchemaId(100),
                    records: template_records.clone(),
                }],
            };

            let frame = encode_client_frame(ClientMessage::Append(req), 512 * 1024);
            ws.send(Message::Binary(frame.clone()))
                .await
                .expect("send append");

            in_flight.insert(
                append_seq,
                InFlightProduce {
                    frame,
                    started_at: Instant::now(),
                    retries: 0,
                },
            );
            next_seq += 1;
        }

        let response = tokio::time::timeout(Duration::from_secs(30), ws.next())
            .await
            .expect("timeout waiting for append response")
            .expect("no append response")
            .expect("websocket append error");

        let data = match response {
            Message::Binary(data) => data,
            _ => panic!("expected binary append response"),
        };

        let produce_resp = decode_produce_response(&data);
        let append_seq = produce_resp.append_seq.0;
        if produce_resp.success {
            assert_eq!(produce_resp.append_acks.len(), 1);
            let request = in_flight
                .remove(&append_seq)
                .expect("unexpected append response sequence");
            let latency_us = request.started_at.elapsed().as_micros().max(1) as u64;
            let _ = request_latency.record(latency_us);
            acked += 1;
            continue;
        }

        assert_eq!(
            produce_resp.error_code, ERR_BACKPRESSURE,
            "append failed unexpectedly: {:?}",
            produce_resp
        );
        let request = in_flight
            .get_mut(&append_seq)
            .expect("missing in-flight request for retry");
        assert!(
            request.retries < max_retries,
            "exhausted backpressure retries for append_seq {}",
            append_seq
        );

        request.retries += 1;
        let backoff_ms = (2u64.pow(request.retries.min(8)) * 2).min(250);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        ws.send(Message::Binary(request.frame.clone()))
            .await
            .expect("resend append");
    }

    assert!(
        in_flight.is_empty(),
        "writer completed with in-flight requests"
    );
    ws.close(None).await.ok();
    WorkerResult {
        request_count: batches_per_producer as u64,
        record_count: (batches_per_producer as u64) * (records_per_batch as u64),
        payload_bytes: (batches_per_producer as u64)
            .saturating_mul(records_per_batch as u64)
            .saturating_mul(payload_bytes as u64),
        request_latency,
    }
}

/// Read back all records using N parallel reader connections, each covering a
/// disjoint contiguous offset stripe (models a consumer fleet; the wire
/// protocol has no correlation IDs, so parallelism comes from connections,
/// not same-socket pipelining).
async fn fetch_all_records(url: &str, topic_id: TopicId, expected_total: u64, timeout: Duration) {
    let readers = env_usize("FLUX_LOAD_FETCH_READERS", 4).max(1) as u64;
    let stripe = expected_total.div_ceil(readers);

    let mut handles = Vec::new();
    for r in 0..readers {
        let start = r * stripe;
        let end = ((r + 1) * stripe).min(expected_total);
        if start >= end {
            break;
        }
        let url = url.to_string();
        handles.push(tokio::spawn(async move {
            fetch_offset_range(&url, topic_id, start, end, timeout).await
        }));
    }

    let mut seen_total = 0u64;
    for handle in handles {
        seen_total += handle.await.expect("fetch reader task");
    }

    assert_eq!(
        seen_total, expected_total,
        "read count mismatch after load run"
    );
}

/// Read records in [start, end) on one connection; returns records seen.
async fn fetch_offset_range(
    url: &str,
    topic_id: TopicId,
    start: u64,
    end: u64,
    timeout: Duration,
) -> u64 {
    let (mut ws, _) = connect_async_with_config(url, Some(websocket_client_config()), false)
        .await
        .expect("read connect");
    let mut current_offset = Offset(start);
    let mut seen_total = 0u64;

    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline && current_offset.0 < end {
        let req = reader::ReadRequest {
            topic_id,
            offset: current_offset,
            max_bytes: 16 * 1024 * 1024,
        };

        ws.send(Message::Binary(encode_client_frame(
            ClientMessage::Read(req),
            64 * 1024,
        )))
        .await
        .expect("send read");

        let response = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("timeout waiting for read response")
            .expect("no read response")
            .expect("websocket read error");

        let data = match response {
            Message::Binary(data) => data,
            _ => panic!("expected binary read response"),
        };

        let fetch_resp = decode_read_response(&data);
        assert!(fetch_resp.success, "read failed: {:?}", fetch_resp);

        let mut progressed = false;
        for result in fetch_resp.results {
            // Count only records inside this reader's stripe; responses may
            // extend past `end` since the server fills max_bytes.
            let count = (result.records.len() as u64).min(end - current_offset.0);
            if count > 0 {
                current_offset = Offset(current_offset.0 + count);
                seen_total += count;
                progressed = true;
            }
            if current_offset.0 >= end {
                break;
            }
        }

        if !progressed {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    ws.close(None).await.ok();
    seen_total
}

async fn run_load_scenario(scenario: LoadScenario, capture_otel: bool) -> LoadRunResult {
    ensure_metrics_registered();
    let ws_hist_before = hot_histogram_snapshot();

    let db = TestDb::new().await;
    let topic_id = db.create_topic("load-test").await;
    let temp_dir = TempDir::new().unwrap();
    let (addr, _server_handle) =
        start_server(db.pool.clone(), &temp_dir, Duration::from_millis(20)).await;
    let url = format!("ws://{}", addr);
    let topic_id = TopicId(topic_id as u32);
    let otel = if capture_otel {
        let sample_ratio = env_f64("FLUX_OTEL_SAMPLE_RATIO", 0.001);
        let capture = init_otel_capture(sample_ratio);
        capture.exporter.reset();
        Some(capture.clone())
    } else {
        None
    };

    let produce_start = Instant::now();
    let mut handles = Vec::with_capacity(scenario.writers);
    for producer_idx in 0..scenario.writers {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            run_producer_worker(
                &url,
                topic_id,
                producer_idx,
                scenario.batches_per_producer,
                scenario.records_per_batch,
                scenario.payload_bytes,
                scenario.max_in_flight,
            )
            .await
        }));
    }

    let mut request_count = 0u64;
    let mut record_count = 0u64;
    let mut payload_bytes = 0u64;
    let mut request_latency = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for handle in handles {
        let worker = handle.await.expect("writer task failed");
        request_count += worker.request_count;
        record_count += worker.record_count;
        payload_bytes += worker.payload_bytes;
        request_latency
            .add(&worker.request_latency)
            .expect("failed to merge writer latency histograms");
    }
    let produce_elapsed = produce_start.elapsed();

    // Allow final flush interval tick to fire before full read.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let fetch_start = Instant::now();
    fetch_all_records(&url, topic_id, record_count, scenario.fetch_timeout).await;
    let fetch_elapsed = fetch_start.elapsed();
    let total_elapsed = produce_elapsed + fetch_elapsed;

    if let Some(otel) = &otel {
        let _ = otel.provider.force_flush();
        let summary = summarize_spans(&otel.exporter, 12);
        eprintln!("otel span summary (top by total time):");
        for stat in summary {
            eprintln!(
                "  span={} count={} total_ms={:.2} avg_ms={:.4} max_ms={:.2}",
                stat.name, stat.count, stat.total_ms, stat.avg_ms, stat.max_ms
            );
        }
    }
    print_ws_hot_path_summary(&ws_hist_before, 30);

    let produce_secs = produce_elapsed.as_secs_f64().max(f64::EPSILON);
    let payload_bps = payload_bytes as f64 / produce_secs;
    let payload_mibps = payload_bps / (1024.0 * 1024.0);
    eprintln!(
        "load scenario complete: requests={} records={} payload_bytes={} produce_time={:?} fetch_time={:?} req_rps={:.0} rec_rps={:.0} payload_Bps={:.0} payload_MiBps={:.2} req_p50_ms={:.3} req_p95_ms={:.3} req_p99_ms={:.3}",
        request_count,
        record_count,
        payload_bytes,
        produce_elapsed,
        fetch_elapsed,
        request_count as f64 / produce_secs,
        record_count as f64 / produce_secs,
        payload_bps,
        payload_mibps,
        request_latency.value_at_quantile(0.50) as f64 / 1000.0,
        request_latency.value_at_quantile(0.95) as f64 / 1000.0,
        request_latency.value_at_quantile(0.99) as f64 / 1000.0,
    );

    LoadRunResult {
        request_count,
        record_count,
        payload_bytes,
        produce_elapsed,
        fetch_elapsed,
        total_elapsed,
        request_latency,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_load_smoke() {
    let scenario = LoadScenario {
        writers: 8,
        batches_per_producer: 10,
        records_per_batch: 20,
        payload_bytes: 128,
        max_in_flight: 8,
        fetch_timeout: Duration::from_secs(45),
    };

    let overall_timeout = scenario.fetch_timeout + Duration::from_secs(30);
    let result = tokio::time::timeout(overall_timeout, run_load_scenario(scenario, false))
        .await
        .expect("load test timed out");

    let records_per_sec = result.record_count as f64 / result.total_elapsed.as_secs_f64();
    if let Ok(min_rps) = std::env::var("FLUX_LOAD_MIN_RPS") {
        let min_rps: f64 = min_rps.parse().expect("invalid FLUX_LOAD_MIN_RPS");
        assert!(
            records_per_sec >= min_rps,
            "records_per_sec {:.2} below minimum {:.2}",
            records_per_sec,
            min_rps
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "long-running sustained load test; run explicitly"]
async fn test_e2e_load_sustained() {
    let scenario = LoadScenario {
        writers: 32,
        batches_per_producer: 40,
        records_per_batch: 50,
        payload_bytes: 256,
        max_in_flight: 32,
        fetch_timeout: Duration::from_secs(120),
    };

    let overall_timeout = scenario.fetch_timeout + Duration::from_secs(30);
    let result = tokio::time::timeout(overall_timeout, run_load_scenario(scenario, false))
        .await
        .expect("sustained load test timed out");

    let records_per_sec = result.record_count as f64 / result.total_elapsed.as_secs_f64();
    let bytes_per_sec = result.payload_bytes as f64 / result.total_elapsed.as_secs_f64();
    eprintln!(
        "sustained load throughput: {:.0} records/sec {:.0} bytes/sec ({:.2} MiB/s) ({} records {} bytes in {:?}) req_p99_ms={:.3}",
        records_per_sec,
        bytes_per_sec,
        bytes_per_sec / (1024.0 * 1024.0),
        result.record_count,
        result.payload_bytes,
        result.total_elapsed,
        result.request_latency.value_at_quantile(0.99) as f64 / 1000.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "1M request stress test with OTEL summary; run explicitly"]
async fn test_e2e_load_one_million_requests() {
    let writers = env_usize("FLUX_LOAD_PRODUCERS", 40);
    let batches_per_producer = env_u32("FLUX_LOAD_BATCHES_PER_PRODUCER", 25_000);
    let records_per_batch = env_u32("FLUX_LOAD_RECORDS_PER_BATCH", 128);
    let payload_bytes = env_usize("FLUX_LOAD_PAYLOAD_BYTES", 32);
    let max_in_flight = env_usize("FLUX_LOAD_MAX_IN_FLIGHT", 64);
    let fetch_timeout = Duration::from_secs(env_u64("FLUX_LOAD_FETCH_TIMEOUT_SECS", 900));
    let enable_otel = std::env::var("FLUX_LOAD_ENABLE_OTEL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let scenario = LoadScenario {
        writers,
        batches_per_producer,
        records_per_batch,
        payload_bytes,
        max_in_flight,
        fetch_timeout,
    };

    let expected_requests = (writers as u64) * (batches_per_producer as u64);
    eprintln!(
        "starting 1M-request scenario: writers={} batches_per_producer={} total_requests={} records_per_batch={} payload_bytes={} max_in_flight={} otel={}",
        writers,
        batches_per_producer,
        expected_requests,
        records_per_batch,
        payload_bytes,
        max_in_flight,
        enable_otel
    );

    let overall_timeout = fetch_timeout + Duration::from_secs(120);
    let result = tokio::time::timeout(overall_timeout, run_load_scenario(scenario, enable_otel))
        .await
        .expect("1M request load test timed out");

    let req_rps = result.request_count as f64 / result.produce_elapsed.as_secs_f64();
    let rec_rps = result.record_count as f64 / result.produce_elapsed.as_secs_f64();
    let bytes_per_sec = result.payload_bytes as f64 / result.produce_elapsed.as_secs_f64();
    eprintln!(
        "1M-request run complete: requests={} records={} payload_bytes={} produce_elapsed={:?} fetch_elapsed={:?} total_elapsed={:?} req_rps={:.0} rec_rps={:.0} payload_Bps={:.0} payload_MiBps={:.2} req_p50_ms={:.3} req_p95_ms={:.3} req_p99_ms={:.3}",
        result.request_count,
        result.record_count,
        result.payload_bytes,
        result.produce_elapsed,
        result.fetch_elapsed,
        result.total_elapsed,
        req_rps,
        rec_rps,
        bytes_per_sec,
        bytes_per_sec / (1024.0 * 1024.0),
        result.request_latency.value_at_quantile(0.50) as f64 / 1000.0,
        result.request_latency.value_at_quantile(0.95) as f64 / 1000.0,
        result.request_latency.value_at_quantile(0.99) as f64 / 1000.0
    );
}
