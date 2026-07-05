// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Remote benchmark mode: drives a running broker over WebSocket.
//!
//! Produce phase: N writer connections, each pipelining appends with a
//! max-in-flight window. Fetch phase: R reader connections, each reading a
//! disjoint contiguous offset stripe.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

async fn connect_with_retry(
    url: &str,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let mut delay = Duration::from_millis(500);
    for attempt in 0..10 {
        match tokio::time::timeout(
            Duration::from_secs(10),
            connect_async_with_config(url, Some(ws_config()), false),
        )
        .await
        {
            Ok(Ok((ws, _))) => return Ok(ws),
            Ok(Err(e)) if attempt == 9 => return Err(e.into()),
            Err(_) if attempt == 9 => anyhow::bail!("connect timed out after retries"),
            _ => {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
    unreachable!()
}

fn ws_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(1024 * 1024 * 1024);
    config.max_frame_size = Some(1024 * 1024 * 1024);
    config
}

use flux_broker::fl::{Codec, FlReader, SegmentMeta};
use flux_common::ids::{AppendSeq, Offset, SchemaId, TopicId, WriterId};
use flux_common::types::{Record, RecordBatch};
use flux_wire::{
    ClientMessage, ERR_BACKPRESSURE, ServerMessage, decode_server_message, encode_client_message,
    reader, writer,
};

#[derive(Debug, Clone)]
pub struct RemoteConfig {
    pub url: String,
    pub database_url: String,
    pub topic: String,
    pub topics: u32,
    pub writers: usize,
    pub requests_per_writer: u64,
    pub records_per_batch: u32,
    pub record_size: usize,
    pub max_in_flight: usize,
    pub fetch_readers: usize,
    pub max_bytes: u32,
    pub raw_reads: bool,
    pub skip_fetch: bool,
    pub concurrent: bool,
}

#[derive(Debug, Serialize)]
pub struct RemoteReport {
    pub topic_ids: Vec<u32>,
    pub concurrent: bool,
    pub requests: u64,
    pub records: u64,
    pub payload_bytes: u64,
    pub produce_secs: f64,
    pub produce_req_rps: f64,
    pub produce_rec_rps: f64,
    pub produce_mib_per_sec: f64,
    pub produce_p50_ms: f64,
    pub produce_p99_ms: f64,
    pub fetch_secs: Option<f64>,
    pub fetch_rec_rps: Option<f64>,
    pub fetch_mib_per_sec: Option<f64>,
    pub fetch_p50_ms: Option<f64>,
    pub fetch_p99_ms: Option<f64>,
    /// Concurrent mode only: wall clock from first append to last fetch.
    pub e2e_secs: Option<f64>,
    /// Concurrent mode only: produced + fetched payload over e2e wall clock.
    pub combined_mib_per_sec: Option<f64>,
}

fn encode_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; capacity];
    let len = encode_client_message(&msg, &mut buf).expect("encode client frame");
    buf.truncate(len);
    buf
}

fn decode_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
}

async fn ensure_topic(database_url: &str, name: &str) -> anyhow::Result<u32> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    let existing: Option<i32> = sqlx::query_scalar("SELECT topic_id FROM topics WHERE name = $1")
        .bind(name)
        .fetch_optional(&pool)
        .await?;
    let id = match existing {
        Some(id) => id,
        None => {
            sqlx::query_scalar("INSERT INTO topics (name) VALUES ($1) RETURNING topic_id")
                .bind(name)
                .fetch_one(&pool)
                .await?
        }
    };
    Ok(id as u32)
}

pub async fn run(cfg: RemoteConfig) -> anyhow::Result<RemoteReport> {
    // Resolve/create the topic set. Writers spread round-robin, so require an
    // even split to keep per-topic record counts exact for verification.
    let num_topics = cfg.topics.max(1) as usize;
    anyhow::ensure!(
        cfg.writers % num_topics == 0,
        "writers ({}) must be a multiple of topics ({})",
        cfg.writers,
        num_topics
    );
    let mut topic_ids = Vec::with_capacity(num_topics);
    for i in 0..num_topics {
        let name = if num_topics == 1 {
            cfg.topic.clone()
        } else {
            format!("{}-{}", cfg.topic, i)
        };
        topic_ids.push(TopicId(ensure_topic(&cfg.database_url, &name).await?));
    }
    eprintln!(
        "remote bench: url={} topic={} topics={} writers={} req/writer={} rec/batch={} rec_size={} window={}",
        cfg.url,
        cfg.topic,
        num_topics,
        cfg.writers,
        cfg.requests_per_writer,
        cfg.records_per_batch,
        cfg.record_size,
        cfg.max_in_flight,
    );

    let requests = cfg.writers as u64 * cfg.requests_per_writer;
    let records = requests * cfg.records_per_batch as u64;
    let payload_bytes = records * cfg.record_size as u64;
    let records_per_topic = records / num_topics as u64;

    // ---- produce phase ----
    let produce_start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..cfg.writers {
        let cfg = cfg.clone();
        let topic_id = topic_ids[w % num_topics];
        handles.push(tokio::spawn(
            async move { run_writer(w, cfg, topic_id).await },
        ));
    }

    // Concurrent mode: readers tail the producers from offset 0 while they
    // write; the empty-read backoff in the fetch loops absorbs the
    // not-yet-produced gap. This is how Ursa-class benchmarks measure.
    let tailing_fetchers = if cfg.concurrent && !cfg.skip_fetch {
        spawn_fetchers(&cfg, &topic_ids, records_per_topic)
    } else {
        Vec::new()
    };

    let mut latency = Histogram::<u64>::new_with_bounds(1, 300_000_000, 3).unwrap();
    for h in handles {
        let writer_hist = h.await??;
        latency.add(&writer_hist).ok();
    }
    let produce_secs = produce_start.elapsed().as_secs_f64();

    // ---- fetch phase ----
    let mut e2e_secs = None;
    let (fetch_secs, fetch_rec_rps, fetch_mib, fetch_p50, fetch_p99) = if cfg.skip_fetch {
        (None, None, None, None, None)
    } else {
        let (fetch_start, fetch_handles) = if cfg.concurrent {
            (produce_start, tailing_fetchers)
        } else {
            let start = Instant::now();
            (start, spawn_fetchers(&cfg, &topic_ids, records_per_topic))
        };
        let mut seen = 0u64;
        let mut fetch_latency = Histogram::<u64>::new_with_bounds(1, 300_000_000, 3).unwrap();
        for h in fetch_handles {
            let (n, hist) = h.await??;
            seen += n;
            fetch_latency.add(&hist).ok();
        }
        anyhow::ensure!(seen == records, "fetch saw {seen} of {records} records");
        let secs = fetch_start.elapsed().as_secs_f64();
        if cfg.concurrent {
            e2e_secs = Some(produce_start.elapsed().as_secs_f64());
        }
        (
            Some(secs),
            Some(records as f64 / secs),
            Some(payload_bytes as f64 / secs / (1024.0 * 1024.0)),
            Some(fetch_latency.value_at_quantile(0.5) as f64 / 1000.0),
            Some(fetch_latency.value_at_quantile(0.99) as f64 / 1000.0),
        )
    };

    Ok(RemoteReport {
        topic_ids: topic_ids.iter().map(|t| t.0).collect(),
        concurrent: cfg.concurrent,
        requests,
        records,
        payload_bytes,
        produce_secs,
        produce_req_rps: requests as f64 / produce_secs,
        produce_rec_rps: records as f64 / produce_secs,
        produce_mib_per_sec: payload_bytes as f64 / produce_secs / (1024.0 * 1024.0),
        produce_p50_ms: latency.value_at_quantile(0.5) as f64 / 1000.0,
        produce_p99_ms: latency.value_at_quantile(0.99) as f64 / 1000.0,
        fetch_secs,
        fetch_rec_rps,
        fetch_mib_per_sec: fetch_mib,
        fetch_p50_ms: fetch_p50,
        fetch_p99_ms: fetch_p99,
        e2e_secs,
        combined_mib_per_sec: e2e_secs
            .map(|secs| 2.0 * payload_bytes as f64 / secs / (1024.0 * 1024.0)),
    })
}

/// Spawn reader connections spread across topics, striping within each.
/// Each returns (records seen, per-request round-trip latency histogram).
fn spawn_fetchers(
    cfg: &RemoteConfig,
    topic_ids: &[TopicId],
    records_per_topic: u64,
) -> Vec<tokio::task::JoinHandle<anyhow::Result<(u64, Histogram<u64>)>>> {
    let num_topics = topic_ids.len();
    let stripes_per_topic = (cfg.fetch_readers.max(1) / num_topics).max(1) as u64;
    let stripe = records_per_topic.div_ceil(stripes_per_topic);
    let mut handles = Vec::new();
    for &topic_id in topic_ids {
        for r in 0..stripes_per_topic {
            let start = r * stripe;
            let end = ((r + 1) * stripe).min(records_per_topic);
            if start >= end {
                break;
            }
            let url = cfg.url.clone();
            let max_bytes = cfg.max_bytes;
            let raw = cfg.raw_reads;
            handles.push(tokio::spawn(async move {
                if raw {
                    fetch_offset_range_raw(&url, topic_id, start, end, max_bytes).await
                } else {
                    fetch_offset_range(&url, topic_id, start, end, max_bytes).await
                }
            }));
        }
    }
    handles
}

async fn run_writer(
    index: usize,
    cfg: RemoteConfig,
    topic_id: TopicId,
) -> anyhow::Result<Histogram<u64>> {
    let mut ws = connect_with_retry(&cfg.url).await?;
    let writer_id = WriterId::new();

    // Incompressible payloads (xorshift PRNG), unique per record and per
    // request: keeps compressed size ~= decoded size so lease budgets and
    // wire numbers reflect real workloads instead of zstd flattering
    // repeated bytes.
    let mut prng_state =
        0x9E3779B97F4A7C15u64 ^ (index as u64).wrapping_mul(0xD1B54A32D192ED03) | 1;
    let mut fill_random = move |buf: &mut [u8]| {
        for chunk in buf.chunks_mut(8) {
            prng_state ^= prng_state << 13;
            prng_state ^= prng_state >> 7;
            prng_state ^= prng_state << 17;
            let bytes = prng_state.to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
    };
    let mut make_records = move || -> Vec<Record> {
        (0..cfg.records_per_batch)
            .map(|_| {
                let mut buf = vec![0u8; cfg.record_size];
                fill_random(&mut buf);
                Record {
                    key: None,
                    value: bytes::Bytes::from(buf),
                }
            })
            .collect()
    };

    struct InFlight {
        frame: Vec<u8>,
        started_at: Instant,
        retries: u32,
    }

    let mut latency = Histogram::<u64>::new_with_bounds(1, 300_000_000, 3).unwrap();
    let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
    let window = cfg.max_in_flight.max(1);
    let mut next_seq = 1u64;
    let mut acked = 0u64;
    let frame_capacity = cfg.record_size * cfg.records_per_batch as usize + 64 * 1024;

    while acked < cfg.requests_per_writer {
        while next_seq <= cfg.requests_per_writer && in_flight.len() < window {
            let req = writer::AppendRequest {
                writer_id,
                append_seq: AppendSeq(next_seq),
                batches: vec![RecordBatch {
                    topic_id,
                    schema_id: SchemaId(100),
                    records: make_records(),
                }],
            };
            let frame = encode_frame(ClientMessage::Append(req), frame_capacity);
            ws.send(Message::Binary(frame.clone())).await?;
            in_flight.insert(
                next_seq,
                InFlight {
                    frame,
                    started_at: Instant::now(),
                    retries: 0,
                },
            );
            next_seq += 1;
        }

        let msg = tokio::time::timeout(Duration::from_secs(60), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("append ack timeout"))?
            .ok_or_else(|| anyhow::anyhow!("connection closed"))??;
        let data = match msg {
            Message::Binary(data) => data,
            _ => continue,
        };
        let resp = match decode_frame(&data) {
            ServerMessage::Append(resp) => resp,
            other => anyhow::bail!("unexpected server message: {other:?}"),
        };

        let seq = resp.append_seq.0;
        if resp.success {
            if let Some(req) = in_flight.remove(&seq) {
                let _ = latency.record(req.started_at.elapsed().as_micros().max(1) as u64);
                acked += 1;
            }
            continue;
        }

        anyhow::ensure!(
            resp.error_code == ERR_BACKPRESSURE,
            "append failed: code={} msg={}",
            resp.error_code,
            resp.error_message
        );
        let req = in_flight
            .get_mut(&seq)
            .ok_or_else(|| anyhow::anyhow!("retry for unknown seq {seq}"))?;
        anyhow::ensure!(req.retries < 10, "backpressure retries exhausted");
        req.retries += 1;
        let backoff_ms = (2u64.pow(req.retries.min(8)) * 2).min(250);
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        let frame = req.frame.clone();
        ws.send(Message::Binary(frame)).await?;
    }

    ws.close(None).await.ok();
    Ok(latency)
}

async fn fetch_offset_range(
    url: &str,
    topic_id: TopicId,
    start: u64,
    end: u64,
    max_bytes: u32,
) -> anyhow::Result<(u64, Histogram<u64>)> {
    let mut ws = connect_with_retry(url).await?;
    let mut current = start;
    let mut seen = 0u64;
    let mut latency = Histogram::<u64>::new_with_bounds(1, 300_000_000, 3).unwrap();

    while current < end {
        let req = reader::ReadRequest {
            topic_id,
            offset: Offset(current),
            max_bytes,
        };
        let req_start = Instant::now();
        ws.send(Message::Binary(encode_frame(
            ClientMessage::Read(req),
            64 * 1024,
        )))
        .await?;

        let msg = tokio::time::timeout(Duration::from_secs(60), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("read timeout"))?
            .ok_or_else(|| anyhow::anyhow!("connection closed"))??;
        let data = match msg {
            Message::Binary(data) => data,
            _ => continue,
        };
        let resp = match decode_frame(&data) {
            ServerMessage::Read(resp) => resp,
            other => anyhow::bail!("unexpected server message: {other:?}"),
        };
        anyhow::ensure!(resp.success, "read failed: {}", resp.error_message);

        let mut progressed = false;
        for result in resp.results {
            let count = (result.records.len() as u64).min(end - current);
            if count > 0 {
                current += count;
                seen += count;
                progressed = true;
            }
            if current >= end {
                break;
            }
        }
        if progressed {
            // Only data-carrying reads count toward fetch latency; empty
            // tail-polls in concurrent mode would swamp the histogram.
            let _ = latency.record(req_start.elapsed().as_micros().max(1) as u64);
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    ws.close(None).await.ok();
    Ok((seen, latency))
}

/// Zero-copy fetch: request compressed FL segments and decode client-side.
async fn fetch_offset_range_raw(
    url: &str,
    topic_id: TopicId,
    start: u64,
    end: u64,
    max_bytes: u32,
) -> anyhow::Result<(u64, Histogram<u64>)> {
    let mut ws = connect_with_retry(url).await?;
    let mut current = start;
    let mut seen = 0u64;
    let mut latency = Histogram::<u64>::new_with_bounds(1, 300_000_000, 3).unwrap();

    while current < end {
        let req = reader::RawReadRequest {
            topic_id,
            offset: Offset(current),
            max_bytes,
        };
        let req_start = Instant::now();
        ws.send(Message::Binary(encode_frame(
            ClientMessage::RawRead(req),
            64 * 1024,
        )))
        .await?;

        let msg = tokio::time::timeout(Duration::from_secs(60), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("raw read timeout"))?
            .ok_or_else(|| anyhow::anyhow!("connection closed"))??;
        let data = match msg {
            Message::Binary(data) => data,
            _ => continue,
        };
        let resp = match decode_frame(&data) {
            ServerMessage::RawRead(resp) => resp,
            other => anyhow::bail!("unexpected server message: {other:?}"),
        };
        anyhow::ensure!(resp.success, "raw read failed: {}", resp.error_message);

        let mut progressed = false;
        for segment in resp.segments {
            // Client-side decode: decompress + parse the FL segment.
            let meta = SegmentMeta {
                topic_id: segment.topic_id,
                schema_id: segment.schema_id,
                start_offset: segment.start_offset,
                end_offset: segment.end_offset,
                record_count: 0,
                byte_offset: 0,
                byte_length: segment.payload.len() as u64,
                ingest_time: 0,
                compression: Codec::Zstd,
                crc32: segment.crc32,
            };
            let records = FlReader::read_segment(&segment.payload, &meta, true)
                .map_err(|e| anyhow::anyhow!("segment decode: {e}"))?;

            // The requested offset may fall mid-segment; skip the prefix,
            // clip at this reader's stripe end.
            let skip = current.saturating_sub(segment.start_offset.0) as usize;
            let available = records.len().saturating_sub(skip) as u64;
            let count = available.min(end - current);
            if count > 0 {
                current += count;
                seen += count;
                progressed = true;
            }
            if current >= end {
                break;
            }
        }
        if progressed {
            let _ = latency.record(req_start.elapsed().as_micros().max(1) as u64);
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    ws.close(None).await.ok();
    Ok((seen, latency))
}
