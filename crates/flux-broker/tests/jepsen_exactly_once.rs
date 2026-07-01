// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired exactly-once chaos test.
//!
//! Unlike `jepsen_chaos_workload` where producers/consumers are immortal (just
//! reconnect on error), this test **hard-kills** producers, consumers, and
//! brokers via `JoinHandle::abort()` and respawns them.
//!
//! Producers keep the same `WriterId` across incarnations and retry the last
//! unacked `AppendSeq`, exercising the three-level dedup path:
//!   1. In-flight coalescing (same broker, same connection)
//!   2. LRU cache hit (same broker, new connection)
//!   3. Postgres `writer_state` lookup (broker also crashed)
//!
//! Values are encoded as `p{id}-s{seq}` so a final scan can detect payload-level
//! duplicates even if the OperationHistory invariants pass.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::status::ERR_STALE_SEQUENCE;
use flux_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, MultiBrokerCluster, OperationHistory, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Helpers (duplicated per test file convention)
// ---------------------------------------------------------------------------

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

async fn ws_produce(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    value: &str,
) -> Result<writer::AppendResponse, String> {
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(seq),
        batches: vec![RecordBatch {
            topic_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from(value.to_string()),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(15), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_read(
    ws: &mut Ws,
    topic_id: TopicId,
    offset: Offset,
) -> Result<reader::ReadResponse, String> {
    let req = reader::ReadRequest {
        topic_id,
        offset,
        max_bytes: 1024 * 1024,
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flux_wire::ServerMessage::Read(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

// ---------------------------------------------------------------------------
// Producer / Consumer state that survives across task kills
// ---------------------------------------------------------------------------

struct ProducerState {
    writer_id: WriterId,
    next_seq: u64,
    /// If set, the previous incarnation was killed before receiving an ack.
    /// The new incarnation must retry this seq before advancing.
    last_unacked_seq: Option<u64>,
}

struct ConsumerState {
    next_offset: Offset,
}

// ---------------------------------------------------------------------------
// Shared spawn helpers
// ---------------------------------------------------------------------------

fn spawn_producer(
    p: usize,
    state: Arc<Mutex<ProducerState>>,
    broker_addr: Arc<Mutex<Vec<std::net::SocketAddr>>>,
    history: Arc<Mutex<OperationHistory>>,
    topic_id: TopicId,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ws: Option<Ws> = None;
        loop {
            // Determine which seq to send and whether this is a retry.
            let (writer_id, seq, val, is_retry) = {
                let mut s = state.lock().await;
                if let Some(unacked) = s.last_unacked_seq {
                    let val = format!("p{}-s{}", p, unacked);
                    (s.writer_id, unacked, val, true)
                } else {
                    let seq = s.next_seq;
                    s.last_unacked_seq = Some(seq);
                    let val = format!("p{}-s{}", p, seq);
                    (s.writer_id, seq, val, false)
                }
            };

            if ws.is_none() {
                let addrs = broker_addr.lock().await;
                let addr = addrs[p % addrs.len()];
                match connect_async(format!("ws://{}", addr)).await {
                    Ok((w, _)) => ws = Some(w),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
            }

            let w = ws.as_mut().unwrap();

            // Always record write attempts so verify_no_phantom_writes can
            // detect fabricated data. The original incarnation may have been
            // killed after setting last_unacked_seq but before record_write,
            // leaving the value untracked if we only record fresh writes.
            //
            // Only return the index for fresh writes — retries never call
            // record_write_complete because the dedup-acked offset is from
            // an earlier epoch, and recording it with a later timestamp
            // would create a false WW causal ordering violation.
            let history_idx = {
                let mut h = history.lock().await;
                let idx = h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()));
                if !is_retry { Some(idx) } else { None }
            };

            match ws_produce(w, writer_id, seq, topic_id, &val).await {
                Ok(resp) if resp.success => {
                    if let Some(idx) = history_idx {
                        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                        history
                            .lock()
                            .await
                            .record_write_complete(idx, offset, true);
                    }
                    let mut s = state.lock().await;
                    s.last_unacked_seq = None;
                    s.next_seq = seq + 1;
                }
                Ok(resp) if resp.error_code == ERR_STALE_SEQUENCE => {
                    // Already committed by a previous incarnation.
                    if let Some(idx) = history_idx {
                        history.lock().await.record_write_complete(idx, None, false);
                    }
                    let mut s = state.lock().await;
                    s.last_unacked_seq = None;
                    s.next_seq = seq + 1;
                }
                _ => {
                    if let Some(idx) = history_idx {
                        history.lock().await.record_write_complete(idx, None, false);
                    }
                    ws = None;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

fn spawn_consumer(
    c: usize,
    state: Arc<Mutex<ConsumerState>>,
    broker_addr: Arc<Mutex<Vec<std::net::SocketAddr>>>,
    history: Arc<Mutex<OperationHistory>>,
    topic_id: TopicId,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let reader_id = format!("eo-reader-{}", c);
        let mut ws: Option<Ws> = None;
        loop {
            let next_offset = state.lock().await.next_offset;

            if ws.is_none() {
                let addrs = broker_addr.lock().await;
                let addr = addrs[c % addrs.len()];
                match connect_async(format!("ws://{}", addr)).await {
                    Ok((w, _)) => ws = Some(w),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
            }

            let w = ws.as_mut().unwrap();
            let idx = {
                let mut h = history.lock().await;
                h.record_read(TopicId(0), reader_id.clone(), next_offset)
            };

            match ws_read(w, topic_id, next_offset).await {
                Ok(resp) if resp.success => {
                    let values: Vec<Bytes> = resp
                        .results
                        .iter()
                        .flat_map(|r| r.records.iter().map(|rec| rec.value.clone()))
                        .collect();
                    let hwm = resp
                        .results
                        .first()
                        .map(|r| r.high_watermark)
                        .unwrap_or(Offset(0));
                    let new_offset = Offset(next_offset.0 + values.len() as u64);
                    state.lock().await.next_offset = new_offset;
                    history
                        .lock()
                        .await
                        .record_read_complete(idx, values, hwm, true);
                }
                _ => {
                    history
                        .lock()
                        .await
                        .record_read_complete(idx, vec![], Offset(0), false);
                    ws = None;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

// ---------------------------------------------------------------------------
// Final read + verification (shared)
// ---------------------------------------------------------------------------

async fn final_read_and_verify(
    addr: std::net::SocketAddr,
    topic_id: TopicId,
    history: Arc<Mutex<OperationHistory>>,
) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(addr).await;
    let mut all_values = Vec::<Bytes>::new();
    let mut high_watermark = Offset(0);
    let mut next_offset = Offset(0);

    loop {
        let resp = ws_read(&mut ws, topic_id, next_offset)
            .await
            .expect("final paginated read should succeed");
        if !resp.success {
            break;
        }
        let mut got = false;
        for r in &resp.results {
            if r.high_watermark.0 > high_watermark.0 {
                high_watermark = r.high_watermark;
            }
            if !r.records.is_empty() {
                got = true;
                next_offset = Offset(next_offset.0 + r.records.len() as u64);
                all_values.extend(r.records.iter().map(|rec| rec.value.clone()));
            }
        }
        if !got || next_offset.0 >= high_watermark.0 {
            break;
        }
    }

    {
        let mut h = history.lock().await;
        let idx = h.record_read(TopicId(0), "final".to_string(), Offset(0));
        h.record_read_complete(idx, all_values.clone(), high_watermark, true);
    }

    let h = history.lock().await;

    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_watermark_monotonic()
        .expect("INVARIANT: watermark monotonic per reader");
    h.verify_no_duplicates()
        .expect("INVARIANT: no duplicate records");
    h.verify_monotonic_sends()
        .expect("INVARIANT: per-producer offsets monotonic");
    h.verify_write_write_causal()
        .expect("INVARIANT: write-write causal ordering");
    h.verify_poll_contiguity()
        .expect("INVARIANT: sequential reads don't skip offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no unacked values in reads");

    // Payload-level exactly-once check.
    let mut seen_payloads: HashSet<Vec<u8>> = HashSet::new();
    for value in &all_values {
        if !seen_payloads.insert(value.to_vec()) {
            panic!(
                "EXACTLY-ONCE VIOLATION: duplicate payload {:?} in committed log",
                String::from_utf8_lossy(value)
            );
        }
    }

    let total_writes = h.writes.len();
    let successful_writes = h.writes.iter().filter(|w| w.success).count();
    let failed_writes = total_writes - successful_writes;
    eprintln!(
        "  [exactly-once] done: {}/{} writes succeeded, {} failed, {} records in log",
        successful_writes,
        total_writes,
        failed_writes,
        all_values.len()
    );

    assert!(
        failed_writes > 0,
        "Expected some failed writes from producer kills, but all {} succeeded",
        total_writes
    );
}

// ---------------------------------------------------------------------------
// Chaos action picker
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ChaosAction {
    KillProducers(Vec<usize>),
    KillConsumers(Vec<usize>),
    CrashBroker,
    S3Latency(u64),
    TransientPutFailure(u32),
    S3Partition,
    /// Kill producers + crash broker simultaneously
    Combined(Vec<usize>),
}

fn pick_single_broker_action(
    rng: &mut StdRng,
    num_producers: usize,
    num_consumers: usize,
) -> ChaosAction {
    let roll: u32 = rng.gen_range(0..100);
    match roll {
        0..30 => {
            let count = rng.gen_range(1..=2.min(num_producers));
            let mut ids: Vec<usize> = (0..num_producers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            ChaosAction::KillProducers(ids)
        }
        30..45 => {
            let count = rng.gen_range(1..=2.min(num_consumers));
            let mut ids: Vec<usize> = (0..num_consumers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            ChaosAction::KillConsumers(ids)
        }
        45..60 => ChaosAction::CrashBroker,
        60..65 => ChaosAction::S3Latency(rng.gen_range(100..400)),
        65..70 => ChaosAction::TransientPutFailure(rng.gen_range(1..4)),
        70..75 => ChaosAction::S3Partition,
        _ => {
            let count = rng.gen_range(1..=2.min(num_producers));
            let mut ids: Vec<usize> = (0..num_producers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            ChaosAction::Combined(ids)
        }
    }
}

// ---------------------------------------------------------------------------
// Single-broker workload (matches existing passing chaos test pattern)
// ---------------------------------------------------------------------------

struct SingleBrokerConfig {
    num_producers: usize,
    num_consumers: usize,
    num_cycles: usize,
    cycle_duration: Duration,
    seed: u64,
}

async fn run_single_broker_workload(cfg: SingleBrokerConfig) {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    eprintln!("  [exactly-once] seed={}", cfg.seed);

    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("exactly-once").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let history = OperationHistory::shared();
    let broker_addr = Arc::new(Mutex::new(vec![broker.addr()]));

    let producer_states: Vec<Arc<Mutex<ProducerState>>> = (0..cfg.num_producers)
        .map(|_| {
            Arc::new(Mutex::new(ProducerState {
                writer_id: WriterId::new(),
                next_seq: 1,
                last_unacked_seq: None,
            }))
        })
        .collect();

    let consumer_states: Vec<Arc<Mutex<ConsumerState>>> = (0..cfg.num_consumers)
        .map(|_| {
            Arc::new(Mutex::new(ConsumerState {
                next_offset: Offset(0),
            }))
        })
        .collect();

    let mut producer_handles: Vec<Option<JoinHandle<()>>> =
        (0..cfg.num_producers).map(|_| None).collect();
    let mut consumer_handles: Vec<Option<JoinHandle<()>>> =
        (0..cfg.num_consumers).map(|_| None).collect();

    // Initial spawn.
    for p in 0..cfg.num_producers {
        producer_handles[p] = Some(spawn_producer(
            p,
            producer_states[p].clone(),
            broker_addr.clone(),
            history.clone(),
            topic_id,
        ));
    }
    for c in 0..cfg.num_consumers {
        consumer_handles[c] = Some(spawn_consumer(
            c,
            consumer_states[c].clone(),
            broker_addr.clone(),
            history.clone(),
            topic_id,
        ));
    }

    // Chaos cycles.
    for cycle in 0..cfg.num_cycles {
        let action = pick_single_broker_action(&mut rng, cfg.num_producers, cfg.num_consumers);
        eprintln!(
            "  [exactly-once] cycle {}/{}: {:?}",
            cycle + 1,
            cfg.num_cycles,
            action
        );

        match action {
            ChaosAction::KillProducers(ids) => {
                for &p in &ids {
                    if let Some(h) = producer_handles[p].take() {
                        h.abort();
                    }
                }
            }
            ChaosAction::KillConsumers(ids) => {
                for &c in &ids {
                    if let Some(h) = consumer_handles[c].take() {
                        h.abort();
                    }
                }
            }
            ChaosAction::CrashBroker => {
                broker.crash();
                tokio::time::sleep(Duration::from_millis(500)).await;
                broker.restart().await;
                *broker_addr.lock().await = vec![broker.addr()];
            }
            ChaosAction::S3Latency(ms) => {
                broker.faulty_store().set_put_delay_ms(ms);
            }
            ChaosAction::TransientPutFailure(n) => {
                broker.faulty_store().fail_put_transiently(n);
            }
            ChaosAction::S3Partition => {
                broker.faulty_store().partition_puts();
            }
            ChaosAction::Combined(producer_ids) => {
                for &p in &producer_ids {
                    if let Some(h) = producer_handles[p].take() {
                        h.abort();
                    }
                }
                broker.crash();
                tokio::time::sleep(Duration::from_millis(500)).await;
                broker.restart().await;
                *broker_addr.lock().await = vec![broker.addr()];
            }
        }

        tokio::time::sleep(cfg.cycle_duration).await;

        // Heal S3 faults between cycles.
        broker.faulty_store().heal_partition();
        broker.faulty_store().set_put_delay_ms(0);
        broker.faulty_store().set_get_delay_ms(0);

        // Respawn killed producers/consumers.
        for p in 0..cfg.num_producers {
            if producer_handles[p].is_none() {
                eprintln!("  [exactly-once]   respawning producer {}", p);
                producer_handles[p] = Some(spawn_producer(
                    p,
                    producer_states[p].clone(),
                    broker_addr.clone(),
                    history.clone(),
                    topic_id,
                ));
            }
        }
        for c in 0..cfg.num_consumers {
            if consumer_handles[c].is_none() {
                eprintln!("  [exactly-once]   respawning consumer {}", c);
                consumer_handles[c] = Some(spawn_consumer(
                    c,
                    consumer_states[c].clone(),
                    broker_addr.clone(),
                    history.clone(),
                    topic_id,
                ));
            }
        }
    }

    // Recovery + settle.
    broker.faulty_store().reset();
    eprintln!("  [exactly-once] settling...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop all workers.
    for h in &mut producer_handles {
        if let Some(handle) = h.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
    for h in &mut consumer_handles {
        if let Some(handle) = h.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    final_read_and_verify(broker.addr(), topic_id, history).await;
}

// ---------------------------------------------------------------------------
// Multi-broker workload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum MultiBrokerChaosAction {
    KillProducers(Vec<usize>),
    KillConsumers(Vec<usize>),
    CrashBrokers(Vec<usize>),
    S3Latency {
        broker: usize,
        ms: u64,
    },
    TransientPutFailure {
        broker: usize,
        count: u32,
    },
    S3Partition {
        broker: usize,
    },
    Combined {
        producers: Vec<usize>,
        brokers: Vec<usize>,
    },
}

fn pick_multi_broker_action(
    rng: &mut StdRng,
    num_producers: usize,
    num_consumers: usize,
    num_brokers: usize,
) -> MultiBrokerChaosAction {
    let roll: u32 = rng.gen_range(0..100);
    match roll {
        0..30 => {
            let count = rng.gen_range(1..=2.min(num_producers));
            let mut ids: Vec<usize> = (0..num_producers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            MultiBrokerChaosAction::KillProducers(ids)
        }
        30..45 => {
            let count = rng.gen_range(1..=2.min(num_consumers));
            let mut ids: Vec<usize> = (0..num_consumers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            MultiBrokerChaosAction::KillConsumers(ids)
        }
        45..60 => {
            let count = rng.gen_range(1..=2.min(num_brokers));
            let mut ids: Vec<usize> = (0..num_brokers).collect();
            ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            ids.truncate(count);
            MultiBrokerChaosAction::CrashBrokers(ids)
        }
        60..70 => {
            let broker = rng.gen_range(0..num_brokers);
            let kind: u32 = rng.gen_range(0..3);
            match kind {
                0 => MultiBrokerChaosAction::S3Latency {
                    broker,
                    ms: rng.gen_range(100..400),
                },
                1 => MultiBrokerChaosAction::TransientPutFailure {
                    broker,
                    count: rng.gen_range(1..4),
                },
                _ => MultiBrokerChaosAction::S3Partition { broker },
            }
        }
        _ => {
            let p_count = rng.gen_range(1..=2.min(num_producers));
            let mut p_ids: Vec<usize> = (0..num_producers).collect();
            p_ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            p_ids.truncate(p_count);

            let b_count = rng.gen_range(1..=2.min(num_brokers));
            let mut b_ids: Vec<usize> = (0..num_brokers).collect();
            b_ids.sort_by_key(|_| rng.gen_range(0..u32::MAX));
            b_ids.truncate(b_count);

            MultiBrokerChaosAction::Combined {
                producers: p_ids,
                brokers: b_ids,
            }
        }
    }
}

struct MultiBrokerConfig {
    num_producers: usize,
    num_consumers: usize,
    num_brokers: usize,
    num_cycles: usize,
    cycle_duration: Duration,
    seed: u64,
}

async fn run_multi_broker_workload(cfg: MultiBrokerConfig) {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    eprintln!(
        "  [exactly-once] seed={} brokers={}",
        cfg.seed, cfg.num_brokers
    );

    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("exactly-once").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), cfg.num_brokers).await;
    let history = OperationHistory::shared();
    let addrs = Arc::new(Mutex::new(cluster.addrs()));

    let producer_states: Vec<Arc<Mutex<ProducerState>>> = (0..cfg.num_producers)
        .map(|_| {
            Arc::new(Mutex::new(ProducerState {
                writer_id: WriterId::new(),
                next_seq: 1,
                last_unacked_seq: None,
            }))
        })
        .collect();

    let consumer_states: Vec<Arc<Mutex<ConsumerState>>> = (0..cfg.num_consumers)
        .map(|_| {
            Arc::new(Mutex::new(ConsumerState {
                next_offset: Offset(0),
            }))
        })
        .collect();

    let mut producer_handles: Vec<Option<JoinHandle<()>>> =
        (0..cfg.num_producers).map(|_| None).collect();
    let mut consumer_handles: Vec<Option<JoinHandle<()>>> =
        (0..cfg.num_consumers).map(|_| None).collect();

    for p in 0..cfg.num_producers {
        producer_handles[p] = Some(spawn_producer(
            p,
            producer_states[p].clone(),
            addrs.clone(),
            history.clone(),
            topic_id,
        ));
    }
    for c in 0..cfg.num_consumers {
        consumer_handles[c] = Some(spawn_consumer(
            c,
            consumer_states[c].clone(),
            addrs.clone(),
            history.clone(),
            topic_id,
        ));
    }

    for cycle in 0..cfg.num_cycles {
        let action = pick_multi_broker_action(
            &mut rng,
            cfg.num_producers,
            cfg.num_consumers,
            cfg.num_brokers,
        );
        eprintln!(
            "  [exactly-once] cycle {}/{}: {:?}",
            cycle + 1,
            cfg.num_cycles,
            action
        );

        match action {
            MultiBrokerChaosAction::KillProducers(ids) => {
                for &p in &ids {
                    if let Some(h) = producer_handles[p].take() {
                        h.abort();
                    }
                }
            }
            MultiBrokerChaosAction::KillConsumers(ids) => {
                for &c in &ids {
                    if let Some(h) = consumer_handles[c].take() {
                        h.abort();
                    }
                }
            }
            MultiBrokerChaosAction::CrashBrokers(ids) => {
                for &b in &ids {
                    cluster.crash_broker(b).await;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                for &b in &ids {
                    cluster.restart_broker(b).await;
                }
                *addrs.lock().await = cluster.addrs();
            }
            MultiBrokerChaosAction::S3Latency { broker, ms } => {
                cluster.broker_store(broker).set_put_delay_ms(ms);
            }
            MultiBrokerChaosAction::TransientPutFailure { broker, count } => {
                cluster.broker_store(broker).fail_put_transiently(count);
            }
            MultiBrokerChaosAction::S3Partition { broker } => {
                cluster.broker_store(broker).partition_puts();
            }
            MultiBrokerChaosAction::Combined { producers, brokers } => {
                for &p in &producers {
                    if let Some(h) = producer_handles[p].take() {
                        h.abort();
                    }
                }
                for &b in &brokers {
                    cluster.crash_broker(b).await;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                for &b in &brokers {
                    cluster.restart_broker(b).await;
                }
                *addrs.lock().await = cluster.addrs();
            }
        }

        tokio::time::sleep(cfg.cycle_duration).await;

        for store in cluster.all_stores() {
            store.heal_partition();
            store.set_put_delay_ms(0);
            store.set_get_delay_ms(0);
        }

        for p in 0..cfg.num_producers {
            if producer_handles[p].is_none() {
                eprintln!("  [exactly-once]   respawning producer {}", p);
                producer_handles[p] = Some(spawn_producer(
                    p,
                    producer_states[p].clone(),
                    addrs.clone(),
                    history.clone(),
                    topic_id,
                ));
            }
        }
        for c in 0..cfg.num_consumers {
            if consumer_handles[c].is_none() {
                eprintln!("  [exactly-once]   respawning consumer {}", c);
                consumer_handles[c] = Some(spawn_consumer(
                    c,
                    consumer_states[c].clone(),
                    addrs.clone(),
                    history.clone(),
                    topic_id,
                ));
            }
        }
    }

    for store in cluster.all_stores() {
        store.reset();
    }
    eprintln!("  [exactly-once] settling...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    for h in &mut producer_handles {
        if let Some(handle) = h.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
    for h in &mut consumer_handles {
        if let Some(handle) = h.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    final_read_and_verify(cluster.broker(0).addr(), topic_id, history).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Short exactly-once test: single broker, 4 producers, 3 consumers, 8 cycles.
/// Exercises producer kill/restart with same WriterId + dedup across incarnations.
#[tokio::test]
async fn test_exactly_once_short() {
    run_single_broker_workload(SingleBrokerConfig {
        num_producers: 4,
        num_consumers: 3,
        num_cycles: 8,
        cycle_duration: Duration::from_secs(3),
        seed: 77777,
    })
    .await;
}

/// Full exactly-once soak: 3 brokers, 4 producers, 3 consumers, 15 cycles.
/// Multi-broker combined chaos (producer kill + broker crash simultaneously).
#[ignore]
#[tokio::test]
async fn test_exactly_once_full() {
    run_multi_broker_workload(MultiBrokerConfig {
        num_producers: 4,
        num_consumers: 3,
        num_brokers: 3,
        num_cycles: 15,
        cycle_duration: Duration::from_secs(6),
        seed: 88888,
    })
    .await;
}
