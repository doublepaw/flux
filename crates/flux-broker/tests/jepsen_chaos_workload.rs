// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired chaos workload: rotating faults during concurrent produce+consume.
//!
//! Producers encode values as `p{id}-s{seq}` for traceability.
//! Consumers poll continuously and record all observed values.
//! Faults rotate on a fixed cycle; all invariants checked after completion.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, DbBlocker, MultiBrokerCluster, OperationHistory, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Helpers (same as in jepsen_pipelined_flush, duplicated to keep test files independent)
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
// Fault rotation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum FaultPhase {
    None,
    S3Latency,
    TransientPutFailure,
    DbLock,
    Crash,
    Recovery,
}

const SHORT_PHASES: &[FaultPhase] = &[
    FaultPhase::None,
    FaultPhase::S3Latency,
    FaultPhase::TransientPutFailure,
    FaultPhase::DbLock,
    FaultPhase::Crash,
    FaultPhase::Recovery,
];

// ---------------------------------------------------------------------------
// Chaos workload core
// ---------------------------------------------------------------------------

struct ChaosConfig {
    num_producers: usize,
    num_consumers: usize,
    phase_duration: Duration,
}

async fn run_chaos_workload(cfg: ChaosConfig) {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("chaos").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let history = OperationHistory::shared();

    // Shared state for the current broker address (changes after crash+restart).
    let broker_addr = Arc::new(Mutex::new(broker.addr()));

    // Cancellation signal.
    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // --- Producers ---
    let mut producer_handles = vec![];
    for p in 0..cfg.num_producers {
        let history = history.clone();
        let addr = broker_addr.clone();
        let mut stop_rx = stop_tx.subscribe();

        producer_handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("p{}-s{}", p, seq);

                // Ensure connected.
                if ws.is_none() {
                    let current_addr = *addr.lock().await;
                    match connect_async(format!("ws://{}", current_addr)).await {
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
                    h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
                };

                match ws_produce(w, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                        history
                            .lock()
                            .await
                            .record_write_complete(idx, offset, true);
                    }
                    _ => {
                        history.lock().await.record_write_complete(idx, None, false);
                        ws = None; // reconnect
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // --- Consumers ---
    let mut consumer_handles = vec![];
    for c in 0..cfg.num_consumers {
        let history = history.clone();
        let addr = broker_addr.clone();
        let mut stop_rx = stop_tx.subscribe();

        consumer_handles.push(tokio::spawn(async move {
            let reader_id = format!("reader-{}", c);
            let mut next_offset = Offset(0);
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                if ws.is_none() {
                    let current_addr = *addr.lock().await;
                    match connect_async(format!("ws://{}", current_addr)).await {
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
                        next_offset = Offset(next_offset.0 + values.len() as u64);
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
        }));
    }

    // --- Fault rotation ---
    for phase in SHORT_PHASES {
        eprintln!("  [chaos] phase: {:?}", phase);
        match phase {
            FaultPhase::None | FaultPhase::Recovery => {
                broker.faulty_store().reset();
            }
            FaultPhase::S3Latency => {
                broker.faulty_store().set_put_delay_ms(200);
            }
            FaultPhase::TransientPutFailure => {
                broker.faulty_store().fail_put_transiently(2);
            }
            FaultPhase::DbLock => {
                let mut blocker = DbBlocker::new(db.pool.clone());
                blocker.block_topic(topic_id.0 as i32).await;
                tokio::time::sleep(cfg.phase_duration).await;
                blocker.unblock().await;
                continue; // skip the normal phase_duration sleep
            }
            FaultPhase::Crash => {
                broker.crash();
                tokio::time::sleep(Duration::from_millis(500)).await;
                broker.restart().await;
                *broker_addr.lock().await = broker.addr();
            }
        }
        tokio::time::sleep(cfg.phase_duration).await;
    }

    // --- Stop workers ---
    let _ = stop_tx.send(());
    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    // --- Final read for verification (paginated, broker returns max 10 batches per read) ---
    broker.faulty_store().reset();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(broker.addr()).await;
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
        h.record_read_complete(idx, all_values, high_watermark, true);
    }

    // --- Invariant checks ---
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
        .expect("INVARIANT: per-producer offsets should be monotonic");
    h.verify_write_write_causal()
        .expect("INVARIANT: write-write causal ordering");
    h.verify_poll_contiguity()
        .expect("INVARIANT: sequential reads don't skip offsets");
    h.verify_no_phantom_writes()
        .expect("INVARIANT: no unacked values in reads");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Short chaos workload: 15s, 2 producers, 1 consumer.
#[tokio::test]
async fn test_chaos_workload_short() {
    run_chaos_workload(ChaosConfig {
        num_producers: 2,
        num_consumers: 1,
        phase_duration: Duration::from_millis(2500),
    })
    .await;
}

/// Full chaos workload: 60s, 4 producers, 2 consumers.
#[ignore]
#[tokio::test]
async fn test_chaos_workload_full() {
    run_chaos_workload(ChaosConfig {
        num_producers: 4,
        num_consumers: 2,
        phase_duration: Duration::from_secs(10),
    })
    .await;
}

// ---------------------------------------------------------------------------
// Randomized fault mixing
// ---------------------------------------------------------------------------

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

#[derive(Debug, Clone, Copy)]
enum RandomFault {
    S3Latency(u64),
    TransientPutFailure(u32),
    DbLock,
    S3Partition,
    Crash,
    GetLatency(u64),
}

const ALL_FAULTS: &[RandomFault] = &[
    RandomFault::S3Latency(300),
    RandomFault::TransientPutFailure(3),
    RandomFault::DbLock,
    RandomFault::S3Partition,
    RandomFault::Crash,
    RandomFault::GetLatency(200),
];

struct RandomChaosConfig {
    num_producers: usize,
    num_consumers: usize,
    num_cycles: usize,
    cycle_duration: Duration,
    seed: u64,
}

async fn run_randomized_chaos_workload(cfg: RandomChaosConfig) {
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    eprintln!("  [randomized-chaos] seed={}", cfg.seed);

    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("random-chaos").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let history = OperationHistory::shared();
    let broker_addr = Arc::new(Mutex::new(broker.addr()));

    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // --- Producers ---
    let mut producer_handles = vec![];
    for p in 0..cfg.num_producers {
        let history = history.clone();
        let addr = broker_addr.clone();
        let mut stop_rx = stop_tx.subscribe();

        producer_handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("rp{}-s{}", p, seq);

                if ws.is_none() {
                    let current_addr = *addr.lock().await;
                    match connect_async(format!("ws://{}", current_addr)).await {
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
                    h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
                };

                match ws_produce(w, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                        history
                            .lock()
                            .await
                            .record_write_complete(idx, offset, true);
                    }
                    _ => {
                        history.lock().await.record_write_complete(idx, None, false);
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // --- Consumers ---
    let mut consumer_handles = vec![];
    for c in 0..cfg.num_consumers {
        let history = history.clone();
        let addr = broker_addr.clone();
        let mut stop_rx = stop_tx.subscribe();

        consumer_handles.push(tokio::spawn(async move {
            let reader_id = format!("rreader-{}", c);
            let mut next_offset = Offset(0);
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                if ws.is_none() {
                    let current_addr = *addr.lock().await;
                    match connect_async(format!("ws://{}", current_addr)).await {
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
                        next_offset = Offset(next_offset.0 + values.len() as u64);
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
        }));
    }

    // --- Randomized fault cycles ---
    for cycle in 0..cfg.num_cycles {
        // Pick 1-2 random faults
        let num_faults = rng.gen_range(1..=2usize);
        let mut chosen = Vec::with_capacity(num_faults);
        for _ in 0..num_faults {
            let idx = rng.gen_range(0..ALL_FAULTS.len());
            chosen.push(ALL_FAULTS[idx]);
        }

        eprintln!("  [randomized-chaos] cycle {}: {:?}", cycle, chosen);

        let mut db_blocker: Option<DbBlocker> = None;

        // Apply faults
        for fault in &chosen {
            match fault {
                RandomFault::S3Latency(ms) => {
                    broker.faulty_store().set_put_delay_ms(*ms);
                }
                RandomFault::TransientPutFailure(n) => {
                    broker.faulty_store().fail_put_transiently(*n);
                }
                RandomFault::DbLock => {
                    let mut blocker = DbBlocker::new(db.pool.clone());
                    blocker.block_topic(topic_id.0 as i32).await;
                    db_blocker = Some(blocker);
                }
                RandomFault::S3Partition => {
                    broker.faulty_store().partition_puts();
                }
                RandomFault::Crash => {
                    broker.crash();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    broker.restart().await;
                    *broker_addr.lock().await = broker.addr();
                }
                RandomFault::GetLatency(ms) => {
                    broker.faulty_store().set_get_delay_ms(*ms);
                }
            }
        }

        tokio::time::sleep(cfg.cycle_duration).await;

        // 25% chance of full recovery between cycles
        if rng.gen_range(0..4) == 0 {
            eprintln!("  [randomized-chaos] recovery pause");
            broker.faulty_store().reset();
            if let Some(mut blocker) = db_blocker.take() {
                blocker.unblock().await;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            // Always clear partition and DB lock between cycles
            broker.faulty_store().heal_partition();
            broker.faulty_store().set_put_delay_ms(0);
            broker.faulty_store().set_get_delay_ms(0);
            if let Some(mut blocker) = db_blocker.take() {
                blocker.unblock().await;
            }
        }
    }

    // --- Full recovery ---
    broker.faulty_store().reset();

    // --- Stop workers ---
    let _ = stop_tx.send(());
    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    // --- Final read ---
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(broker.addr()).await;
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
        h.record_read_complete(idx, all_values, high_watermark, true);
    }

    // --- Invariant checks ---
    let h = history.lock().await;
    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_watermark_monotonic()
        .expect("INVARIANT: watermark monotonic");
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
}

/// Short randomized chaos: 30s, 6 cycles of 5s.
#[tokio::test]
async fn test_randomized_chaos_short() {
    run_randomized_chaos_workload(RandomChaosConfig {
        num_producers: 2,
        num_consumers: 1,
        num_cycles: 6,
        cycle_duration: Duration::from_secs(5),
        seed: 42,
    })
    .await;
}

/// Full randomized chaos: 180s, 18 cycles of 10s.
#[ignore]
#[tokio::test]
async fn test_randomized_chaos_full() {
    run_randomized_chaos_workload(RandomChaosConfig {
        num_producers: 4,
        num_consumers: 2,
        num_cycles: 18,
        cycle_duration: Duration::from_secs(10),
        seed: 12345,
    })
    .await;
}

// ---------------------------------------------------------------------------
// Multi-broker soak test
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum MultiBrokerFault {
    S3Latency(usize, u64),
    TransientPut(usize, u32),
    S3Partition(usize),
    Crash(usize),
    GetLatency(usize, u64),
}

/// 2-minute multi-broker soak: 2 brokers, 6 producers, 3 consumers.
/// 12 cycles of 10s each. All fault types including per-broker S3 partition.
/// Full OperationHistory verification including WW causal ordering.
#[ignore]
#[tokio::test]
async fn test_soak_2min() {
    let seed = 99999u64;
    let mut rng = StdRng::seed_from_u64(seed);
    eprintln!("  [soak-2min] seed={}", seed);

    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("soak").await as u32);

    let mut cluster = MultiBrokerCluster::start(db.url(), 2).await;
    let history = OperationHistory::shared();
    let addrs = Arc::new(Mutex::new(cluster.addrs()));
    let (stop_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // --- Producers ---
    let mut producer_handles = vec![];
    for p in 0..6 {
        let history = history.clone();
        let addrs = addrs.clone();
        let mut stop_rx = stop_tx.subscribe();

        producer_handles.push(tokio::spawn(async move {
            let writer_id = WriterId::new();
            let mut seq = 0u64;
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                seq += 1;
                let val = format!("sp{}-s{}", p, seq);

                if ws.is_none() {
                    let all_addrs = addrs.lock().await;
                    let addr = all_addrs[p % all_addrs.len()];
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
                    h.record_write(writer_id, TopicId(0), Bytes::from(val.clone()))
                };

                match ws_produce(w, writer_id, seq, topic_id, &val).await {
                    Ok(resp) if resp.success => {
                        let offset = resp.append_acks.first().map(|a| Offset(a.start_offset.0));
                        history
                            .lock()
                            .await
                            .record_write_complete(idx, offset, true);
                    }
                    _ => {
                        history.lock().await.record_write_complete(idx, None, false);
                        ws = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    // --- Consumers ---
    let mut consumer_handles = vec![];
    for c in 0..3 {
        let history = history.clone();
        let addrs = addrs.clone();
        let mut stop_rx = stop_tx.subscribe();

        consumer_handles.push(tokio::spawn(async move {
            let reader_id = format!("sreader-{}", c);
            let mut next_offset = Offset(0);
            let mut ws: Option<Ws> = None;

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                if ws.is_none() {
                    let all_addrs = addrs.lock().await;
                    let addr = all_addrs[c % all_addrs.len()];
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
                        next_offset = Offset(next_offset.0 + values.len() as u64);
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
        }));
    }

    // --- 12 fault cycles of 10s each ---
    let all_multi_faults = [
        MultiBrokerFault::S3Latency(0, 300),
        MultiBrokerFault::S3Latency(1, 200),
        MultiBrokerFault::TransientPut(0, 3),
        MultiBrokerFault::S3Partition(0),
        MultiBrokerFault::S3Partition(1),
        MultiBrokerFault::Crash(0),
        MultiBrokerFault::Crash(1),
        MultiBrokerFault::GetLatency(0, 200),
    ];

    for cycle in 0..12 {
        let num_faults = rng.gen_range(1..=2usize);
        let mut chosen = Vec::with_capacity(num_faults);
        for _ in 0..num_faults {
            let idx = rng.gen_range(0..all_multi_faults.len());
            chosen.push(all_multi_faults[idx]);
        }

        eprintln!("  [soak-2min] cycle {}/12: {:?}", cycle + 1, chosen);

        for fault in &chosen {
            match fault {
                MultiBrokerFault::S3Latency(b, ms) => {
                    cluster.broker_store(*b).set_put_delay_ms(*ms);
                }
                MultiBrokerFault::TransientPut(b, n) => {
                    cluster.broker_store(*b).fail_put_transiently(*n);
                }
                MultiBrokerFault::S3Partition(b) => {
                    cluster.broker_store(*b).partition_puts();
                }
                MultiBrokerFault::Crash(b) => {
                    cluster.crash_broker(*b).await;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    cluster.restart_broker(*b).await;
                    *addrs.lock().await = cluster.addrs();
                }
                MultiBrokerFault::GetLatency(b, ms) => {
                    cluster.broker_store(*b).set_get_delay_ms(*ms);
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(10)).await;

        // Heal all faults between cycles
        for store in cluster.all_stores() {
            store.heal_partition();
            store.set_put_delay_ms(0);
            store.set_get_delay_ms(0);
        }

        // 20% chance of full recovery pause
        if rng.gen_range(0..5) == 0 {
            eprintln!("  [soak-2min] recovery pause");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // --- Full recovery ---
    for store in cluster.all_stores() {
        store.reset();
    }

    // --- Stop workers ---
    let _ = stop_tx.send(());
    for h in producer_handles {
        let _ = h.await;
    }
    for h in consumer_handles {
        let _ = h.await;
    }

    // --- Final read ---
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ws = ws_connect(cluster.broker(0).addr()).await;
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
        h.record_read_complete(idx, all_values, high_watermark, true);
    }

    // --- Invariant checks ---
    let h = history.lock().await;
    h.verify_acknowledged_writes_visible()
        .expect("INVARIANT: all acked writes visible");
    h.verify_unique_offsets()
        .expect("INVARIANT: no duplicate offsets");
    h.verify_watermark_monotonic()
        .expect("INVARIANT: watermark monotonic");
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

    let total_writes = h.writes.len();
    let successful_writes = h.writes.iter().filter(|w| w.success).count();
    eprintln!(
        "  [soak-2min] done: {}/{} writes succeeded",
        successful_writes, total_writes
    );
}
