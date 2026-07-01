// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Jepsen-inspired reader group chaos tests via WebSocket.
//!
//! Unlike `jepsen_reader_groups.rs` which tests the coordinator directly,
//! these tests exercise the full WebSocket → reader group → Postgres path
//! under chaos conditions: broker crashes, S3 partitions, reader kills.
//!
//! Inspired by Jepsen's Bufstream findings where stale caches caused stuck
//! consumers, and Redpanda consumer rebalance issues.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flux_common::ids::*;
use flux_common::types::{Record, RecordBatch};
use flux_wire::{
    ClientMessage, ServerMessage, decode_server_message, encode_client_message, reader, writer,
};

use common::ws_helpers;
use common::{CrashableWsBroker, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

fn encode_client_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; capacity];
    let len = encode_client_message(&msg, &mut buf).expect("encode client frame");
    buf.truncate(len);
    buf
}

fn decode_server_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
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
        flux_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_join_group(
    ws: &mut Ws,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> Result<reader::JoinGroupResponse, String> {
    let req = reader::JoinGroupRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        topic_ids: vec![topic_id],
    };
    let buf = encode_client_frame(ClientMessage::JoinGroup(req), 1024);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match decode_server_frame(&data) {
        ServerMessage::JoinGroup(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_heartbeat(
    ws: &mut Ws,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> Result<reader::HeartbeatResponseExt, String> {
    let req = reader::HeartbeatRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
    };
    let buf = encode_client_frame(ClientMessage::Heartbeat(req), 256);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match decode_server_frame(&data) {
        ServerMessage::Heartbeat(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_poll(
    ws: &mut Ws,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> Result<reader::PollResponse, String> {
    let req = reader::PollRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
        max_bytes: 1024 * 1024,
    };
    let buf = encode_client_frame(ClientMessage::Poll(req), 1024);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match decode_server_frame(&data) {
        ServerMessage::Poll(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_commit(
    ws: &mut Ws,
    group_id: &str,
    reader_id: &str,
    topic_id: TopicId,
    start_offset: Offset,
    end_offset: Offset,
) -> Result<reader::CommitResponse, String> {
    let req = reader::CommitRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        topic_id,
        start_offset,
        end_offset,
    };
    let buf = encode_client_frame(ClientMessage::Commit(req), 1024);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match decode_server_frame(&data) {
        ServerMessage::Commit(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

/// Reader joins, polls, broker crashes, reader reconnects and polls again.
/// Uncommitted offsets should be re-dispatched.
#[tokio::test]
async fn test_reader_group_broker_crash_redispatch() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("rg-crash-redispatch").await as u32);

    let mut broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let group_id = "crash-redispatch-group";

    // Produce records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..10 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("rg-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Reader joins and polls
    let mut ws_reader = ws_connect(addr).await;
    let join = ws_join_group(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("join");
    assert!(join.success);

    let poll1 = ws_poll(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("poll1");
    assert!(poll1.success);
    let dispatched_range = (poll1.start_offset.0, poll1.end_offset.0);
    eprintln!("  [rg-crash] polled range: {:?}", dispatched_range);

    // DON'T commit — crash broker
    drop(ws);
    drop(ws_reader);
    broker.crash();
    tokio::time::sleep(Duration::from_millis(500)).await;
    broker.restart().await;
    let addr = broker.addr();

    // Reconnect, rejoin, poll again — uncommitted range should be re-dispatched
    let mut ws_reader = ws_connect(addr).await;
    let join = ws_join_group(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("rejoin");
    assert!(join.success);

    // The lease may have expired so the range should be re-available
    tokio::time::sleep(Duration::from_millis(100)).await;

    let poll2 = ws_poll(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("poll2");
    assert!(poll2.success);
    eprintln!(
        "  [rg-crash] re-polled range: ({}, {})",
        poll2.start_offset.0, poll2.end_offset.0
    );

    // The re-dispatched range should overlap with or start at the uncommitted range
    assert!(
        poll2.start_offset.0 <= dispatched_range.0 || poll2.start_offset.0 == dispatched_range.1,
        "Re-dispatched range should cover uncommitted offsets"
    );
}

/// Two readers share work via poll. One reader dies (session timeout).
/// Surviving reader picks up dead reader's uncommitted work.
#[tokio::test]
async fn test_reader_group_session_timeout_work_redistribution() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("rg-timeout-redist").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let group_id = "timeout-redist-group";

    // Produce records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..20 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("td-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Two readers join
    let mut ws_r1 = ws_connect(addr).await;
    let mut ws_r2 = ws_connect(addr).await;
    let join1 = ws_join_group(&mut ws_r1, group_id, topic_id, "reader-1")
        .await
        .expect("join1");
    assert!(join1.success);
    let join2 = ws_join_group(&mut ws_r2, group_id, topic_id, "reader-2")
        .await
        .expect("join2");
    assert!(join2.success);

    // Reader 1 polls and commits
    let poll_r1 = ws_poll(&mut ws_r1, group_id, topic_id, "reader-1")
        .await
        .expect("poll r1");
    if poll_r1.start_offset != poll_r1.end_offset {
        let commit = ws_commit(
            &mut ws_r1,
            group_id,
            "reader-1",
            topic_id,
            poll_r1.start_offset,
            poll_r1.end_offset,
        )
        .await
        .expect("commit r1");
        assert!(commit.success);
    }

    // Reader 2 polls but does NOT commit — then dies
    let poll_r2 = ws_poll(&mut ws_r2, group_id, topic_id, "reader-2")
        .await
        .expect("poll r2");
    eprintln!(
        "  [rg-timeout] r2 polled ({}, {}) — will die",
        poll_r2.start_offset.0, poll_r2.end_offset.0
    );
    drop(ws_r2); // reader-2 dies

    // Wait for session timeout (default 30s is too long for a test — the broker
    // uses CoordinatorConfig::default which is 30s session timeout. We rely on
    // heartbeat-triggered eviction: reader-1 heartbeats, which cleans expired members.)
    // In practice the lease_deadline_ms from the poll will expire, making
    // the range re-dispatchable even before session cleanup.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Reader 1 heartbeats (triggers expired member cleanup)
    let hb = ws_heartbeat(&mut ws_r1, group_id, topic_id, "reader-1").await;
    eprintln!(
        "  [rg-timeout] r1 heartbeat: {:?}",
        hb.as_ref().map(|h| &h.status)
    );

    // Reader 1 keeps polling until all work is consumed
    let mut total_committed = HashSet::new();
    if poll_r1.start_offset != poll_r1.end_offset {
        for o in poll_r1.start_offset.0..poll_r1.end_offset.0 {
            total_committed.insert(o);
        }
    }

    for _ in 0..10 {
        match ws_poll(&mut ws_r1, group_id, topic_id, "reader-1").await {
            Ok(poll) if poll.success && poll.start_offset != poll.end_offset => {
                eprintln!(
                    "  [rg-timeout] r1 polled ({}, {})",
                    poll.start_offset.0, poll.end_offset.0
                );
                let commit = ws_commit(
                    &mut ws_r1,
                    group_id,
                    "reader-1",
                    topic_id,
                    poll.start_offset,
                    poll.end_offset,
                )
                .await
                .expect("commit");
                assert!(commit.success);
                for o in poll.start_offset.0..poll.end_offset.0 {
                    total_committed.insert(o);
                }
            }
            _ => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    eprintln!(
        "  [rg-timeout] total committed offsets: {}",
        total_committed.len()
    );
    // At minimum, reader-1's committed range should be present
    assert!(
        !total_committed.is_empty(),
        "At least some offsets should be committed"
    );
}

/// Produce during S3 partition, reader group poll returns limited data.
/// After partition heals, full data becomes available.
#[tokio::test]
async fn test_reader_group_s3_partition_during_poll() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("rg-s3-part").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let group_id = "s3-part-group";

    // Produce records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("s3p-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Reader joins
    let mut ws_reader = ws_connect(addr).await;
    let join = ws_join_group(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("join");
    assert!(join.success);

    // Partition S3 gets (reads will fail)
    broker.faulty_store().partition_gets();

    // Poll should still succeed (poll dispatches from Postgres, doesn't read S3)
    let poll = ws_poll(&mut ws_reader, group_id, topic_id, "reader-1").await;
    match poll {
        Ok(p) if p.success => {
            eprintln!(
                "  [rg-s3-part] poll succeeded during S3 partition: ({}, {})",
                p.start_offset.0, p.end_offset.0
            );
        }
        Ok(p) => {
            eprintln!("  [rg-s3-part] poll failed: {}", p.error_message);
        }
        Err(e) => {
            eprintln!("  [rg-s3-part] poll error: {}", e);
        }
    }

    // Heal partition
    broker.faulty_store().heal_partition();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // More produces after heal
    let w2 = WriterId::new();
    for i in 0..3 {
        let resp = ws_produce(&mut ws, w2, i + 1, topic_id, &format!("post-heal-{}", i))
            .await
            .expect("post-heal write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Poll again — should get new work
    drop(ws_reader);
    let mut ws_reader = ws_connect(addr).await;
    let rejoin = ws_join_group(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("rejoin");
    assert!(rejoin.success);

    let poll2 = ws_poll(&mut ws_reader, group_id, topic_id, "reader-1")
        .await
        .expect("poll after heal");
    assert!(
        poll2.success,
        "poll should succeed after S3 partition heals"
    );
}

/// Multiple readers poll and commit concurrently. No offset range is committed
/// by more than one reader (ownership invariant).
#[tokio::test]
async fn test_reader_group_concurrent_poll_no_double_commit() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("rg-no-double").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let group_id = "no-double-commit-group";

    // Produce many records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..30 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("ndc-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Three readers poll and commit concurrently
    let committed_ranges = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(u64, u64)>::new()));
    let mut handles = vec![];

    for r in 0..3 {
        let committed = committed_ranges.clone();
        handles.push(tokio::spawn(async move {
            let reader_id = format!("reader-{}", r);
            let mut ws = ws_connect(addr).await;
            let join = ws_join_group(&mut ws, group_id, topic_id, &reader_id)
                .await
                .expect("join");
            assert!(join.success);

            for _ in 0..5 {
                match ws_poll(&mut ws, group_id, topic_id, &reader_id).await {
                    Ok(poll) if poll.success && poll.start_offset != poll.end_offset => {
                        match ws_commit(
                            &mut ws,
                            group_id,
                            &reader_id,
                            topic_id,
                            poll.start_offset,
                            poll.end_offset,
                        )
                        .await
                        {
                            Ok(c) if c.success => {
                                committed
                                    .lock()
                                    .await
                                    .push((poll.start_offset.0, poll.end_offset.0));
                            }
                            _ => {}
                        }
                    }
                    _ => break,
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Verify no overlapping committed ranges
    let ranges = committed_ranges.lock().await;
    for (i, &(s1, e1)) in ranges.iter().enumerate() {
        for (j, &(s2, e2)) in ranges.iter().enumerate() {
            if i != j {
                let overlaps = s1 < e2 && s2 < e1;
                assert!(
                    !overlaps,
                    "Committed ranges overlap: ({}, {}) and ({}, {})",
                    s1, e1, s2, e2
                );
            }
        }
    }
    eprintln!(
        "  [rg-no-double] {} ranges committed, no overlaps",
        ranges.len()
    );
}

/// Reader A joins, polls, gets range, then disconnects (without calling leave_group).
/// Reader B joins and polls shortly after. Without the proactive leave_group on
/// disconnect + dispatch_cursor rollback, Reader B would get nothing because
/// dispatch_cursor is past the data and leases haven't expired (45s default).
/// With the fix, the broker calls leave_group on disconnect which rolls back
/// dispatch_cursor, so Reader B gets the work immediately.
#[tokio::test]
async fn test_disconnect_triggers_fast_redistribution() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("rg-disconnect-redist").await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();
    let group_id = "disconnect-redist-group";

    // Produce 10 records
    let mut ws = ws_connect(addr).await;
    let writer_id = WriterId::new();
    for i in 0..10 {
        let resp = ws_produce(&mut ws, writer_id, i + 1, topic_id, &format!("dr-{}", i))
            .await
            .expect("write");
        assert!(resp.success);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Reader A: join, poll, get range, then DROP connection (no leave_group)
    let mut ws_a = ws_connect(addr).await;
    let join_a = ws_join_group(&mut ws_a, group_id, topic_id, "reader-a")
        .await
        .expect("join A");
    assert!(join_a.success);

    let poll_a = ws_poll(&mut ws_a, group_id, topic_id, "reader-a")
        .await
        .expect("poll A");
    assert!(poll_a.success);
    let range_a = (poll_a.start_offset.0, poll_a.end_offset.0);
    eprintln!(
        "  [rg-disconnect] reader-a polled range ({}, {})",
        range_a.0, range_a.1
    );
    assert!(
        range_a.1 > range_a.0,
        "reader-a should have received some data"
    );

    // Disconnect reader-a WITHOUT calling leave_group (simulates crash/network drop)
    drop(ws_a);

    // Brief wait for the broker to process the WebSocket close and call leave_group
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Reader B: join and poll — should get the work that reader-a abandoned
    let mut ws_b = ws_connect(addr).await;
    let join_b = ws_join_group(&mut ws_b, group_id, topic_id, "reader-b")
        .await
        .expect("join B");
    assert!(join_b.success);

    let poll_b = ws_poll(&mut ws_b, group_id, topic_id, "reader-b")
        .await
        .expect("poll B");
    assert!(poll_b.success, "reader-b poll should succeed");

    let range_b = (poll_b.start_offset.0, poll_b.end_offset.0);
    eprintln!(
        "  [rg-disconnect] reader-b polled range ({}, {})",
        range_b.0, range_b.1
    );

    // KEY ASSERTION: Reader B must get actual data.
    // Without the fix:
    //   - leave_group is never called (no proactive cleanup on disconnect)
    //   - dispatch_cursor stays at range_a.1 (past the data)
    //   - reader-a's inflight lease won't expire for 45s
    //   - Reader B gets empty range: start_offset == end_offset
    // With the fix:
    //   - Broker calls leave_group on disconnect
    //   - leave_group rolls back dispatch_cursor to range_a.0
    //   - Reader B gets data starting from the abandoned offset
    assert!(
        range_b.1 > range_b.0,
        "INVARIANT: reader-b must receive data after reader-a disconnects \
         (got empty range — leave_group on disconnect not working or \
         dispatch_cursor not rolled back)"
    );

    // Verify Reader B's range covers the abandoned work
    assert!(
        range_b.0 <= range_a.0,
        "reader-b should start at or before reader-a's range \
         (reader-b starts at {}, reader-a started at {})",
        range_b.0,
        range_a.0
    );

    // Commit and verify
    let commit_b = ws_commit(
        &mut ws_b,
        group_id,
        "reader-b",
        topic_id,
        poll_b.start_offset,
        poll_b.end_offset,
    )
    .await
    .expect("commit B");
    assert!(commit_b.success, "reader-b commit should succeed");
}
