# Flux: Core Failure-Tolerance Algorithms

This document describes the algorithms that provide failure tolerance between writers, brokers, and readers. Each section maps to specific code paths and explains the invariants maintained.

## System Model

```
Writer ──WebSocket──► Broker ──S3 PUT──► Object Store (FL files)
                        │
                        ├──Postgres──► topic_offsets
                        ├──Postgres──► topic_batches  (segment index)
                        ├──Postgres──► writer_state   (dedup)
                        └──Postgres──► reader_*       (group coordination)
                        │
Reader ◄──WebSocket──── Broker ◄──S3 GET──── Object Store
```

**Key design choice:** brokers are stateless. All durable state lives in Postgres. Any broker can serve any writer or reader because coordination happens through the database.

---

## 1. Writer → Broker: Idempotent Append

### Problem

A writer sends an `AppendRequest(writer_id, append_seq, batches[])`. The broker buffers it, flushes to S3, commits offsets + segment index to Postgres, then returns `AppendResponse(append_seq, acks[])`. If the network drops after the commit but before the response, the writer retries with the same `(writer_id, append_seq)`. Without dedup, records are duplicated.

### Algorithm

Each writer maintains a monotonically increasing `append_seq` (`AtomicU64`, starts at 1). The broker uses this to implement idempotent appends.

**Writer side** (`crates/flux-sdk/src/writer.rs`):

1. `append_seq` is allocated atomically via `fetch_add(1, SeqCst)` before the request is sent.
2. On backpressure (`ERR_BACKPRESSURE`), the writer retries with the **same** `append_seq` using exponential backoff (100ms → 10s, up to 5 retries).
3. Pipelined in-flight requests are bounded by a semaphore (`max_in_flight`, default 256). This prevents unbounded memory growth if the broker is slow.
4. Each in-flight request has a 30s timeout. On timeout or disconnect, the pending oneshot channel is cleaned up.

**Broker side** (`crates/flux-broker/src/batched_server/append.rs`):

Appends are processed synchronously on the connection handler to preserve per-writer TCP arrival order. Non-append messages are dispatched concurrently to spawned tasks.

ACL check happens in the connection handler before `enqueue_append` is called. If the principal lacks append permission for the target topic, the request is rejected with `ERR_AUTHZ_DENIED` without entering the dedup or buffering path.

```
enqueue_append(req):
  1. in_flight_append_decision(writer_id, append_seq)
     → if same (writer_id, append_seq) is already in-flight:
         wait on its outcome (coalesce duplicate retries)
     → otherwise: register as in-flight, proceed

  2. check backpressure flag → reject with ERR_BACKPRESSURE if active

  3. dedup_cache.check(writer_id, append_seq):
     a. LRU cache hit (fast path):
        - append_seq == last_seq  → return Duplicate(cached_acks)
        - append_seq <  last_seq  → return Stale
        - append_seq >  last_seq  → return Accept
     b. Cache miss → query writer_state table (slow path):
        - row found: cache it, apply same logic as (a)
        - row missing: cache as Missing, return Accept

  4. On Stale: check if a higher append_seq from this writer is in-flight.
     If yes, allow it (out-of-order pipelining). If no, reject.

  5. On dedup error: fail open (continue processing).

  6. On Accept: enqueue into flush channel → return Pending

await_append_ack(pending):
  7. Wait for flush ack oneshot
  8. On success: update dedup cache, complete in-flight entry,
     notify any coalesced waiters
  9. On channel closed: return ERR_INTERNAL_ERROR, complete in-flight
```

**Dedup cache** (`crates/flux-broker/src/dedup.rs`):

- Two-tier: LRU in-memory (100k entries) + Postgres `writer_state` table.
- On `check()`: read lock for cache hit, write lock + DB query for cache miss.
- The `Missing` sentinel prevents repeated DB queries for new writers before their first commit.
- On `update()`: only advances if `append_seq > existing.last_seq` (monotonicity guard, uses `<=` check to skip no-ops).

**Durability of dedup state** (`crates/flux-broker/src/batched_server/flush.rs`):

During `commit_batch`, the broker persists the latest `(writer_id, last_seq, last_acks)` in the same Postgres transaction that commits the offsets and segment index. When multiple appends from the same writer are in the same flush, only the highest `append_seq` is persisted. The upsert has a `WHERE writer_state.last_seq < EXCLUDED.last_seq` guard, so concurrent brokers processing the same writer never regress the sequence.

### Dedup limitation: single `last_seq`

The dedup tracks only `last_seq` (the highest committed sequence), not a window of recent sequences. This means the broker cannot distinguish "seq=N was already committed in a batch where last_seq > N" from "seq=N hasn't arrived yet." The `has_higher_in_flight_sequence` check (step 4) is a best-effort heuristic for pipelining — it allows lower sequences through when a higher one is still in-flight, under the assumption that both are from the same pipelining burst and the lower one hasn't committed yet.

This is safe given the SDK's guarantees: `append_seq` only increments and is never re-sent (the SDK only retries on backpressure, using the same seq). A custom client that re-sends old sequences while new ones are in-flight could bypass the dedup, but this is outside the supported protocol contract.

### Invariant

> For any `(writer_id, append_seq)` sent through the standard SDK, the records are committed to storage **at most once**. If the writer retries on backpressure, it receives the same acks.

### Failure Scenarios

| Failure | Behavior |
|---------|----------|
| Network drop before broker receives request | Writer times out, retries with same `append_seq` |
| Broker crashes after S3 write but before DB commit | S3 file is orphaned (GC cleans up). Writer retries; no duplicate because DB has no record |
| Broker crashes after DB commit but before response | Writer retries. Dedup cache misses, DB lookup finds `last_seq == append_seq`, returns `Duplicate(cached_acks)` |
| Writer crashes mid-retry | New writer instance gets new `WriterId(UUID)`. Old writer's dedup state is eventually GCed (`cleanup_stale_writers`) |
| Dedup cache eviction | Falls through to DB lookup. No correctness impact, only latency |
| Two brokers process same retry | Both do `check()`. One commits first (atomic DB tx). The other either sees `Duplicate` from DB or its commit fails the upsert WHERE guard |

---

## 2. Broker Buffering and Pipelined Flush

### Problem

The broker must merge records from many writers into efficient S3 objects while ensuring that offsets are gap-free, per-topic monotonic, and that each writer gets correct ack offsets.

### Algorithm

**Buffer** (`crates/flux-broker/src/buffer.rs`):

Records are grouped by `BatchKey(topic_id, schema_id)`. Multiple writers targeting the same key have their records appended into a single `BufferedSegment`. Each `PendingWriter` tracks:
- `segment_keys`: which batch keys this writer contributed to
- `record_counts`: how many records per key
- `start_indices`: the insertion point within each merged batch

The buffer tracks `total_bytes` and triggers a flush when either of:
- `total_bytes >= max_size_bytes` (default 256 MB)
- Time since first record in buffer `>= max_wait` (default 200ms)

**Pipelined flush loop** (`crates/flux-broker/src/batched_server/flush.rs`):

The flush loop decouples write ingestion from flush I/O: while a flush runs on a spawned task, the loop continues accepting new writes into the buffer. At most one flush is in-flight at a time, preserving offset ordering and dedup correctness.

```
flush_loop:
  flush_in_flight = false

  select:
    recv(cmd) →
      drain up to 2048 commands from channel (batch draining via try_recv)
      for each Insert: add to buffer, check backpressure
      if (buffer.should_flush() or force_flush) and not flush_in_flight:
        drain buffer, spawn flush task → flush_in_flight = true
      on Shutdown: wait for in-flight flush, flush remaining, exit

    tick(flush_interval, default 100ms) →
      if buffer not empty and not flush_in_flight:
        drain buffer, spawn flush task → flush_in_flight = true

    flush_done_rx →
      flush_in_flight = false
      if flush succeeded and buffer below low_water: release backpressure
      if buffer not empty: start next flush immediately
```

**Flush pipeline** (`execute_flush` in `flush.rs`):

```
execute_flush(drain_result):
  1. drain() → DrainResult { batches, pending_writers, key_to_index }
  2. Build FL file (ZSTD-compressed, CRC32 per segment)
  3. S3 PUT (single write for all segments in this flush)
  4. commit_batch() in a single Postgres transaction:
     a. Allocate offsets: atomic increment of topic_offsets.next_offset
        per topic_id. Uses INSERT ... ON CONFLICT DO UPDATE
        with a single batched query for all topics.
     b. Insert segment index rows into topic_batches.
     c. Persist writer dedup state (highest append_seq per writer in this flush).
     d. COMMIT transaction.
  5. distribute_acks(): for each PendingWriter, compute their slice
     of the allocated offset range based on start_indices.
  6. Return success/failure to flush_done channel.
```

**Offset allocation** (`commit_batch` in `flush.rs`):

The key algorithm for gap-free offsets:

```
For each topic_id:
  total_delta = sum of record_counts for all segments in this topic
  topic_end = atomically increment topic_offsets.next_offset by total_delta
  cursor = topic_end - total_delta  (this is the start)

  For each segment in this topic (in insertion order):
    segment_offsets[seg_idx] = (cursor, cursor + segment.record_count)
    cursor += segment.record_count
```

This is done in a single batched `INSERT ... ON CONFLICT ... RETURNING` query, so all topics get their offsets in one round-trip.

**Ack distribution** (`BrokerBuffer::distribute_acks` in `buffer.rs`):

```
For each PendingWriter:
  For each (segment_key, record_count, start_idx) in this writer:
    seg_idx = key_to_index[segment_key]
    (seg_start, seg_end) = segment_offsets[seg_idx]
    writer_start = seg_start + start_idx
    writer_end = writer_start + record_count
    → BatchAck { topic_id, schema_id, start_offset, end_offset }
```

### Backpressure

- **High water mark** (default 384 MB): when buffer `total_bytes >= high_water_bytes`, set `backpressure = true`. New appends get `ERR_BACKPRESSURE`.
- **Low water mark** (default 128 MB): after a successful flush, if `total_bytes <= low_water_bytes`, clear the flag.
- Writers retry with exponential backoff on backpressure.

### Invariant

> Within a single topic, offsets are contiguous and monotonically increasing. No offset is assigned twice. The S3 data, the segment index, and the offset counter are committed atomically.

### Failure Scenarios

| Failure | Behavior |
|---------|----------|
| S3 PUT fails | execute_flush returns false. Acks are never sent. Writers time out and retry. Buffer has already been drained, but writers will re-send the same `append_seq` |
| Postgres commit fails | Same as S3 failure — acks not sent, writers retry. S3 file is orphaned |
| Broker crashes mid-flush | In-flight writers lose their oneshot channels → `SdkError::Disconnected`. They reconnect and retry. Since DB was not committed, dedup will Accept |
| Flush loop panics | Tokio task dies. `flush_tx` is dropped. All pending senders get `Err` → `ERR_INTERNAL_ERROR`. Writers reconnect and retry |

---

## 3. FL Storage Format

### Problem

Records must be stored durably in S3 and retrieved efficiently for reads. A single S3 object may contain segments for multiple (topic, schema) combinations.

### Format (`crates/flux-broker/src/fl.rs`)

```
┌───────────────────────────────────────┐
│ Segment 0 data (ZSTD compressed)      │  ← byte_offset=0, byte_length=N₀
├───────────────────────────────────────┤
│ Segment 1 data (ZSTD compressed)      │  ← byte_offset=N₀, byte_length=N₁
├───────────────────────────────────────┤
│ ...                                   │
├───────────────────────────────────────┤
│ Footer: varint-encoded SegmentMeta[]  │
│ Footer length (4B big-endian)         │
│ Magic: "FLRN" (4B)                    │
└───────────────────────────────────────┘
```

Each `SegmentMeta` contains: `topic_id, schema_id, start_offset, end_offset, record_count, byte_offset, byte_length, ingest_time, compression_codec, crc32`.

The footer is encoded as: a varint count, then for each segment: varints for topic_id, schema_id, start_offset (unsigned), end_offset (unsigned), record_count; unsigned varints for byte_offset, byte_length, ingest_time; a single byte for compression codec; and 4 bytes big-endian for crc32.

**Integrity:** each segment is independently CRC32-checksummed over the compressed bytes. On read, the CRC is verified before decompression.

**Read path:** the `topic_batches` table stores `(s3_key, byte_offset, byte_length, crc32)` per segment. Readers issue HTTP range reads (`get_range`) to fetch only the bytes for the segment they need — no need to download the full FL file.

---

## 4. Reader → Broker: Read Path

### Problem

Readers need to fetch records starting from a given offset for a topic. The data is in S3, indexed by the `topic_batches` table.

### Algorithm (`crates/flux-broker/src/batched_server/read.rs`, `fetch.rs`)

```
process_read(ReadRequest { topic_id, offset, max_bytes }):
  1. Query topic_batches WHERE topic_id
     AND end_offset > requested_offset
     ORDER BY start_offset LIMIT 10

  2. Query topic_offsets for high_watermark (next_offset)

  3. For each matching segment:
     a. S3 range read: get_range(s3_key, byte_offset, byte_length)
     b. Construct SegmentMeta with byte_offset=0 (range read is the slice)
     c. FlReader::read_segment(data, meta, verify_crc=true)
     d. Skip records before requested_offset

  4. Group records by schema_id: when the schema changes between
     segments, a new TopicResult is emitted. This preserves
     schema boundaries so the reader knows which schema to use
     for deserialization.

  5. Accumulate records up to max_bytes limit (counted by
     key + value byte sizes). Stop fetching segments once
     the limit is reached.

  6. If no segments matched, return an empty TopicResult
     with schema_id=0 and the current high_watermark.
```

### Failure Tolerance

- **S3 read failure:** the read returns an error response. The reader retries on next poll.
- **Stale segment index:** if a segment's S3 key has been GCed, the read fails for that segment. The reader can skip forward by committing a higher offset.
- **CRC mismatch on read:** `FlError::CrcMismatch` is returned. The segment is treated as corrupt. The reader will see an error and can skip past it.

---

## 5. Reader Groups: Broker-Driven Offset-Range Dispatch

### Problem

Multiple readers in a group need to consume a topic in parallel without duplicating work. Rather than assigning fixed ownership of offsets to readers (which requires rebalancing when membership changes), the broker acts as a work dispatcher — handing out leased offset ranges to readers on demand.

### State Model (all in Postgres)

| Table | Purpose |
|-------|---------|
| `reader_groups` | `(group_id, topic_id)` — group registration |
| `reader_members` | `(group_id, topic_id, reader_id, broker_id, last_heartbeat)` |
| `reader_group_state` | `(group_id, topic_id, dispatch_cursor, committed_watermark)` — group-level progress |
| `reader_inflight` | `(group_id, topic_id, start_offset, end_offset, reader_id, lease_expires_at)` — leased offset ranges |

The `dispatch_cursor` tracks the frontier of dispatched work. The `committed_watermark` tracks the lowest offset that has not yet been committed — everything below it has been fully processed.

### Coordinator Configuration (`crates/flux-broker/src/coordinator/mod.rs`)

| Field | Default | Purpose |
|-------|---------|---------|
| `lease_duration` | 45s | How long an inflight range lease remains valid after renewal |
| `session_timeout` | 30s | How long before a member without heartbeat is considered expired |
| `broker_id` | random UUID | Identifies this broker instance; stored in `reader_members.broker_id` |
| `max_inflight_per_reader` | 10 | Maximum outstanding (uncommitted) poll ranges per reader |

### Join Protocol (`crates/flux-broker/src/coordinator/db.rs`)

```
join_group(group_id, topic_id, reader_id):
  BEGIN TRANSACTION
  1. Initialize group if first reader (idempotent):
     - INSERT reader_groups ON CONFLICT DO NOTHING
     - INSERT reader_group_state (dispatch_cursor=0, committed_watermark=0)
       ON CONFLICT DO NOTHING
  2. Upsert into reader_members (register membership, update heartbeat)
  COMMIT
```

Join is lightweight — it registers the member. No assignment computation, no generation bump. The reader begins polling immediately after join.

### Poll Protocol (`crates/flux-broker/src/coordinator/db.rs`)

The poll is the core dispatch mechanism. The broker hands out the next available offset range:

```
poll(group_id, topic_id, reader_id, max_bytes):
  BEGIN TRANSACTION
  1. Verify member exists → empty result if not
  2. Check inflight count for this reader vs max_inflight_per_reader
     → return MaxInflight status if at limit
  3. Try to steal an expired inflight lease (lease_expires_at < NOW()):
     UPDATE reader_inflight SET reader_id = this_reader, renew lease
     WHERE expired, ORDER BY start_offset LIMIT 1 FOR UPDATE SKIP LOCKED
     → if stolen: COMMIT, return stolen range
  4. Lock dispatch_cursor FOR UPDATE (serializes concurrent polls)
  5. Read high_watermark from topic_offsets
     → if dispatch_cursor >= high_watermark: no new data, return empty
  6. Find batches from topic_batches WHERE topic_id
     AND end_offset > dispatch_cursor AND start_offset < high_watermark
     Accumulate byte_length up to max_bytes
  7. INSERT into reader_inflight (start_offset, end_offset, reader_id, lease)
  8. Advance dispatch_cursor to end_offset
  COMMIT
  Return (start_offset, end_offset, lease_deadline_ms)
```

**Key details:**
- Step 3 reclaims expired leases before advancing the cursor, preventing offset gaps when readers die.
- Step 4 uses `FOR UPDATE` on `reader_group_state` to serialize concurrent polls. This is the single serialization point for the group.
- The broker also fetches and returns the actual records for the dispatched range (via `fetch_records`), so the poll response includes both the lease and the data.

### Heartbeat Protocol (`crates/flux-broker/src/coordinator/db.rs`)

```
heartbeat(group_id, topic_id, reader_id):
  BEGIN TRANSACTION
  1. Update last_heartbeat for this member
     → if no row updated: return UnknownMember
  2. Renew leases on all inflight ranges owned by this reader
  3. Find expired members (last_heartbeat < NOW() - session_timeout)
  4. If expired members exist:
     a. Delete expired members
     b. Delete their inflight ranges, capturing the minimum start_offset
     c. Roll back dispatch_cursor to MIN(deleted start_offset, dispatch_cursor)
        so those ranges get re-dispatched on next poll
     d. Update committed_watermark = MIN(remaining inflight start_offsets)
        or dispatch_cursor if no inflight ranges remain
  COMMIT
  Return Ok
```

**How dead readers are recovered:** the heartbeat handler of any live reader detects expired members and rolls back the dispatch cursor so their uncommitted work is re-dispatched. No rebalance protocol — the work simply becomes available again.

### Commit Protocol (`crates/flux-broker/src/coordinator/db.rs`)

```
commit_range(group_id, topic_id, reader_id, start_offset, end_offset):
  BEGIN TRANSACTION
  1. DELETE FROM reader_inflight
     WHERE group_id AND topic_id AND start_offset AND end_offset AND reader_id
     → if no row deleted: return NotOwner
  2. Update committed_watermark = MIN(remaining inflight start_offsets)
     or dispatch_cursor if no inflight ranges remain
  COMMIT
  Return Ok
```

The commit requires exact match on `(start_offset, end_offset, reader_id)`. A reader that has been evicted (inflight deleted by heartbeat) cannot commit — the row no longer exists.

### Leave Protocol (`crates/flux-broker/src/coordinator/db.rs`)

```
leave_group(group_id, topic_id, reader_id):
  BEGIN TRANSACTION
  1. Delete this reader's inflight ranges
  2. Update committed_watermark
  3. Delete member row
  COMMIT
```

Before leaving, the SDK commits all outstanding inflight ranges. Any uncommitted ranges are released for re-dispatch.

### Force Reset (Break-Glass) (`crates/flux-broker/src/coordinator/db.rs`)

```
force_reset(group_id, topic_id):
  BEGIN TRANSACTION
  1. Delete all inflight ranges
  2. Delete all members
  3. Roll back dispatch_cursor to committed_watermark
  COMMIT
```

This is a manual intervention for stuck groups — clears all state and resets to the last committed position.

### Reader SDK State Machine (`crates/flux-sdk/src/reader/mod.rs`)

```
Init ──join()──► Active ──poll()──► Active (accumulate inflight)
                   │                   │
                   │                   ├──commit(batch)──► Active (release inflight)
                   │                   │
                   ├──heartbeat(UnknownMember)──► Init ──join()──► Active
                   │
                   └──stop()──► Stopped
```

The SDK tracks inflight ranges locally. Before leaving, it commits all outstanding ranges. On `UnknownMember` (eviction detected via heartbeat), it clears local state and re-joins.

### Failure Scenarios

| Failure | Behavior |
|---------|----------|
| Reader crashes without leaving | Heartbeat stops. Other readers' heartbeat handlers detect expired member within `session_timeout` (30s). Inflight ranges deleted, dispatch_cursor rolled back, ranges re-dispatched on next poll |
| Reader network isolation | Same as crash — heartbeat expires. Inflight leases expire. On reconnect, reader gets `UnknownMember` and re-joins |
| Broker crashes | Reader reconnects to another broker. All state is in Postgres — new broker dispatches correctly |
| Lease expires while reader is slow | On next poll by any reader, the expired lease is stolen (step 3). The slow reader's commit will return `NotOwner` for that range |
| Concurrent polls | Serialized by `FOR UPDATE` on `reader_group_state.dispatch_cursor`. Each poll gets a distinct, non-overlapping range |
| Commit after eviction | The inflight row was deleted during heartbeat-driven cleanup. `commit_range` finds no matching row — returns `NotOwner` |

### Current Limitations

- **Single-topic groups only:** `join_group` accepts a single `topic_id`. Multi-topic reader groups are not yet supported.
- **Dispatch cursor serialization:** All polls for a group serialize on `FOR UPDATE` of the dispatch cursor. This limits poll throughput to one concurrent poll per group (though pipelining via `max_inflight_per_reader` amortizes this).

---

## 6. Broker Graceful Shutdown

### Algorithm (`crates/flux-broker/src/batched_server/mod.rs`, `crates/flux-broker/src/shutdown.rs`)

```
run_with_shutdown(state, shutdown_signal):
  loop:
    select:
      accept(connection) → spawn handler with TrackedConnection guard
      shutdown_signal → break

  1. Stop accepting new connections
  2. wait_for_drain(timeout=25s) — poll TrackedConnection count every 100ms
  3. Send FlushCommand::Shutdown to flush loop
  4. Flush loop:
     a. Waits for any in-flight flush to complete
     b. Flushes any remaining buffered data synchronously
     c. Exits
  5. Sleep 2s for flush completion
```

**TrackedConnection** is a RAII guard: `new()` increments the connection count, `Drop` decrements it. This ensures the count is accurate even if handlers panic.

### Invariant

> On shutdown, all buffered records that have been acknowledged are flushed to S3 and committed to Postgres. Unacknowledged records (still in the buffer or in-flight) are lost — writers will retry them.

---

## 7. End-to-End Durability Summary

A record is **durably committed** when all of the following are true:
1. The FL file containing the record is written to S3
2. The `topic_batches` segment index row is committed to Postgres
3. The `topic_offsets` counter is incremented in the same transaction
4. The `writer_state` dedup entry is persisted in the same transaction

Steps 2-4 happen in a single Postgres transaction. If the transaction fails, the S3 file is orphaned but no offset is allocated and no segment is indexed — the record effectively doesn't exist and the writer will retry.

The writer receives its `BatchAck` (with concrete offsets) **only after** the transaction commits. This means:
- If the writer receives an ack, the record is durable
- If the writer does not receive an ack (timeout, disconnect), it retries with the same `append_seq`, and the dedup layer ensures at-most-once semantics

Combined: **exactly-once delivery from writer to storage** (given the SDK's protocol contract).

Readers see records only when the `topic_batches` index exists (post-commit), so they never read uncommitted data.
