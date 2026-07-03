# Flux Architecture

Flux is a **disaggregated event bus** — stateless Rust brokers backed by S3 for data and Postgres for metadata. Any broker can serve any client; all durable state lives outside the broker process.

## System Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                   Clients                                    │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐      ┌─────────────────────┐    │
│  │ Rust SDK  │  │ Java SDK  │  │Python SDK │      │   CLI (flux)    │    │
│  │  Writer   │  │  Writer   │  │  Writer   │      │bootstrap│topic│tail │    │
│  │  Reader   │  │  Reader   │  │  Reader   │      └─────────────────────┘    │
│  └───────────┘  └───────────┘  └───────────┘                                 │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
         │               │               │                    │
         │  WebSocket (binary protobuf)  │          HTTP REST │
         │               │               │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌──────────────────────────────┐  ┌─────────────────────────────┐      │
│  │    Data Plane (:9000)        │  │    Admin Plane (:9001)      │      │
│  │    WebSocket Server          │  │    HTTP/REST (axum)         │      │
│  │                              │  │                             │      │
│  │  ┌────────────────────────┐  │  │  GET/POST/PUT/DEL /topics   │      │
│  │  │  Connection Handler    │  │  │  GET /groups                │      │
│  │  │  (per client)          │  │  │  POST /groups/.../reset     │      │
│  │  │                        │  │  │  DEL /groups/.../members    │      │
│  │  │  • Auth handshake      │  │  └─────────────────────────────┘      │
│  │  │  • ACL checks          │  │                                       │
│  │  │  • Dispatch by type    │  │                                       │
│  │  └────────────────────────┘  │                                       │
│  └──────────────────────────────┘                                       │
│                                                                         │
│         Broker Process (stateless)                                      │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                      Message Dispatch                           │    │
│  │                                                                 │    │
│  │  Append ──────► enqueue_append() ──► flush channel (mpsc)       │    │
│  │  Read ────────► handle_read()                                   │    │
│  │  Poll ────────► handle_poll()       ┌────────────────┐          │    │
│  │  Join ────────► coordinator ──────► │  Coordinator   │          │    │
│  │  Heartbeat ──► coordinator          │  (Postgres)    │          │    │
│  │  Commit ─────► coordinator          └────────────────┘          │    │
│  │  Leave ──────► coordinator                                      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Write Path (Append → Flush → Ack)

```
Writer SDK                    Broker                              External
───────────                   ──────                              ────────

append(records)
  │
  │  ClientMessage::Append
  │  (writer_id, append_seq,
  │   topic_id, schema_id,
  │   records)
  │
  ▼
              enqueue_append()
              ┌─────────────────────────────┐
              │ 1. In-flight decision       │
              │    • Same (writer,seq)?     │  Coalesce: wait on existing
              │    • New request            │
              │                             │
              │ 2. Backpressure check       │  ERR_BACKPRESSURE if > 384MB
              │                             │
              │ 3. Dedup check              │
              │    ┌─────────────┐          │
              │    │  LRU Cache  │ hit ──►  │  Return cached BatchAck
              │    │  (100k)     │          │
              │    └──────┬──────┘          │
              │           │ miss            │
              │    ┌──────▼──────┐          │
              │    │  Postgres   │ dup ──►  │  Return stored acks
              │    │writer_state │          │
              │    └──────┬──────┘          │
              │           │ accept          │
              │                             │
              │ 4. Send FlushCommand        │
              │    via mpsc channel         │
              └─────────────────────────────┘
                              │
                              ▼
              ┌─────────────────────────────┐
              │     BrokerBuffer            │
              │                             │
              │  Merge by BatchKey          │
              │  (topic_id, schema_id)      │
              │                             │
              │  Writer₁ ─┐                 │
              │  Writer₂ ─┼─► Segment       │
              │  Writer₃ ─┘                 │
              │                             │
              │  Track PendingWriter        │
              │  start_indices for          │
              │  offset calculation         │
              └─────────────────────────────┘
                              │
          Trigger: size ≥ 256MB │ age ≥ 200ms │ tick 100ms
                              │
                              ▼
              ┌─────────────────────────────┐
              │     execute_flush()         │
              │                             │
              │ 1. Build FL file            │
              │    (ZSTD + CRC32)        ►  │  S3 PUT (single object)
              │                             │
              │ 2. commit_batch()           │
              │    Single Postgres TX:   ►  │  Postgres: topic_offsets,
              │    • Allocate offsets       │  topic_batches, writer_state
              │      (atomic increment)     │
              │    • Insert batch index     │
              │    • Upsert dedup state     │
              │                             │
              │ 3. distribute_acks()        │
              │    Compute per-writer       │
              │    offset slices            │
              └─────────────────────────────┘
                              │
                              │ oneshot channel
                              ▼
              Writer receives BatchAck
              (topic_id, schema_id,
               start_offset, end_offset)
```

## Read Path

```
Reader SDK                    Broker                              External
───────────                   ──────                              ────────

read(topic, offset)
  │
  │  ClientMessage::Read
  │
  ▼
              handle_read_request()
              ┌─────────────────────────────┐
              │ 1. Query topic_batches      │  Postgres (segment index)
              │    WHERE end_offset >       │
              │    requested_offset         │
              │                             │
              │ 2. Query topic_offsets      │  Postgres (high watermark)
              │    for high_watermark       │
              │                             │
              │ 3. For each batch:          │
              │    S3 range read ────────►  │  S3 GET range read
              │    FlReader::read_segment   │  (decompress + CRC verify)
              │                             │
              │ 4. Group by schema_id       │
              │    Respect max_bytes        │
              └─────────────────────────────┘
                              │
                              ▼
              ReadResponse
              (records, high_watermark)
```

## Reader Groups (Consumer Group Protocol)

```
┌─────────────────────────────────────────────────────────────────┐
│                   Reader Group Lifecycle                        │
│                                                                 │
│  ┌──────┐  join()   ┌────────┐  poll()   ┌──────────┐           │
│  │ Init │──────────►│ Active │──────────►│ Active   │           │
│  └──────┘           └───┬────┘           │ + batch  │           │
│      ▲                  │                └────┬─────┘           │
│      │                  │ heartbeat           │                 │
│      │ UnknownMember    │ (background)        │ commit(batch)   │
│      │                  │                     │                 │
│      └──────────────────┘                     ▼                 │
│                                          ┌──────────┐           │
│                          stop()          │ Active   │           │
│                         ┌────────────────│ - batch  │           │
│                         ▼                └──────────┘           │
│                    ┌─────────┐                                  │
│                    │ Stopped │                                  │
│                    └─────────┘                                  │
└─────────────────────────────────────────────────────────────────┘

Broker-side dispatch (no partition assignment):

  Postgres State:
  ┌───────────────────────────────────────────────────────┐
  │ reader_group_state                                    │
  │   dispatch_cursor ────► next offset to hand out       │
  │   committed_watermark ► all offsets below committed   │
  │                                                       │
  │ reader_inflight                                       │
  │   ┌──────────┬───────────┬──────────┬──────────────┐  │
  │   │ group_id │ start_off │ end_off  │ reader_id    │  │
  │   │          │ 100       │ 150      │ reader-A     │  │
  │   │          │ 150       │ 200      │ reader-B     │  │
  │   │          │ 200       │ 250      │ reader-A     │  │
  │   └──────────┴───────────┴──────────┴──────────────┘  │
  │                                                       │
  │ reader_members                                        │
  │   reader_id, last_heartbeat, broker_id                │
  └───────────────────────────────────────────────────────┘

  Poll flow:
  1. Check inflight count < max_inflight_per_reader (10)
  2. Try steal expired lease (FOR UPDATE SKIP LOCKED)
  3. Lock dispatch_cursor (FOR UPDATE)
  4. Read high watermark from topic_offsets
  5. Find batches, accumulate up to max_bytes
  6. Insert into reader_inflight with lease
  7. Advance dispatch_cursor
  8. Fetch records from S3, return to reader

  Heartbeat:
  • Renews all leases for this reader
  • Detects expired members (session_timeout)
  • Expired → delete member + inflight → roll back dispatch_cursor
```

## FL Storage Format

```
┌──────────────────────────────────────────────────────┐
│                                                      │
│  FL File (S3 Object)                                 │
│                                                      │
│  ┌──────────────────────────────────────────────┐    │
│  │  Segment 0 (ZSTD compressed)                 │    │  ◄── byte_offset = 0
│  │  Records for (topic_1, schema_1)             │    │
│  │  CRC32 verified on read                      │    │
│  └──────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────┐    │  ◄── byte_offset = N₀
│  │  Segment 1 (ZSTD compressed)                 │    │
│  │  Records for (topic_2, schema_1)             │    │
│  └──────────────────────────────────────────────┘    │  ◄── byte_offset = N₁
│  ┌──────────────────────────────────────────────┐    │
│  │  Segment 2 (ZSTD compressed)                 │    │
│  │  Records for (topic_1, schema_2)             │    │
│  └──────────────────────────────────────────────┘    │
│                                                      │
│  ┌──────────────────────────────────────────────┐    │
│  │  Footer (varint-encoded SegmentMeta[])       │    │
│  │  Per segment:                                │    │
│  │    topic_id, schema_id,                      │    │
│  │    start_offset, end_offset,                 │    │
│  │    byte_offset, byte_length,                 │    │
│  │    crc32, codec                              │    │
│  └──────────────────────────────────────────────┘    │
│  ┌────────────────┐  ┌────────────────┐              │
│  │ Footer Length  │  │  Magic "FLRN"  │              │
│  │   (4B BE)      │  │    (4B)        │              │
│  └────────────────┘  └────────────────┘              │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### Segment payload encoding (cross-language compatibility contract)

The bytes inside each segment are a public format: the zero-copy `RawRead`
protocol ships them to SDKs verbatim, and the Rust (`flux_wire::segment`,
reference implementation), Java (`SegmentDecoder`), and Python
(`flux.segment`) decoders must stay wire-compatible.

```
segment payload = zstd( record_0 ‖ record_1 ‖ ... )     zstd level 3, standard frame

record:
  key_len   : zigzag varint i64   (-1 = null key)
  key_bytes : key_len bytes       (absent when null)
  value_len : zigzag varint i64
  value     : value_len bytes
```

- Zigzag varint is Avro-style: 0→0, -1→1, 1→2, ...; little-endian 7-bit
  groups with a 0x80 continuation bit.
- Integrity: CRC-32 (IEEE) computed over the *compressed* payload, carried
  in the segment index (`topic_batches.crc32`) and in `RawSegment.crc32`.
  Decoders must verify before decompressing.
- Records are packed back-to-back; decode until the decompressed buffer is
  exhausted. Record offsets are implicit: the i-th record in a segment has
  offset `start_offset + i`.
- Any change here is a breaking protocol change and requires versioning the
  segment (new codec value in the footer + a `RawSegment` field).

## Crate Dependency Graph

```
                    ┌───────────────┐
                    │ flux-cli  │
                    │   (binary)    │
                    └───────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │ flux-sdk  │ │   flux-   │ │   flux-   │
    │    (lib)      │ │    broker     │ │    common     │
    │               │ │  (bin+lib)    │ │    (lib)      │
    └───────────────┘ └───────────────┘ └───────────────┘
           │               │              ▲
           │               │              │
           ▼               ▼              │
     ┌───────────────────────────┐
     │      flux-wire        │
     │         (lib)             │──────┘
     │  protobuf encode/decode   │
     └───────────────────────────┘
```

## Wire Protocol (Protobuf over WebSocket)

```
ClientMessage (oneof):              ServerMessage (oneof):
┌───────────────────────┐           ┌──────────────────────────┐
│ append                │ ────────► │ batch_ack                │
│   writer_id           │           │   topic_id               │
│   append_seq          │           │   schema_id              │
│   topic_id            │           │   start_offset           │
│   schema_id           │           │   end_offset             │
│   records[]           │           │   append_seq             │
├───────────────────────┤           ├──────────────────────────┤
│ read                  │ ────────► │ read_response            │
│   topic_id            │           │   records[]              │
│   start_offset        │           │   high_watermark         │
│   max_bytes           │           │                          │
├───────────────────────┤           ├──────────────────────────┤
│ join_group            │ ────────► │ join_group_response      │
│   group_id            │           │   status                 │
│   topic_id            │           │                          │
│   reader_id           │           │                          │
├───────────────────────┤           ├──────────────────────────┤
│ poll                  │ ────────► │ poll_response            │
│   group_id            │           │   records[]              │
│   topic_id            │           │   start_offset           │
│   reader_id           │           │   end_offset             │
│   max_bytes           │           │   high_watermark         │
├───────────────────────┤           ├──────────────────────────┤
│ heartbeat             │ ────────► │ heartbeat_response       │
│   group_id            │           │   status                 │
│   topic_id            │           │     OK                   │
│   reader_id           │           │     UnknownMember        │
├───────────────────────┤           ├──────────────────────────┤
│ commit                │ ────────► │ commit_response          │
│   group_id            │           │   status                 │
│   topic_id            │           │                          │
│   reader_id           │           │                          │
│   start_offset        │           │                          │
│   end_offset          │           │                          │
├───────────────────────┤           ├──────────────────────────┤
│ leave_group           │ ────────► │ error                    │
│ auth                  │           │   status_code            │
└───────────────────────┘           └──────────────────────────┘

Status Codes:
  0    STATUS_OK
  1001 ERR_AUTHZ_DENIED
  1002 ERR_DECODE_ERROR
  1004 ERR_BACKPRESSURE
  1005 ERR_INTERNAL_ERROR
  1007 ERR_STALE_SEQUENCE
  1008 ERR_SEQUENCE_IN_FLIGHT
  1009 ERR_MAX_INFLIGHT
```

## Postgres Schema (key tables)

```
┌─────────────────────┐      ┌──────────────────────┐      ┌──────────────────────────────┐
│ topics              │      │ topic_offsets        │      │ topic_batches                │
│─────────────────────│      │──────────────────────│      │──────────────────────────────│
│ id     (PK)         │      │ topic_id  (PK,FK)    │      │ topic_id      (FK)           │
│ name   (UNIQUE)     │      │ next_offset          │      │ start_offset                 │
│ retention_hours     │      └──────────────────────┘      │ end_offset                   │
└─────────────────────┘                                    │ s3_key                       │
                                                           │ byte_offset                  │
                                                           │ byte_length                  │
                                                           │ crc32                        │
                                                           │ ingest_time                  │
                                                           │ (range partitioned)          │
                                                           └──────────────────────────────┘

┌─────────────────────┐      ┌──────────────────────┐      ┌──────────────────────────────┐
│ writer_state        │      │ reader_groups        │      │ reader_group_state           │
│─────────────────────│      │──────────────────────│      │──────────────────────────────│
│ writer_id  (PK)     │      │ group_id  (PK)       │      │ group_id      (PK)           │
│ last_seq            │      │ topic_id  (PK)       │      │ topic_id      (PK)           │
│ last_acks  (JSON)   │      └──────────────────────┘      │ dispatch_cursor              │
└─────────────────────┘                                    │ committed_watermark          │
                                                           └──────────────────────────────┘

┌─────────────────────┐      ┌──────────────────────┐      ┌──────────────────────────────┐
│ schemas             │      │ reader_members       │      │ reader_inflight              │
│─────────────────────│      │──────────────────────│      │──────────────────────────────│
│ id     (PK)         │      │ group_id             │      │ group_id                     │
│ hash   (UNIQUE)     │      │ topic_id             │      │ topic_id                     │
│ content             │      │ reader_id  (PK)      │      │ start_offset                 │
└─────────────────────┘      │ broker_id            │      │ end_offset                   │
                             │ last_heartbeat       │      │ reader_id                    │
                             └──────────────────────┘      │ lease_expires_at             │
                                                           └──────────────────────────────┘
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Stateless brokers** | Horizontal scaling, no rebalancing on broker failure |
| **S3 for data, Postgres for metadata** | Cheap durable storage + strong transactional guarantees |
| **Exactly-once append** | Monotonic `append_seq` + two-tier dedup (LRU + Postgres) |
| **Gap-free offsets** | Atomic `INSERT ... ON CONFLICT DO UPDATE RETURNING` per topic |
| **Pipelined flush** | Next batch fills while current flush writes to S3 + Postgres |
| **Broker-driven reader dispatch** | No partition assignment; offset ranges leased on poll |
| **FL format with footer index** | S3 range reads for individual segments without reading whole file |
| **ZSTD + CRC32 per segment** | Compression + integrity verification at the segment level |
| **Protobuf over WebSocket** | Efficient binary framing, cross-language SDK compatibility |
