#set page(
  paper: "us-letter",
  margin: (x: 0.75in, y: 0.85in),
)

#set text(size: 10pt)
#set par(justify: true, leading: 0.58em)
#set heading(numbering: "1.")

#align(center)[
  #text(weight: "bold", size: 18pt)[Flux: A Transaction-Indexed Object-Storage Event Bus]
  #v(0.35em)
  #text(size: 10pt)[Semantics, Availability Envelope, and Cross-System Comparison]
  #v(0.55em)
  #text(size: 9pt)[Anonymous Authors]
]

#v(0.9em)

*Abstract.*
Flux is an event bus architecture that separates payload durability from ordering and coordination: payload bytes are stored in object storage, while offsets, deduplication state, and reader-group ownership are stored in a transactional metadata service. This paper specifies the implemented data-plane and control-plane algorithms, states formal safety properties, and delineates non-guarantees. It also compares Flux with Kafka, Redpanda, and Bufstream across dimensions that matter in production design reviews: region-failure availability posture, exactly-once scope, schema enforcement and evolution, type-system flexibility, and consumer-group behavior. The emphasis is implementation-grounded semantics.

#v(0.7em)
#set page(columns: 2)

= Introduction

Most event buses collapse durability, ordering, and coordination into one replicated-log substrate. Flux uses a split architecture: object storage for payload durability and transactional metadata for ordering and group state. This decomposition yields a simple broker role and explicit transaction boundaries, but it creates a two-system failure boundary between payload and metadata commits.

This paper focuses on concrete claims that can be tied to implementation behavior.

Contributions:

- A formal model of append publication based on object persistence followed by transactional metadata commit.
- Code-grounded algorithms for append, read, and reader-group coordination.
- Proof sketches for implemented safety properties and explicit non-guarantees.
- A multi-dimensional comparison with Kafka, Redpanda, and Bufstream.

== Claim Matrix

#table(
  columns: (1.3fr, 2.35fr, 1.65fr),
  align: left + top,
  inset: 5pt,
  stroke: 0.35pt,
  table.header(
    [*Dimension*],
    [*Scope in This Paper*],
    [*Status*],
  ),
  [Correctness],
  [Partition offset integrity, append acknowledgment discipline, writer retry idempotence, lease-based ownership convergence.],
  [Stated with proof sketches],
  [Availability],
  [Region-failure posture via deployment envelope assumptions over metadata HA and object-store durability.],
  [Architecture-level analysis],
  [Exactly-once],
  [Writer-sequence idempotence and replay handling for one writer identity.],
  [Global EOS excluded],
  [Performance],
  [Methodology and measurement plan.],
  [No throughput-superiority claim],
)

= System Model

== Components

Flux has three runtime roles:

- *Broker:* stateless WebSocket endpoint for append, read, and reader-group APIs.
- *Object store:* durable batch payloads addressed by object key and byte range.
- *Metadata store:* transactional authority for partition offsets, batch index rows, writer dedup state, and reader-group leases.

== Data Model

Each successful flush publishes:

- object payload bytes,
- batch index rows `(topic, partition, schema, [start,end), key, byte_range, checksum)`,
- updated `next_offset` per partition,
- updated writer dedup state `(writer_id, last_seq, last_acks)`.

Offsets use half-open intervals `[start, end)`. `next_offset` is the next free logical slot.

== Availability Envelope

The broker layer is stateless; availability is dominated by metadata and object-store availability.

Deployment intent:

- metadata on a managed HA service with automatic write-leader failover (for example Aurora PostgreSQL-compatible deployments or Spanner-class services),
- object storage with cross-zone and optional cross-region durability (for example S3 or GCS classes configured for regional goals),
- brokers deployed in multiple regions behind failover-capable ingress.

Under this envelope, zone failure is expected to be transparent, and region failure tolerance depends on metadata writer failover time and object-store replication policy.

Artifact note: the current implementation uses PostgreSQL interfaces and an S3-compatible object-store abstraction; cross-region behavior is therefore deployment-configured, not hardcoded.

== Fault Model

Assumptions:

- crash-stop brokers,
- transient metadata-store and object-store failures,
- transient network partitions and delay.

No atomic distributed transaction spans object storage and metadata store.

== Schema and Type Model

Implemented behavior separates transport typing from payload typing:

- Wire protocol: Protobuf messages for control and RPC framing.
- Payload records: opaque bytes labeled by `schema_id` in each batch.
- Broker append path: no inline schema-registry lookup or compatibility check.
- Separate schema service: Avro schema registry with content-addressed dedup and backward-compatibility checks (including Iceberg-style union constraints).

This means schema governance is available but not in the append critical path of the broker.

= Data Plane Algorithms

== Append State

Append execution uses four state machines:

- in-flight coalescing keyed by `(writer_id, append_seq)`,
- dedup cache plus durable writer state,
- merge buffer keyed by `(topic, partition, schema)`,
- backpressure gate based on high/low-water thresholds.

#figure(
  caption: [Algorithm A1: A(w,s,B) append admission and dedup path.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
*Inputs:* writer w, sequence s, batches B.
*State:* in-flight map I, dedup state D, queue Q.

1. If ACL denies (w,B), return E_auth.
2. If (w,s) exists in I, wait on the existing outcome and return it.
3. If backpressure gate is active, return E_backpressure.
4. Query dedup state: d := D.check(w,s).
5. If d = duplicate(acks), return success with prior acks.
6. If d = stale and there is no higher in-flight sequence for w, return E_stale_seq.
7. If dedup lookup errors, continue (fail-open path).
8. Enqueue (w,s,B) into Q, await flush outcome.
9. On success, update D with (w,s,acks) and return success; otherwise return retryable internal error.
],
  ),
)

Code-grounded detail: stale sequences are normally rejected, but a stale request can proceed when a higher sequence for the same writer is already in flight.

#figure(
  caption: [Algorithm A2: F() flush and commit order.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
*Trigger set:* size threshold, dwell-time threshold, explicit flush, periodic tick.

1. Drain buffer into batch set U; if U is empty, stop.
2. Encode U into one payload object and persist to object storage.
3. If object write fails, fail pending requests in U.
4. Begin metadata transaction.
5. Aggregate per-partition record deltas.
6. Upsert partition_offsets with additive deltas and return updated next_offset.
7. Derive contiguous ranges for each batch in partition order.
8. Insert batch index rows into topic_batches.
9. Upsert latest writer dedup rows into writer_state.
10. Commit transaction.
11. On commit success, compute and return acknowledgments.
12. On commit failure, return errors; payload object may be orphaned until cleanup.
],
  ),
)

== Read Path

Read is metadata-first and object-range-second. For each requested partition, the broker queries at most 10 index rows with `end_offset > requested_offset`, then range-reads corresponding bytes and verifies checksums.

#figure(
  caption: [Algorithm R: R(Q) read with schema-aware grouping.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
*Input:* read vector Q = {(topic,partition,offset,max_bytes)}.

1. For each query q in Q, scan index rows ordered by start_offset, limit 10.
2. Fetch high_watermark := next_offset(topic,partition).
3. For each index row, range-read bytes and verify checksum while decoding records.
4. Skip records below requested offset.
5. Append decoded records until byte budget is reached.
6. Emit results grouped by contiguous schema-id runs; include high_watermark on each emitted partition result.

high_watermark is a visibility frontier, not a returned-record count.
],
  ),
)

= Control Plane: Reader Groups

Reader-group coordination is lease-based and generation-fenced in metadata tables.

- join_group: register member, bump generation, compute deterministic range assignment from sorted readers, claim unowned/expired leases.
- heartbeat: renew membership and lease TTL, evict expired members, bump generation on shrink.
- rejoin: reconcile owned vs target partitions.
- commit_offset: conditional update requiring owner and generation equality.

#figure(
  caption: [Algorithm G: J/H/RJ/C group operations.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
*Join J(g,t,r)*
1. Ensure group rows exist for (g,t).
2. Upsert member heartbeat for r.
3. Increment group generation.
4. Compute target(r) by deterministic range assignment over sorted members.
5. Claim only partitions whose lease is unowned or expired.

*Heartbeat H(g,t,r,gen)*
1. Refresh member heartbeat.
2. Renew leases owned by r.
3. Remove expired members and release their leases.
4. If membership changed, increment generation and return rebalance-needed.

*Rejoin RJ(g,t,r,gen)*
1. If gen is stale, return rebalance-needed.
2. Compute target(r); read owned(r).
3. Release owned - target, renew owned inter target, claim target - owned when claimable.

*Commit C(g,t,r,gen,p,off)*
1. Update committed offset iff owner is r and both assignment generation and group generation equal gen.
2. Otherwise classify as stale-generation or not-owner.
],
  ),
)

SDK note: auto-rebalancing behavior is client-assisted. The GroupReader starts a heartbeat loop, reacts to rebalance-needed responses, then executes rejoin with delay and retries.

Artifact caveats:

- `join_group` currently consumes only the first topic ID in the request.
- `commit` requests are handled entry-by-entry; multi-partition commit response is not atomic.

= Correctness Guarantees

Guarantees are under the crash-stop model above.

== Theorem 1 (Partition Offset Integrity)

Committed offsets in each partition are contiguous and non-overlapping.

*Proof sketch.* Offset deltas are aggregated and committed in one transaction. Batch ranges are derived from transactional `next_offset` results. Failed transactions publish no new offsets or index rows.

== Theorem 2 (Append Ack Discipline)

A successful append acknowledgment implies durable payload bytes and committed metadata index rows.

*Proof sketch.* Acknowledgments are distributed only after object write succeeds and metadata transaction commits.

== Theorem 3 (Writer Retry Idempotence)

For a fixed writer identity, replaying a sequence number resolves to the original logical append.

*Proof sketch.* Duplicate requests are either coalesced in-flight or resolved from durable writer state (`last_seq`, `last_acks`).

== Theorem 4 (Eventual Single Owner per Partition)

With bounded skew, lease expiry, and stable membership, ownership converges to one reader per `(group, topic, partition)`.

*Proof sketch.* Claim predicates require unowned/expired leases; generation fencing prevents stale owners from committing post-rebalance.

= Explicit Non-Guarantees

- No atomic two-phase commit spans object store and metadata store.
- Metadata failure after payload write can leave orphan payload objects.
- Exactly-once is writer-scoped idempotence, not global cross-writer EOS.
- Multi-partition commit requests can partially succeed.
- Relative order across independently merged schema buckets requires application-level constraints.

= Cross-System Dimension Matrix

#set page(columns: 1)

#table(
  columns: (1.2fr, 1.45fr, 1.45fr, 1.45fr, 1.45fr),
  align: left + top,
  inset: 4pt,
  stroke: 0.35pt,
  table.header(
    [*Dimension*],
    [*Flux*],
    [*Kafka*],
    [*Redpanda*],
    [*Bufstream*],
  ),
  [Region-failure availability],
  [Stateless brokers; availability depends on metadata-writer failover and object-store durability policy. Target envelope uses managed metadata HA and multi-region-capable object storage.],
  [Typical single-cluster deployment is zone-tolerant; region-failure continuity generally uses cross-cluster replication/mirroring workflows.],
  [Single cluster uses Raft quorum; region DR uses cross-cluster mechanisms such as shadowing/restore workflows.],
  [Documented multi-region active-active mode with pluggable metadata backend; project materials claim tolerance to full-region outage when configured accordingly.],
  [Exactly-once scope],
  [Writer-sequence idempotence via `(writer_id, append_seq)` and durable writer state.],
  [Idempotent producer and transactions provide EOS patterns, including stream-processing integrations.],
  [Kafka-compatible transactions and EOS semantics.],
  [Kafka-compatible transactions and EOS are documented.],
  [Schema enforcement location],
  [Broker append path treats `schema_id` as an opaque label; no inline registry check in append fast path.],
  [Kafka core treats record payload as bytes; schema contracts are typically external.],
  [Schema Registry integration available; enforcement depends on client and registry usage.],
  [Broker-side semantic validation is documented when schema provider and validation mode are enabled.],
  [Schema evolution],
  [Separate schema service supports backward compatibility checks for Avro schemas.],
  [Policy is external to Kafka core and depends on selected registry tooling.],
  [Registry supports versioned schemas and compatibility controls.],
  [Confluent-compatible registry path and BSR-based governance workflows are documented.],
  [Type-system flexibility],
  [Wire RPC is Protobuf; payload is opaque bytes keyed by `schema_id`; current registry artifact is Avro-focused.],
  [Payload format defined by serializers; ecosystem commonly uses Avro, Protobuf, and JSON with external tooling.],
  [Registry API reports AVRO, PROTOBUF, and JSON support.],
  [Kafka bytes model plus Protobuf-first governance and validation workflows in project docs.],
  [Consumer-group algorithm],
  [Deterministic range assignment over sorted members, lease claims, generation fencing, and client-driven heartbeat/rejoin loops.],
  [Coordinator-based group management with pluggable assignors, including cooperative sticky options.],
  [Kafka-compatible group protocol and offset management.],
  [Kafka-protocol compatibility implies Kafka client group behavior and rebalancing model.],
)

#set page(columns: 2)

Interpretation notes:

- Flux and Bufstream are close architectural peers in separating payload durability from metadata coordination.
- Flux's distinctive focus in this artifact is transactional offset-index publication with explicit writer-state dedup in metadata tables.
- Region-failure behavior for all systems is deployment-dependent; matrix entries describe documented mechanisms, not universal defaults.

= Evaluation Priorities

A VLDB-grade empirical section should prioritize:

- *Availability under regional faults:* measure write/read continuity through metadata-writer failover and object-store regional impairments.
- *Exactly-once boundaries:* quantify duplicate/loss behavior under client retry, broker crash, and commit-path failures.
- *Schema-governance cost:* compare append-path latency with and without broker-inline schema validation.
- *Group stability:* churn experiments on rebalance latency, partition movement, and commit fencing under load.

Current artifact evidence supports functional safety claims through integration and fault-oriented tests, but does not yet establish comparative throughput leadership.

= Threats to Validity

- Metadata failover behavior depends on the selected managed service and topology.
- Object-store replication policy directly affects region-level recovery point objectives.
- Consumer-group outcomes depend on client heartbeat cadence, timeout settings, and retry policies.
- Cross-system comparisons are sensitive to configuration parity and workload shape.

= Related Work

Kafka established the partitioned replicated-log model and later idempotent/transactional semantics for stronger delivery guarantees [1, 2, 3]. Redpanda provides Kafka-protocol compatibility with Raft-backed internals, Kafka-compatible transactions, and schema-registry support [4, 5, 6]. Bufstream is an object-storage-centric Kafka-compatible system with documented EOS support, schema-governance features, and multi-region claims [7, 8, 9]. Flux contributes a code-explicit transactional offset-index model with stateless brokers and metadata-centric fencing.

= Conclusion

Flux is best understood as a metadata-centric event bus over object-stored payloads. Its strongest implemented properties are partition-level offset integrity, post-commit acknowledgment discipline, writer-scoped idempotence, and generation-fenced reader ownership. The main open systems problem remains the non-atomic boundary between payload and metadata stores. Future work should pair regional-failure experiments with formal recovery invariants and side-by-side benchmarks under matched workloads.

= Appendix: Core Algorithms and Failure Walkthroughs

This appendix provides implementation-oriented pseudocode in the code-listing style, aligned to the current writer SDK, broker, and reader SDK behavior.

== A.1 Writer Ack and Retry

#figure(
  caption: [Algorithm AW1: Writer append with retry and backpressure handling.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AW1: AppendBatchWithRetry(batches)
append_seq <- atomic_fetch_add(next_seq, 1)
retries <- 0
backoff <- initial_backoff

loop:
  result <- SendAppendOnce(append_seq, batches)
  if result == OK(acks):
    return OK(acks)

  if result == BACKPRESSURE:
    if retries >= max_retries:
      return BACKPRESSURE
    sleep(backoff)
    backoff <- min(backoff * 2, max_backoff)
    retries <- retries + 1
    continue

  return result
```
    ],
  ),
)

*Failure Walkthrough.*
- Backpressure is retried with exponential backoff using the same `append_seq`.
- Timeout or disconnection returns an error to the caller; application-level retry can resend with the same writer identity and sequence semantics.
- Late responses after local timeout are ignored by the pending map once the pending entry is removed.

#figure(
  caption: [Algorithm AW2: Single append request lifecycle over WebSocket.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AW2: SendAppendOnce(append_seq, batches)
acquire in_flight_permit
request <- AppendRequest(writer_id, append_seq, batches)
payload <- encode(request)

create one-shot channel (tx, rx)
pending[append_seq] <- tx

if websocket_send(payload) fails:
  remove pending[append_seq]
  return CONNECTION_ERROR

wait timeout for rx:
  if rx returns OK(acks):
    return OK(acks)
  if rx closed:
    return DISCONNECTED
  if timeout:
    remove pending[append_seq]
    return TIMEOUT
```
    ],
  ),
)

*Failure Walkthrough.*
- Pending state is cleaned on send failure and timeout.
- The writer uses response demultiplexing keyed by `append_seq`.

== A.2 Broker Append Admission and In-Flight Coalescing

#figure(
  caption: [Algorithm AB1: Broker append handler with dedup and queueing.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AB1: HandleAppend(req = (writer_id, append_seq, batches))
if ACL denies req:
  return AUTHZ_DENIED

decision <- InFlightDecision(writer_id, append_seq)
if decision == WAIT(rx):
  return wait(rx)

if backpressure_active:
  CompleteInFlight(writer_id, append_seq, BACKPRESSURE)
  return BACKPRESSURE

dedup <- DedupCheck(writer_id, append_seq)
if dedup == DUPLICATE(acks):
  CompleteInFlight(writer_id, append_seq, OK(acks))
  return OK(acks)

if dedup == STALE:
  if HasHigherInFlightSequence(writer_id, append_seq):
    // current implementation allows progress in this case
    continue
  CompleteInFlight(writer_id, append_seq, STALE_SEQUENCE)
  return STALE_SEQUENCE

if dedup == ERROR:
  // fail-open path
  continue

enqueue FlushCommand::Insert(req, response_tx)
if enqueue fails:
  CompleteInFlight(writer_id, append_seq, INTERNAL_ERROR)
  return INTERNAL_ERROR

wait response_rx:
  if OK(acks):
    DedupUpdate(writer_id, append_seq, acks)
    CompleteInFlight(writer_id, append_seq, OK(acks))
    return OK(acks)
  else:
    CompleteInFlight(writer_id, append_seq, INTERNAL_ERROR)
    return INTERNAL_ERROR
```
    ],
  ),
)

*Failure Walkthrough.*
- Duplicate concurrent requests coalesce at in-flight state and share one outcome.
- Dedup lookup failure does not block append (fail-open), which favors availability over strict reject-on-dedup-error behavior.
- Stale sequence handling has an exception path when a higher sequence is already in flight.

== A.3 Broker Flush, Commit, and Ack Distribution

#figure(
  caption: [Algorithm AB2: Flush scheduler and commit pipeline.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AB2: FlushLoop()
state: buffer, backpressure_gate, command_queue, periodic_timer

on Insert(req):
  buffer.insert(req)
  if buffer >= high_water:
    backpressure_gate <- true

flush when:
  command == ForceFlush
  or buffer.should_flush()
  or timer tick with non-empty buffer

on Shutdown:
  flush remaining buffer and exit


Algorithm AB3: FlushBuffer()
drain <- buffer.drain()
if drain empty:
  return

payload <- FLEncode(drain.batches)
if ObjectPut(payload) fails:
  fail all pending writers in drain
  return

result <- CommitBatchTx(drain)
if result == ERROR:
  fail all pending writers in drain
  // object may be orphaned until cleanup
  return

DistributeAcks(drain, result.segment_offsets)
if buffer <= low_water:
  backpressure_gate <- false


Algorithm AB4: CommitBatchTx(drain)
begin tx
  aggregate deltas per (topic, partition)
  upsert partition_offsets with additive deltas and return updated next_offset
  derive contiguous [start, end) for each drained batch
  insert topic_batches rows with object key + byte ranges + checksum
  upsert writer_state(last_seq, last_acks) for latest sequence per writer
commit tx
return segment_offsets
```
    ],
  ),
)

*Failure Walkthrough.*
- Object-write failure prevents metadata publication and success acknowledgments.
- Transaction failure after object write yields no success acknowledgments; payload cleanup is deferred.
- Acknowledgments are emitted only after successful transaction commit.
- Backpressure is set at high-water and released at low-water after successful drain progression.

== A.4 Broker Read Path

#figure(
  caption: [Algorithm BR1: Broker metadata-first read.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm BR1: ProcessRead(reads)
results <- []

for each read in reads:
  rows <- SELECT topic_batches
          WHERE end_offset > read.offset
          ORDER BY start_offset
          LIMIT 10

  high_watermark <- SELECT partition_offsets.next_offset

  grouped <- []
  current_schema <- none
  current_records <- []
  total_bytes <- 0

  for each row in rows:
    bytes <- ObjectGetRange(row.s3_key, row.byte_offset, row.byte_length)
    records <- FLReadSegment(bytes, verify_crc = true)
    skip <- max(0, read.offset - row.start_offset)

    rotate schema group if schema changes
    append records after skip until total_bytes >= read.max_bytes
    stop if byte budget reached

  emit grouped partition results with high_watermark
  if no records, emit empty partition result

return results
```
    ],
  ),
)

*Failure Walkthrough.*
- Range-read or decode failure currently fails the read request path and returns a read error response.
- `high_watermark` can exceed returned record count when byte budget truncates output.
- Result grouping may emit multiple partition result entries for one partition when schema ID boundaries are crossed.

== A.5 Reader Poll, Heartbeat, and Rejoin

#figure(
  caption: [Algorithm AR1: GroupReader poll and offset tracking.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AR1: Poll()
require reader_state == ACTIVE
assignments <- current assigned partitions
if assignments empty:
  return []

for each assignment a:
  read_offset <- local_offsets[a.partition] default a.committed_offset
  build PartitionRead(topic, partition, read_offset, max_bytes)

results <- SendReadRequest(reads)

for each partition result r in results:
  if r.records non-empty:
    local_offsets[r.partition] <- local_offsets[r.partition] + len(r.records)

return results
```
    ],
  ),
)

*Failure Walkthrough.*
- Offset advancement is client-side and based on returned record count.
- Commit is explicit and separate; uncommitted progress may be replayed on restart/rejoin.

#figure(
  caption: [Algorithm AR2: Heartbeat loop with rebalance handling.],
  placement: top,
  block(
    inset: 6pt,
    stroke: 0.35pt + luma(200),
    radius: 2pt,
    [
```text
Algorithm AR2: HeartbeatLoop()
while running:
  sleep(heartbeat_interval)
  hb <- SendHeartbeat(group, topic, reader, generation)

  if hb.status == OK:
    continue

  if hb.status == REBALANCE_NEEDED:
    DoRejoin(hb.generation)
    continue

  if hb.status == UNKNOWN_MEMBER:
    DoJoin()


Algorithm AR3: DoRejoin(new_generation)
state <- REBALANCING
best effort Commit()
sleep(rebalance_delay)

gen <- new_generation
loop:
  rj <- SendRejoin(group, topic, reader, gen)
  if rj.status == REBALANCE_NEEDED:
    gen <- rj.generation
    sleep(rebalance_delay)
    continue

  assignments <- rj.assignments
  local_offsets <- committed offsets from assignments
  generation <- rj.generation
  state <- ACTIVE
  return
```
    ],
  ),
)

*Failure Walkthrough.*
- `UNKNOWN_MEMBER` triggers a fresh join, which can reset local assignment state.
- Commit during rebalance/leave is best-effort; failures are logged and processing continues.
- Rejoin can spin through multiple generations under sustained group churn.

= References

- [1] Apache Kafka Design Documentation. https://kafka.apache.org/documentation/
- [2] Apache Kafka Design Notes on Idempotence and Transactions. https://kafka.apache.org/22/design/design/
- [3] Apache Kafka Consumer Assignment and Config Documentation. https://kafka.apache.org/41/configuration/consumer-configs/
- [4] Redpanda Kafka Compatibility. https://docs.redpanda.com/25.2/develop/kafka-clients/
- [5] Redpanda Transactions. https://docs.redpanda.com/current/develop/transactions/
- [6] Redpanda Schema Registry API and Supported Types. https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/
- [7] Bufstream Quickstart and Broker-Side Validation. https://buf.build/docs/bufstream/quickstart/
- [8] Bufstream Kafka Compatibility and EOS Feature List. https://buf.build/docs/bufstream/kafka-compatibility/conformance/
- [9] Bufstream Multi-Region Active-Active Announcement. https://buf.build/blog/bufstream-multi-region
- [10] Kafka Geo-Replication and Mirroring. https://kafka.apache.org/38/operations/geo-replication-cross-cluster-data-mirroring/
- [11] Redpanda Shadowing and Disaster Recovery Docs. https://docs.redpanda.com/redpanda-cloud/manage/disaster-recovery/shadowing/overview/
- [12] Kafka Multi-Tenancy Note on External Schema Registry. https://kafka.apache.org/33/operations/multi-tenancy/
