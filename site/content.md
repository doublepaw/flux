# Flux

## Event streaming, simplified.

Flux replaces Kafka + Schema Registry + Iceberg pipelines with a single stateless binary.
No partitions to tune. No brokers to replicate. No connectors to deploy.
Just S3, Postgres, and your code.

[Get Started](#getting-started) | [Docs](#)

---

## Simpler than Kafka

Kafka makes you think about partitions. How many? Which key? What happens when they're skewed? What about rebalancing?

Flux doesn't have partitions in the traditional sense. The broker dispatches records to consumers directly — no static partition assignment, no consumer group rebalancing storms, no hot partitions.

| | Kafka | Flux |
|---|---|---|
| **Consumer assignment** | Client-side partition assignment, rebalancing | Broker-side dispatch, no rebalancing |
| **Partition tuning** | Too few = hot spots, too many = overhead | No partition tuning required |
| **Infrastructure** | ZooKeeper/KRaft + replicated brokers + local disks | Stateless binary + S3 + Postgres |
| **Schema management** | Separate Schema Registry service | Built into the broker |
| **Iceberg ingestion** | Kafka Connect + Iceberg connector | Built into the broker |
| **CDC** | Debezium + Kafka Connect | Built in (coming soon) |
| **Exactly-once** | Careful config of `transactional.id`, `isolation.level`, idempotent producers | On by default |

---

## Schema as code

Define schemas as native types in your language. No `.proto` files, no Avro IDL, no build plugins, no code generation step.

Flux auto-infers Avro schemas from your types and handles evolution — renames, deletions, type widening — tracked explicitly in your code.

### Java

```java
@FluxSchema(namespace = "com.example")
public class OrderEvent {
    @NonNull public String orderId;
    public long amount;
    public List<String> tags;
}

// Serialize
byte[] data = Schemas.toBytes(event);

// Deserialize
OrderEvent restored = Schemas.fromBytes(OrderEvent.class, data);
```

Evolution is explicit:

```java
@FluxSchema(
    namespace = "com.example",
    renames = {@Rename(from = "old_name", to = "newName")},
    deletions = {"removedField"}
)
public class OrderEvent {
    @NonNull public String orderId;
    public long amount;
    public int newName;   // renamed from old_name
    // removedField — deleted, old data still readable
}
```

### Python

```python
@schema(namespace="com.example")
@dataclass
class OrderEvent:
    order_id: Annotated[str, NonNull]
    amount: int
    tags: list[str]

# Serialize
data: bytes = event.to_bytes()

# Deserialize
restored: OrderEvent = OrderEvent.from_bytes(data)
```

Evolution works the same way:

```python
@schema(
    namespace="com.example",
    renames={"old_name": "new_name"},
    deletions=["removed_field"],
)
@dataclass
class OrderEvent:
    order_id: Annotated[str, NonNull]
    amount: int
    new_name: int   # renamed from old_name
    # removed_field — deleted, old data still readable
```

### High-level client

Send and consume events with a single line of code:

```java
try (FluxClient client = FluxClient.connect(config)) {
    // Send
    client.send(new OrderEvent("abc", 100));

    // Consume
    client.consume(OrderEvent.class, "my-group", event -> {
        System.out.println(event.orderId);
    });
}
```

```python
async with FluxClient.connect(config) as client:
    await client.send(OrderEvent(order_id="abc", amount=100))

    async for event in client.consume(OrderEvent, group_id="my-group"):
        print(event.order_id)
```

---

## Iceberg ingestion, built in

Data appears in Iceberg tables within seconds of being written — not minutes, not hours.

The broker's flush loop pushes records directly into per-table Iceberg buffers (hot path). Schema evolution carries through automatically — rename a field in your code and it's renamed in Iceberg.

A catch-up path runs in the background, finding any un-ingested batches via anti-join and re-reading them from S3. No data loss, no manual intervention.

**No Debezium. No Kafka Connect. No Iceberg connector configs. It just works.**

---

## Built-in CDC *(coming soon)*

Capture database changes as first-class Flux events with full schema evolution. No separate connector infrastructure, no Debezium deployment, no Kafka Connect cluster.

One system for event streaming and CDC.

---

## 6x cheaper

Flux benchmarks at 5M events/sec on 8 cores. Core count matched at 32 vCPUs. Workload: 100,000 events/sec · 1 KB avg · 7-day retention · AWS us-east-1

| | Self-managed Kafka | Amazon MSK | Flux |
|---|---|---|---|
| **Brokers** | 8× m6i.xlarge (32 vCPUs) — $1,120/mo | 8× kafka.m5.xlarge (32 vCPUs) — $1,220/mo | 1× c6g.8xlarge (32 vCPUs) — $800/mo |
| **Storage** | 180 TB EBS gp3 (3× RF) — $14,400/mo | 180 TB provisioned (3× RF) — $18,000/mo | 60 TB S3 + requests — $1,580/mo |
| **Iceberg ingestion** | Kafka Connect (4 workers) — $240/mo | MSK Connect (4 MCU) — $320/mo | Built in |
| **Coordination & schema** | ZK + Schema Registry — $120/mo | Included + Glue (free) | Postgres (RDS) — $50/mo |
| **Total** | **~$15,900/mo** | **~$19,500/mo** | **~$2,430/mo** |

With matched core counts, compute costs are comparable ($1,120 vs $800). The gap is almost entirely storage: no 3× replication ($14,400 → $1,380) and no Iceberg connector infrastructure ($240–$320 → $0).

```
┌──────────┐     ┌──────────────────┐     ┌─────────────┐
│ Writers  │────▶│ Stateless Broker │────▶│     S3      │
└──────────┘     │   (Rust binary)  │     │  (storage)  │
                 └────────┬─────────┘     └─────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │    Postgres     │
                 │   (metadata)    │
                 └─────────────────┘
```

---

## Run anywhere

Flux supports S3, GCS, and ABFS. Run on AWS, GCP, or Azure — or all three.

The broker is the same stateless binary everywhere. Bring your own object store, point the broker at it, and go.

No cloud lock-in. No vendor-specific APIs. Disaggregated storage means you own your data.

---

## 5M events/second

A single Flux broker handles 5 million events per second.

This is powered by a custom Avro engine that's 5–9x faster than apache-avro:

| Payload | Flux | apache-avro | Speedup |
|---|---|---|---|
| Small | 341 ns | 3.05 µs | **8.9x** |
| Medium | 1.04 µs | 6.39 µs | **6.1x** |
| Large | 2.88 µs | 14.59 µs | **5.1x** |

**Throughput: 1.5–1.6 GiB/s** with ZSTD compression and CRC32 integrity checks on every segment.

Key optimizations:
- Zero-copy deserialization with arena allocation
- Schema-driven O(1) field lookups
- Varint decoding with inline fast paths
- Pre-computed schema graphs with cached field indices

---

## Built for correctness

Flux ships with a built-in Jepsen-style test suite — not as an afterthought, but as a core part of the system.

**17 chaos test scenarios** covering:
- Exactly-once semantics across producer kill/restart cycles
- Linearizability verification with full operation history tracking
- Crash recovery with watermark freshness guarantees
- Network partitions between brokers, clients, and storage
- Multi-broker coordination under concurrent failures
- Backpressure behavior under sustained overload
- Pipelined flush correctness with concurrent writers

**Fault injection framework** — every test runs against a faulty object store and database:
- S3 PUT/GET failures (transient and persistent)
- S3 network partitions (black-hole mode until healed)
- Configurable latency injection
- Database lock contention simulation
- Broker crash/restart cycles

**7 invariants verified after every test:**
1. All acknowledged writes are visible to readers
2. No two writes receive the same offset
3. High watermark never regresses
4. No duplicate records across the entire history
5. Sequential reads never skip offsets
6. Causal ordering: if write A completes before write B starts, A.offset < B.offset
7. Per-producer offsets are monotonically increasing

Every test is reproducible via seed. Every failure is a regression test.

Other systems discover correctness bugs in production. We find them before you do.

---

## Getting started

### 1. Start Postgres

```bash
cd docker && docker-compose up -d
```

### 2. Start the broker

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux
flux-broker
```

### 3. Create a topic and start streaming

```bash
# Bootstrap your API key
flux bootstrap
export FLUX_API_KEY=tb_...

# Create a topic
flux topic create my-events

# Write events
flux write --topic my-events '{"name": "alice", "amount": 100}'

# Tail events in real-time
flux tail --topic my-events
```

---

## Architecture

```
┌──────────┐                              ┌─────────────┐
│ Writers  │──── WebSocket (protobuf) ───▶│             │──── S3 PUT ────▶┌─────┐
│ (SDK)    │                              │   Broker    │                 │ S3  │
└──────────┘                              │ (stateless) │──── Postgres ──▶│     │
                                          │             │    (metadata)   └──┬──┘
┌──────────┐                              │             │                    │
│ Readers  │◀─── WebSocket (protobuf) ────│             │◀── S3 GET ────────┘
│ (SDK)    │                              │             │
└──────────┘                              │             │──── Iceberg ───▶┌─────────┐
                                          └─────────────┘    (hot path)  │ Iceberg │
                                                                         │ Tables  │
                                                                         └─────────┘
```

**Write path**: Append → in-flight dedup → buffer merge → FL file (ZSTD + CRC32) → S3 PUT → atomic Postgres commit → ack

**Read path**: Query segment index → S3 range reads → per-schema record grouping → dispatch

**Exactly-once**: Monotonic `append_seq` per writer + three-tier dedup (in-flight → LRU cache → Postgres)

**Storage format (FL)**: Multiple (topic, schema) segments per file. Per-segment ZSTD compression and CRC32 verification. Varint-encoded footer for efficient S3 range reads.
