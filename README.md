# Flux

Unified data infrastructure for event streaming, CDC, and schema management.

## The Problem

Production data infrastructure is typically several independent systems: Kafka for transport, Debezium + Connect for CDC, a schema registry for compatibility, Avro/Parquet/Iceberg for serialization and table formats. Each solves a narrow problem. Together they require ZooKeeper (or KRaft), Connect clusters, a separate registry service, format converters, and catalog sync.

Schema management is particularly fragmented. Confluent Schema Registry is a separate service that enforces compatibility at registration time but not on the broker. Clients manually look up schema IDs. When a field is renamed or removed, the migration happens outside the system — the registry has no concept of renames or deletions. The schema is an artifact managed alongside your code rather than derived from it.

Flux unifies event ingestion, transport, schema management, and (planned) CDC and cataloging in a single binary. Schemas are defined as native language types — annotated dataclasses in Python, annotated POJOs in Java — and the SDK generates Avro schemas, handles serialization, and tracks evolution (field renames and deletions) in the schema metadata. These concerns share metadata, storage, and failure semantics; unifying them reduces operational surface area.

## What Flux Does

Disaggregated event bus: stateless Rust brokers, S3 for data, Postgres for metadata. Writers send records over WebSocket. Brokers buffer, compress (ZSTD), and flush to S3 as FL files. A single Postgres transaction per flush commits offsets, segment index, and writer dedup state atomically. Readers fetch segments via S3 range reads.

Key properties:
- **Exactly-once writer-to-storage** — idempotent writers with LRU + Postgres dedup
- **Gap-free offsets** — atomic allocation per partition in the flush transaction
- **CRC-verified storage** — per-segment CRC32 in FL files, checked on every read
- **Stateless brokers** — all durable state in Postgres; any broker serves any client

## Architecture

```
                     ┌──────────────────────────────────┐
  Writers ──WS──►    │          Broker (Rust)           │
                     │                                  │
                     │  buffer → FL (ZSTD+CRC32)     │
                     │      │              │            │
                     │      ▼              ▼            │
                     │     S3           Postgres        │
                     │  (segments)   (offsets, index,   │
                     │               dedup, groups,     │
                     │               schemas, auth)     │
                     │      │              │            │
                     │      ▼              ▼            │
  Readers ◄──WS──   │  range read    segment lookup    │
                     └──────────────────────────────────┘
```

What this replaces:

| Flux component         | Replaces                          |
|----------------------------|-----------------------------------|
| Broker (write/read path)   | Kafka/Redpanda brokers            |
| Schema registry            | Confluent Schema Registry         |
| FL storage on S3         | Kafka log segments + tiered storage |
| CDC ingestion (planned)    | Debezium + Kafka Connect          |
| Data catalog (planned)     | Separate catalog services         |
| Iceberg sink (planned)     | Kafka Connect S3/Iceberg sinks    |

## What's Built Today

**Exactly-once append.** Monotonic writer sequence numbers with two-tier dedup (LRU in-memory + Postgres). Retries on backpressure or disconnect are safe.

**Pipelined flush.** Buffer records from multiple writers, merge by (topic, partition, schema), compress into FL (ZSTD + CRC32), write to S3, commit offsets + index + dedup in one Postgres transaction. One flush runs while the next batch fills.

**Reader groups.** Broker-driven offset-range dispatch via Postgres. Readers poll for work, broker hands out leased offset ranges. Dead readers detected via heartbeat expiry; expired ranges are re-dispatched.

**Schema registry.** Avro backward compatibility validation at registration time. Built-in Avro deserializer (5-9x faster than apache-avro, 1.5 GiB/s) with zero-copy and bump allocation.

**Auth.** API key authentication with ACLs on the admin API.

**CLI.** Topic/schema management, record writes, live-tail with TUI or JSON output.

**Observability.** Prometheus metrics and OpenTelemetry tracing.

## SDKs

Rust, Java, and Python. Binary wire protocol over WebSocket. Idempotent writes, consumer groups, automatic retry with backoff.

### Schema-as-Code

Define schemas as native types. The SDK generates Avro schemas, serializes/deserializes, and tracks evolution (renames, deletions). No separate schema files or manual registry calls.

**Python** — `@schema` decorator on dataclasses:

```python
from flux import schema, NonNull, Int32
from dataclasses import dataclass
from typing import Annotated

@schema(
    namespace="com.example",
    renames={"old_name": "new_name"},  # generates Avro alias
    deletions=["removed_field"],       # tracked in schema metadata
)
@dataclass
class Event:
    id: Annotated[str, NonNull]
    new_name: int
    count: Annotated[int, Int32]       # Avro int instead of default long
    tags: list[str]                    # nullable by default

event = Event(id="evt-1", new_name=42, count=7, tags=["a"])
data: bytes = event.to_bytes()                # schemaless Avro binary
restored: Event = Event.from_bytes(data)
schema_json: str = Event.schema_json()        # for registry upload
```

**Java** — `@FluxSchema` annotation on POJOs:

```java
@FluxSchema(
    namespace = "com.example",
    renames = {@Rename(from = "old_name", to = "newName")},
    deletions = {"removedField"}
)
public class Event {
    @NonNull public String id;
    public int newName;
    public List<String> tags;          // nullable by default
}

byte[] data = Schemas.toBytes(event);          // schemaless Avro binary
Event restored = Schemas.fromBytes(Event.class, data);
String json = Schemas.schemaJson(Event.class); // for registry upload
```

Both generate Avro schemas with aliases for renamed fields and `flux.deletions` metadata for dropped fields. The registry's backward compatibility check passes across renames and deletions.

### Writer API

Connect over WebSocket, get a unique writer ID, append with automatic idempotency (`append_seq` + broker dedup). Backpressure retries with exponential backoff.

**Python:**

```python
async with Writer.connect("ws://localhost:9000") as writer:
    ack = await writer.send_one(topic_id=1, partition_id=0, schema_id=100,
                                key=b"user-1", value=event.to_bytes())
    # ack.start_offset, ack.end_offset
```

**Java:**

```java
try (Writer writer = Writer.connect("ws://localhost:9000")) {
    BatchAck ack = writer.sendOne(1, 0, 100, "user-1".getBytes(),
                                  Schemas.toBytes(event));
}
```

**Rust:**

```rust
let writer = Writer::connect("ws://localhost:9000").await?;
let ack = writer.append_one(TopicId(1), PartitionId(0), SchemaId(100),
                            Some(Bytes::from("user-1")), event_bytes).await?;
```

### Reader API

Join consumer groups with lease-based partition assignment. The SDK handles heartbeats, rebalancing, and offset tracking. Poll returns records grouped by partition and schema ID.

**Python:**

```python
config = ReaderConfig(url="ws://localhost:9000", group_id="my-group", topic_id=1)
async with GroupReader.join(config) as reader:
    reader.start_heartbeat()
    async for results in reader.poll_loop():
        for result in results:
            for record in result.records:
                event = Event.from_bytes(record.value)
        await reader.commit()
```

**Java:**

```java
ReaderConfig config = new ReaderConfig()
    .url("ws://localhost:9000").groupId("my-group").topicId(1);
try (GroupReader reader = GroupReader.join(config)) {
    reader.startHeartbeat();
    List<PartitionResult> results = reader.poll();
    for (PartitionResult r : results)
        for (Record rec : r.getRecordsList())
            Event event = Schemas.fromBytes(Event.class, rec.getValue().toByteArray());
    reader.commit();
}
```

**Rust:**

```rust
let reader = GroupReader::join(config).await?;
reader.start_heartbeat();
let results = reader.poll().await?;
for result in &results {
    for record in &result.records {
        // deserialize record.value
    }
}
reader.commit().await?;
```

## Roadmap

- **CDC ingestion** — database change capture without Debezium/Connect
- **Data catalog** — unified metadata across topics, schemas, consumers
- **Iceberg sink** — streaming to lakehouse (Avro→Arrow→Parquet infra exists in `flux-core`)
- **JS SDK**, multi-topic reader groups, quotas, multi-tenancy

## Quick Start

### 1. Start PostgreSQL

```bash
cd docker
docker-compose up -d
```

### 2. Start the broker

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux
cargo run -p flux-broker --bin flux-broker
```

The broker starts:
- WebSocket on `127.0.0.1:9000` (data plane)
- Admin HTTP on `127.0.0.1:9001` (management)

### 3. Bootstrap an API key

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flux
cargo run -p flux-cli -- bootstrap
```

Export the printed key:

```bash
export FLUX_API_KEY=tb_...
```

### 4. Create a topic, write, and tail

```bash
flux topic create my-events --partitions 3
flux write --topic my-events '{"name":"alice"}'
flux tail --topic my-events
```

See [crates/flux-cli/README.md](crates/flux-cli/README.md) for the full CLI reference.

## Project Structure

| Crate / Directory | Purpose |
|-------------------|---------|
| `crates/flux-broker` | Broker binary: WebSocket server, flush pipeline, coordinator |
| `crates/flux-sdk` | Rust SDK: Writer, GroupReader |
| `crates/flux-core` | Shared types, Avro engine, Arrow/Parquet converters |
| `crates/flux-schema` | Schema registry client and compatibility logic |
| `crates/flux-wire` | Binary wire protocol encoding/decoding |
| `crates/flux-common` | Shared utilities |
| `crates/flux-cli` | CLI tool (topic, schema, write, tail) |
| `sdks/java` | Java SDK |
| `sdks/python` | Python SDK |

## License

Flux is dual-licensed:

- **Open source:** [AGPL-3.0-only](LICENSE) — free for open-source use under
  the terms of the GNU Affero General Public License v3.0.
- **Commercial:** A commercial license is available for use cases where AGPL
  obligations are not feasible. Contact licensing@flux.io for details.

All contributions require agreeing to our [CLA](CLA.md). See
[CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Links

- [logic.md](logic.md) — failure-tolerance algorithms (idempotent append, flush pipeline, reader groups)
- [eventbus_comparison.md](eventbus_comparison.md) — detailed comparison with Kafka, Redpanda, WarpStream, Bufstream, Ursa
