# Design: The Iceberg Path

Status: P0 landed (2026-07-08) — snapshot-property watermarks (D1),
committer lease + epoch fencing (D2), REST catalog option (D3),
iceberg-rust 0.9.1; exactly-once verified by tests/exactly_once.rs.
P1+ remain as below. Originally proposed 2026-07, informed by a survey
of Ursa (VLDB'25 + 2025-26
posts), Bufstream, Confluent/WarpStream Tableflow, Redpanda Iceberg Topics,
AutoMQ Table Topics, Apache Fluss, Aiven's KIP-1150-adjacent RSM plugin, and
the mid-2026 state of apache/iceberg-rust. Sources in `docs/` commit message
and the research notes at the end.

## Goal

Every flux topic can materialize as an Apache Iceberg table that any engine
(Spark, Trino, DuckDB, Snowflake, Databricks) can query through a standard
REST catalog — no connectors, no ETL. Streaming consumers keep reading
through flux; analytics reads the same data as a table.

Non-goals (this design): upsert/CDC tables (append-only first; Iceberg v3
deletion vectors make upsert a clean later addition), multi-table
transactions, zero-copy "the Parquet is the log" (our FL segments are
row-format, so unlike Bufstream we always convert; the stream-backed
read-path integration is Phase 3).

## What already exists (`flux-iceberg`, ~2.6k lines, feature-gated)

- **Hot path**: the flush loop pushes committed segments into per-table
  buffers (128 MB / 60 s triggers); `TableWriter` converts
  Avro→Arrow→Parquet and appends via iceberg-rust 0.8 + `iceberg-catalog-sql`
  (same Postgres).
- **Catch-up path**: `iceberg_claims` table; a periodic task anti-joins
  `topic_batches` vs claims (`FOR UPDATE SKIP LOCKED`), re-reads missed
  segments from object storage. Stale-claim expiry.
- **Schema**: Avro registry-driven; `RecordConverter` handles multiple
  writer schemas; union-merge style evolution with nullable widening.

This skeleton matches the industry shape (Bufstream's archiver + reconciler)
and is worth keeping. The gaps below are what stands between it and
production.

## Gaps found (ranked)

1. **Exactly-once is broken across crashes.** `mark_completed` runs in a
   separate Postgres transaction *after* the Iceberg commit. A crash between
   them re-ingests those batches via catch-up → duplicate rows.
2. **No commit serialization.** Every broker runs its own buffer + writer;
   N brokers race optimistic catalog commits on the same table. Every system
   surveyed converged on a single committer per table (AutoMQ: coordinator on
   partition 0; Fluss: one commit operator + epoch fencing; Ursa: Oxia locks).
3. **Catalog isn't engine-readable.** `iceberg-catalog-sql` on flux's
   Postgres is invisible to Spark/Trino/DuckDB. The ecosystem is REST-first.
4. **Unpartitioned tables** — every query scans everything; streaming
   best practice is day/hour partitioning on ingest time.
5. **No schemaless mode, no DLQ.** Topics without a registered Avro schema
   error out; malformed records would poison ingestion. Redpanda's
   `<topic>~dlq` table and key/value fallback are table stakes.
6. **iceberg-rust 0.8** — 0.9.x added the retrying commit path
   (`commit.retry.*` properties), rolling partitioned writers, and the
   DataFusion integration. Upgrade unlocks most of this design.
7. **No lifecycle**: no snapshot expiry, no small-file compaction story, no
   FL retention tie-in.

## Design

### D1. Exactly-once via snapshot properties (the Fluss/Connect pattern)

The Iceberg snapshot itself becomes the source of truth for ingestion
progress, so data and bookkeeping commit atomically:

- Each commit writes summary properties:
  `flux.commit-id` (uuid), `flux.watermarks` = JSON
  `{ "<topic_id>": max_batch_id }` covering everything in this snapshot,
  carried forward cumulatively (bounded: one entry per topic in the table —
  one topic per table today, so one entry).
- On startup / before each commit, the committer reads the current
  snapshot's `flux.watermarks` and drops any claimed batch with
  `batch_id <= watermark` instead of writing it. Postgres
  `iceberg_claims` remains — but demoted to *work distribution and
  progress metrics*, no longer correctness.
- Result: crash after commit but before `mark_completed` → catch-up
  re-claims the batches → committer consults the snapshot watermark →
  skips them. Duplicates become impossible rather than unlikely.

### D2. Single committer per table, leased in Postgres

- A `pg_advisory_lock`-style lease (`iceberg_committers` table:
  table_ident, holder, lease_expiry, epoch) elects one committer per table.
  Any broker (or dedicated worker, see D6) can hold it; leases renew on
  heartbeat and expire like reader-group leases.
- **Epoch fencing**: the lease carries an epoch; the committer stamps
  `flux.committer-epoch` into each snapshot and aborts if it observes a
  newer epoch (Fluss's exact scheme). This makes a paused/partitioned
  ex-committer harmless.
- The committer batches one `fast_append` per table per interval —
  many Parquet files, one snapshot — keeping commit rate ~1/min/table
  (survey consensus: 1–5 min cadence, 32–128 MB files; metadata churn
  dominates below ~30 s). iceberg-rust 0.9's retrying commit handles the
  residual conflict case (external writers).

### D3. REST catalog first; Lakekeeper as the shipped default

- Upgrade to iceberg-rust **0.9.1+**; add `ICEBERG_CATALOG=rest|sql` with
  REST as the documented default (`iceberg-catalog-rest`, OIDC bearer/token
  auth). SQL catalog stays for dev/tests only.
- Helm gains an optional **Lakekeeper** deployment (single Rust binary +
  the Postgres we already ship) as the batteries-included catalog —
  chosen over Polaris for weight and Rust-native fit; Polaris/Glue/Unity/
  Snowflake Open Catalog are bring-your-own via the same REST protocol, so
  broker code doesn't change. (Matches the federate-to-the-customer's-
  catalog deployment model.)

### D4. Table shape

- Default partition spec: `day(_ingest_time)` (configurable per topic;
  hour for high-volume). System columns alongside the Avro-derived ones:
  `_offset` (BIGINT), `_ingest_time` (TIMESTAMPTZ), `_schema_id` (INT) —
  the Fluss/Aiven pattern that preserves stream lineage and enables
  offset-based handoff from table reads back to live stream reads.
- File sizing via `RollingFileWriter`, target 128 MB, row groups ~64 MB.
- Target format v2 now; adopt v3 features as iceberg-rust exposes them
  (VARIANT for schemaless topics, row lineage for CDC-out).

### D5. Schemaless mode and DLQ

- Topics without a registered schema materialize as
  `(_offset, _ingest_time, key BINARY, value BINARY)` — later VARIANT.
- Records that fail Avro decode against their declared schema route to
  `<table>__dlq` (offset, ingest_time, raw bytes, error) — gap-free main
  table offsets, inspectable failures (Ursa/Redpanda pattern).

### D6. Where it runs

- Phase 1 keeps ingestion in-broker (feature flag) — right for
  single-broker self-hosted deployments; the hot path avoids S3 re-reads.
- The claims + committer-lease design already makes a **dedicated
  worker** safe: `flux-broker --role=iceberg-worker` (or env) runs only
  catch-up + committer, scaling table materialization independently of
  brokers (Ursa's separate compaction service, in our clothes). No new
  protocol: workers read S3 + Postgres.

### D7. Lifecycle and maintenance

- **Snapshot expiry**: bound metadata growth by expiring snapshots older
  than N hours (default 24) — until iceberg-rust grows maintenance actions,
  do this via raw `TableUpdate` (remove-snapshots) on the committer's
  cadence; AutoMQ bounds at 1 h, Confluent caps at 100 snapshots.
- **Small-file compaction**: explicitly external in Phase 1-2 (document
  Spark/Trino `rewrite_data_files` or Lakekeeper-ecosystem jobs; compact
  only cold partitions). Revisit an embedded DataFusion-based compactor
  later (RisingWave proved it viable in Rust).
- **Freshness SLA**: export `flux_iceberg_lag_batches`/`_seconds` metrics;
  alert-only by default. Producer backpressure on lag (Redpanda's tradeoff)
  as an opt-in later.
- **FL retention tie-in (Phase 3)**: only GC FL segments whose batches are
  ≤ the table watermark + retention grace; `topic_batches` gains a
  `file_type` discriminator so old-offset reads can be served from Parquet
  (Ursa's stream-backed table duality — and the natural moment for an
  Arrow-based read API).

## Phases

- **P0 (correctness, ~small)**: iceberg-rust 0.9.1 upgrade; snapshot-property
  watermarks (D1); committer lease + epoch fencing (D2); REST catalog
  support (D3). Jepsen-style tests: crash between Parquet write and commit,
  between commit and mark_completed, dueling committers, catch-up dedupe —
  assert table row count == exactly produced count under all of them.
- **P1 (usability)**: partitioning + system columns (D4); schemaless + DLQ
  (D5); Lakekeeper in helm; lag metrics; DuckDB-based conformance test in CI
  (read the table back, compare against produced records — same spirit as
  the cross-language raw-read tests).
- **P2 (operations)**: snapshot expiry; worker role (D6); maintenance
  runbook; optional backpressure SLA.
- **P3 (duality)**: FL GC after watermark; Parquet-backed reads for old
  offsets; Arrow read API for analytics consumers.

## Open questions

1. One table per topic is the current mapping. Do we want topic-pattern →
   table routing (fan-in) like Tableflow? (Defer; keep 1:1.)
2. Where do table configs live — Postgres per-topic settings vs table
   properties? (Lean: flux settings in Postgres, mirror as table props.)
3. Multi-tenant namespace strategy for the catalog (currently single
   `flux` namespace).
