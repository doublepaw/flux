# Performance Plan: Closing the Throughput Gap

Context: at 1KB records on one 32-core box (broker + clients + Postgres + local FS
store sharing cores), produce sustains ~858 MiB/s but fetch only ~157 MiB/s.
Ursa-class systems report ~5 GB/s on 12-broker clusters (~420 MB/s/broker) with
consumers keeping pace. The produce path is competitive per-broker; the read
path and cloud-deployability are not. This plan closes both.

## Measured bottleneck analysis

**Read path** (83.7s to read back what took 14.6s to write):

1. `fetch_records` caps every response at `LIMIT 10` index rows regardless of
   the client's `max_bytes`. At 128-record batches that is ~1.3 MB per
   response, forcing ~10,000 serial round-trips for 12.8M records
   (measured: ~8.4 ms/trip → 83.7s).
2. The up-to-10 `get_range` calls per request run **serially**. On local FS
   that is ~10 syscall round-trips; on real S3 it would be 10 × 20-50 ms —
   catastrophic.
3. Two Postgres round-trips per read (index rows + high watermark) that can
   be one.
4. The benchmark/SDK client reads on a **single connection, strictly
   request→response serial**. The server already dispatches non-append
   messages concurrently per connection, but the wire protocol has no
   correlation ID, so a single connection cannot safely pipeline reads.
   Parallelism must come from multiple connections reading disjoint offset
   stripes (which is also what a real consumer fleet looks like).

**Write path** (fine locally, at risk on real object storage):

5. The flush loop allows **one in-flight flush** per broker (a deliberate
   correctness choice: concurrent flushes could commit out of order and
   violate per-writer offset monotonicity). Throughput ceiling on S3 is
   `buffer_bytes_per_flush / (S3 PUT + PG commit latency)`. With the default
   256 MB buffer / 200 ms max_wait this is not the first bottleneck, but ack
   latency includes full flush duration.

**Deployability blockers for cloud benchmarks:**

6. `flux-broker` binary hardcodes `LocalFsStore` ("local filesystem for
   now") — `S3ObjectStore` exists but is unreachable. No GCS/Azure backend.
7. `BrokerConfig` (flush interval, buffer sizes) is not configurable from the
   environment.
8. `flux-bench` is a *simulated* benchmark (in-process counters, no network).
   There is no driver that can benchmark a remote broker.

## Changes

### P0 — read path (this is the gap)

- **R1: byte-budgeted index selection.** Replace `LIMIT 10` with a single
  CTE query that (a) returns index rows whose *cumulative* `byte_length`
  covers `max_bytes` (hard cap 256 rows), and (b) returns the high watermark
  in the same round-trip. Expected: one read request now returns up to
  `max_bytes` (16 MB), cutting round-trips ~12x.
- **R2: concurrent segment fetch.** Issue `get_range` for selected rows via
  `futures::stream::iter(..).buffered(16)` — order-preserving, up to 16
  concurrent object-store reads. Biggest win on real S3.
- **R3: parallel fetch in the load test.** `fetch_all_records` gains N
  striped reader connections (default 8, `FLUX_LOAD_FETCH_READERS`), each
  reading a disjoint contiguous offset range.

### P1 — cloud deployability (prerequisite for AWS/GCP/Azure benchmarks)

- **C1: store selection in the binary.** `OBJECT_STORE=fs|s3|gcs|azure` +
  `OBJECT_STORE_BUCKET`, `OBJECT_STORE_PREFIX`, `S3_ENDPOINT` (LocalStack /
  GCS-interop). S3 uses the existing native `S3ObjectStore`; GCS and Azure
  get a thin adapter implementing flux's `ObjectStore` trait over the
  `object_store` crate (already a dependency; enable `gcp`,`azure`
  features).
- **C2: env-tunable broker config.** `FLUSH_INTERVAL_MS`,
  `BUFFER_MAX_BYTES`, `BUFFER_MAX_WAIT_MS`, water marks, `PG_POOL_SIZE`.
- **C3: real network mode for flux-bench.** `flux-bench produce|fetch|e2e
  --url ws://... --topic-id N --writers W --record-size B --batch-size R
  --requests N --max-in-flight K --readers R --output json`. Reuses the
  e2e_load client logic (pipelined writers, striped readers). This is the
  container the k8s benchmark Job runs.

### P2 — noted, not in this pass

- Pipelined flush (concurrent S3 PUTs, in-order PG commits) to cut produce
  ack latency on S3. Guarded by jepsen_pipelined_flush/exactly_once when
  attempted.
- Broker-side watermark cache (10-20 ms TTL) to shed hot PG reads.
- Zero-copy segment serving (ship compressed FL segments to SDK, decode
  client-side) — protocol change, v2. Removes broker zstd+re-encode CPU from
  the read path entirely.
- Correlation IDs in the wire protocol to allow single-connection
  pipelining.

## Verification

- Full jepsen + workspace suite green after each change.
- Local before/after: 100k-request, 1KB-record run
  (produce MiB/s, fetch MiB/s, req p50/p99).
- Cloud: helm-deployed broker(s) + managed Postgres + real object storage,
  flux-bench Job, per-cloud numbers in runbook.md.
