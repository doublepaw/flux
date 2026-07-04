# flux2: Session Report — Migration, Hardening, Performance, and the Road Ahead

Covers the work from the fluorite→flux migration through the Iceberg design
(2026-07-01 → 2026-07-04). Everything described here is on `main`.

---

## 1. Migration (fluorite → flux)

- Renamed all ~4,300 `fluorite`/`Fluorite`/`FLUORITE` occurrences and 24
  files/dirs across 245 files; wiped history to a single initial commit
  authored as `Nikhil Simha <r.nikhilsimha@gmail.com>`; pushed to the new
  private repo `github.com/doublepaw/flux`; archived `fluorite-io/fluorite`
  (no public pointer, since the new repo is private).
- Guardrails: `.githooks/pre-push` blocks pushes to `doublepaw/*` remotes
  with non-`@gmail.com` commit identities; installed via
  `crates/flux-common/build.rs` (`git config core.hooksPath .githooks`) —
  the cargo analog of a pnpm `prepare` script. Verified end-to-end by
  attempting a bad-identity push (blocked with a fix-it message).
- One real rename casualty: the blanket sed corrupted protoc-generated
  descriptors (length-prefixed strings inside Java/Python gencode). Fixed by
  regenerating via `cargo xtask gen-proto` with protoc 33.4.

## 2. Test-suite audit and the first real bug

Studied the 24 `jepsen_*` suites (117 test fns + shared fault harness:
black-hole partitions, transient/persistent store faults, corruption,
DB row-lock stalls, crashable WS brokers, multi-broker clusters) and the
8-invariant `OperationHistory` checker. Ran everything, including the five
`#[ignore]`d chaos/soak tests and the 1M-request load test.

**Bug #1 — lost watermark advancement (pre-existing).**
`Coordinator::commit_range` recomputed `committed_watermark` from
`MIN(start_offset)` of remaining inflight rows after deleting its own.
Under READ COMMITTED, two concurrent commits each still saw the other's
undeleted row; the last writer pinned the watermark below the true
committed prefix — permanently. Its own jepsen test caught it ~70% of runs.
**Fix:** `FOR UPDATE` on the group-state row taken *first* in every
delete-then-recompute transaction (commit, heartbeat expiry, leave,
force-remove), establishing a global lock order (group state → inflight →
members) that matches poll/join — no deadlock cycle. Deterministic
barrier-synchronized regression test added.

## 3. Performance: closing the consumption gap (three rounds)

Baseline complaint: 1M-request load test (12.8M × 1KB records) showed fetch
at ~1/5 of produce. Root causes found by reading the data path end to end:

**Round 1 — read path plumbing.**
- `fetch_records` had a hard `LIMIT 10` (≈1.3 MB per response regardless of
  `max_bytes`) → replaced with a byte-budgeted CTE that also returns the
  high watermark in the same round trip.
- The up-to-N segment `get_range`s ran serially → 16-way concurrent,
  order-preserving.
- Load-test client fetched on one serial connection → striped parallel
  readers.
- Local fetch: 157 → 422 MiB/s.

**Round 2 — one read = one segment + read-ahead.**
- Flush wrote one segment per topic per flush (segment size = produce rate ×
  flush interval; 50 MB+ on cloud). Any 16 MB read decompressed the whole
  segment; sequential readers re-downloaded and re-decompressed it per
  chunk (~3× read amplification on cloud).
- `SEGMENT_MAX_BYTES` caps segments at drain time (split chunks stay
  offset-contiguous per key, so ack math is untouched); readers set
  `max_bytes == segment cap` so one poll returns one whole segment.
- Server-side read-ahead cache: after serving a read, the broker prefetches
  and decodes the next window while the consumer processes the current one
  (serve-once entries, bytes-capped, `READAHEAD_MAX_BYTES`).
- Cloud effect: AKS fetch 210 → 713 MiB/s (3.4×), EKS 192 → 407 (2.1×).

**Round 3 — zero-copy raw reads.**
- New `RawRead` protocol: broker ships compressed FL segments as-is
  (metadata + payload); the client decompresses, CRC-verifies, and decodes.
  Broker CPU per fetch byte → ~0. `max_bytes` budgets compressed bytes;
  raw reads get their own (compressed-entry) read-ahead cache.
- AKS fetch 713 → 4,322 MiB/s — though see §5's compression caveat.

## 4. SDKs: the segment format goes cross-language; consumers get pipelining

- **Format as a contract**: segment codec extracted to
  `flux_wire::segment` (reference implementation); layout documented in
  ARCHITECTURE.md (zstd frame; zigzag-varint key/value framing; CRC-32 over
  compressed bytes). Java (`SegmentDecoder`) and Python (`flux.segment`)
  reimplement it; cross-language conformance tests produce through the
  broker and decode with each independent implementation.
- **Raw poll for reader groups**: `PollRequest.raw` — leases were already
  segment-aligned by construction (the coordinator accumulates whole index
  rows), so raw segments exactly cover `[start, end)`; commit semantics
  unchanged. Rust/Java/Python group consumers all support it.
- **Pipelined polling** in all three consumers (default depth 2): a
  prefetcher keeps N polls outstanding and decodes ahead into a bounded
  queue — Java via a thread, Rust via a split-socket router task +
  prefetcher, Python via asyncio tasks (no threads needed). Single-consumer
  effect: Java classic 153→201 MiB/s, raw 144→235.
- **Language finding worth remembering**: raw mode is a per-language
  tradeoff, not a universal win — Java raw (235) beats classic (201), but
  Python raw (122) *loses* to classic (218) because pure-Python per-record
  protobuf construction outweighs the broker decode it avoids. Python's
  default stays classic. (This is also the empirical argument for an Arrow
  read API eventually: a wire format whose decode is ~free everywhere.)

Bugs found by building the consumer benchmarks:

- **Bug #2 — committed-but-never-delivered leases (pre-existing, serious).**
  The coordinator budgets leases in *compressed* bytes; the classic poll
  fetch re-budgeted the response in *decoded* bytes. For compressible
  payloads a lease could span ~300k records while the response carried the
  first ~1k — and the SDK commits the whole lease. Fixed: poll fetches the
  entire lease; an e2e assertion now pins response records == leased range.
- **Bug #3 — shared response queues (pre-existing, Java and Python).**
  Both SDKs let the heartbeat task's response race the poll response on one
  shared queue/`recv()`. Fixed with per-message-type response routing in
  both (and the Rust restructure got the same router by design).
- Assorted: `usize::MAX` bound as SQL `i64` becomes −1 (empty-but-advancing
  polls); helm renders big numbers in scientific notation (`| int`);
  `websockets` (Python) default 1 MiB frame cap; bench clients need WS
  connect retry because job pods race broker rollout.

## 5. Honest benchmarking: incompressible payloads change everything

The original bench payload compressed ~40×, flattering every wire/storage
number. `flux-bench` now generates unique xorshift bytes per record.
With real bytes on AWS (EKS 2×c6i.4xlarge, S3):

| Stage | Produce MiB/s | Ack p50 | Fetch MiB/s |
|---|---|---|---|
| v4 (honest baseline) | **79** | 4,329 ms | 267 raw / 278 classic |
| v5: multipart PUT + pipelined flush | **352** | 815 ms | 313 raw |

- **Produce was S3-PUT-bound**: a single-in-flight flush pushing ~256 MB
  objects up one HTTP stream (~60–90 MB/s). Fixes: (a) multipart uploads
  (16 MB parts, 8-way concurrent) in `S3ObjectStore`; (b) pipelined flush —
  up to `FLUSH_PIPELINE_DEPTH` (4) flushes with concurrent puts and
  Postgres commits chained in flush order (per-writer offset monotonicity
  preserved; a failed flush releases its successor's turn).
- **vs Ursa (VLDB'25)**: their ~5 GB/s / 12 brokers ≈ 420 MB/s per
  32-vCPU broker ≈ ~13 MB/s/core. flux v5: 352 MiB/s on ~15 cores ≈
  **~23 MB/s/core** — ahead per-core on produce, fetch in range, with
  latency (815 ms p50 vs their 200–500 ms) closable via smaller/faster
  flushes now that the pipeline absorbs PUT latency.
- Fetch on real S3 is GET-bound, so raw ≈ classic at low broker load —
  consistent with raw's value being broker-CPU offload under consumer fan-in.
- `--topics N` multi-topic bench mode added (writers round-robin, per-topic
  verification). Single-broker A/B confirms topics don't help one broker
  (commit chain serializes regardless); the mode exists for the multi-broker
  scaling run.

All results, per-cloud commands, and teardown procedures: `runbook.md`.
Azure (AKS+Blob) numbers parallel AWS throughout. GCP remains blocked on
IAM grants in the hardened project (GKE control plane parked at 0 nodes).

## 6. Infrastructure learnings (sandbox-specific but durable)

- No Docker daemon possible → images assembled as OCI layouts by hand
  (skopeo pull base → tar layer with binaries + CA certs → python patches
  manifests → skopeo push). Works for ECR/ACR/GAR alike.
- Cloud permission boundaries: resource creation fine; IAM grants need a
  human. Scoped, least-privilege policies pass review where broad ones
  don't (`flux-bench-s3-scoped` vs `AmazonS3FullAccess`).
- Helm/K8s: two-phase deploy (rollout complete, then bench job) avoids
  endpoint races; explicit `--context` everywhere when two clusters are
  live.

## 7. Next direction

**Committed design — the Iceberg path (`DESIGN_ICEBERG.md`).** Research
across Ursa, Bufstream, Confluent/WarpStream Tableflow, Redpanda, AutoMQ,
Fluss, and iceberg-rust 0.9.x converged on: exactly-once via watermarks in
Iceberg snapshot properties; a single epoch-fenced committer per table
(Postgres lease); REST catalog first (Lakekeeper shipped default, Polaris
et al. BYO); day-partitioning with `_offset`/`_ingest_time` system columns;
schemaless + DLQ modes; maintenance external for now. The existing
`flux-iceberg` crate (hot-path buffers + claims/catch-up) is the right
skeleton; P0 fixes its at-least-once hole and commit racing.

Phases: **P0** correctness (snapshot watermarks, committer lease,
iceberg-rust 0.9.1, REST catalog) → **P1** usability (partitioning,
schemaless/DLQ, Lakekeeper in helm, DuckDB conformance CI) → **P2**
operations (snapshot expiry, dedicated worker role, lag SLA) → **P3**
stream/table duality (FL GC past the table watermark, Parquet-backed
old-offset reads, Arrow read API). Format strategy: FL rows stay the hot
path, Parquet holds history, Arrow is the wire format for table-backed
reads — not hot storage (Fluss's Arrow-log choice trades write throughput
we care about for columnar reads we don't need yet).

**Benchmark follow-ups**, in order of value: multi-broker × multi-topic
cluster scaling run (the mode is ready); concurrent produce+consume (how
Ursa measures); ack-latency tuning toward 200–500 ms; the GCP row when IAM
is granted.

**Smaller items**: poll-path read-ahead; SDK docs for the raw/classic
per-language guidance; Postgres HA runbook (the honest availability gap vs
Oxia-class metadata stores — mitigated operationally, not architecturally,
at this scale).

## 8. Bug ledger (all fixed, all with regression tests)

| # | Bug | Class |
|---|---|---|
| 1 | commit_range watermark race (READ COMMITTED MIN-recompute) | pre-existing, correctness |
| 2 | Lease under-delivery: compressed-vs-decoded budget mismatch → committed-but-undelivered records | pre-existing, data loss at consumer API |
| 3 | Java & Python SDK shared response queues (heartbeat vs poll race) | pre-existing, SDK correctness |
| 4 | Rename corrupted protoc gencode descriptors | migration fallout |
| 5 | `usize::MAX as i64 = -1` sqlx bind → empty poll bodies | introduced+caught in-session |
| 6 | Helm scientific-notation rendering of large ints | deploy fallout |
| 7 | flux-cli test broke workspace build silently (crate not covered by `-p flux-broker` runs) | test-coverage gap |

Final state: 548 workspace tests + 5 chaos soaks green; `main` clean and
pushed; no cloud resources billing except the parked GKE control plane.
