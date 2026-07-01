# Flux Development Notes

## Build Commands

```bash
# Preferred unified task runner
cargo xtask help

# Build entire workspace
cargo build --workspace

# Build specific crate
cargo build -p flux-broker
cargo build -p flux-wire
cargo build -p flux-schema
cargo build -p flux-common

# Release build
cargo build --workspace --release
```

## Unified Cargo Automation (`xtask`)

```bash
# Show all unified commands
cargo xtask help

# Regenerate protobuf code for Java/Python + rebuild Rust wire proto
cargo xtask gen-proto

# Build Rust workspace
cargo xtask build

# Run Rust tests
cargo xtask test-rust

# Run all flux-broker integration suites (includes ignored tests)
# Auto-detects suites from crates/flux-broker/tests/*.rs.
# Note: this includes cross_language_e2e, which requires Java + Python SDK toolchains.
# Uses DATABASE_URL if set, otherwise defaults to postgres://postgres:postgres@localhost:5433
cargo xtask test-db

# Run SDK tests (Java + Python)
cargo xtask test-sdk

# Run all tests across all modules/languages (Rust + Java + Python + DB suites)
# Uses DATABASE_URL if set, otherwise defaults to postgres://postgres:postgres@localhost:5433
cargo xtask test-all

# Full local CI: gen-proto + build + all tests
cargo xtask ci
```

## Adding New Modules / Suites

```bash
# 1) New Rust crate/module in workspace
# Put it under crates/<name>; workspace membership is auto-detected by:
# members = ["crates/*", "xtask"]

# 2) New flux-broker integration test suite
# Add crates/flux-broker/tests/<suite_name>.rs
# No xtask code change needed: `cargo xtask test-db` auto-detects *.rs suites.

# 3) New protobuf schema file
# Add proto/<name>.proto and update language wiring if used by SDK/runtime code.
# Current `cargo xtask gen-proto` regenerates from proto/flux_wire.proto.

# 4) New SDK language
# Add it under sdks/<language>/ and then extend `xtask test-sdk`
# (currently runs Java + Python explicitly).
```

## Test Commands

```bash
# Run all tests
cargo test --workspace

# Run all tests across all modules/languages (Rust + Java + Python + DB suites)
cargo xtask test-all

# Run tests for specific crate
cargo test -p flux-broker
cargo test -p flux-wire
cargo test -p flux-schema

# Run specific test
cargo test -p flux-broker test_distribute_acks

# Run tests with output
cargo test --workspace -- --nocapture

# Run tests with backtrace on failure
RUST_BACKTRACE=1 cargo test --workspace
```

## Lint Commands

```bash
# Run clippy on entire workspace
cargo clippy --workspace -- -D warnings

# Run clippy on specific crate
cargo clippy -p flux-broker -- -D warnings
cargo clippy -p flux-wire -- -D warnings
cargo clippy -p flux-schema -- -D warnings

# Format code
cargo fmt --all

# Check formatting without applying
cargo fmt --all -- --check
```

## Database Commands

```bash
# Start Postgres (assuming Docker)
docker run -d --name flux-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=flux \
  -p 5433:5432 \
  postgres:16

# Connect to Postgres
psql -h localhost -p 5433 -U postgres -d flux

# Apply authoritative schema migration (single consolidated file)
psql -h localhost -p 5433 -U postgres -d flux -f migrations/001_init.sql

# Reset schema quickly (local/dev only)
psql -h localhost -p 5433 -U postgres -d flux -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
psql -h localhost -p 5433 -U postgres -d flux -f migrations/001_init.sql
```

## Crate Structure

```
crates/
├── flux-common/     # Shared types: IDs, errors, Record, RecordBatch, BatchAck
├── flux-wire/       # Wire protocol: varint, writer/reader encoding
├── flux-schema/     # Schema registry: canonicalization, compatibility, HTTP API
├── flux-broker/      # Broker server: batching, FL, S3, WebSocket
└── flux-core/       # Core Avro handling (existing)
```

## Key Files

| Crate | File | Purpose |
|-------|------|---------|
| flux-wire | `varint.rs` | Zigzag varint encoding (Avro-compatible) |
| flux-wire | `writer.rs` | AppendRequest/Response encoding |
| flux-wire | `reader.rs` | Read/group protocol encoding (read, join, heartbeat, rejoin, commit) |
| flux-broker | `fl.rs` | FL file format (ZSTD + footer index) |
| flux-broker | `buffer.rs` | Request batching and merging |
| flux-broker | `dedup.rs` | LRU dedup cache |
| flux-broker | `batched_server.rs` | WebSocket server with batching + flush loop |
| flux-broker | `coordinator.rs` | Reader-group coordination and assignment |
| flux-broker | `admin/topics.rs` | Admin API topic lifecycle endpoints |
| flux-broker | `bin/flux-broker.rs` | Broker binary (WebSocket + Admin API + shutdown) |
| flux-schema | `canonical.rs` | Schema canonicalization + SHA-256 |
| flux-schema | `compat.rs` | Backward compatibility checking |
| flux-schema | `registry.rs` | Database-backed schema registry |
| flux-schema | `api.rs` | HTTP API endpoints |

## Test Suite Discovery

```bash
# List flux-broker integration suites
ls crates/flux-broker/tests/*.rs | xargs -n1 basename | sed 's/\.rs$//'

# Only ignored suites (currently cross_language_e2e)
rg -n "#\\[ignore" crates/flux-broker/tests
```

## Running Integration Tests

```bash
# Run all tests in workspace
# Note: flux-broker integration tests require local Postgres on :5433
cargo test --workspace

# Run only unit tests (no DB required)
cargo test -p flux-broker --lib
cargo test -p flux-wire
cargo test -p flux-schema

# Run flux-broker integration tests that do not need external SDK toolchains
# DATABASE_URL must NOT include a database name; tests create/drop per-test DBs.
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --tests -- --nocapture

# Run integration tests without DB setup (pure in-memory suite)
cargo test -p flux-broker --test integration

# Run specific test suites with database:

# Admin API integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test admin_api_integration -- --nocapture

# Auth integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test auth_integration -- --nocapture

# DB integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test db_integration -- --nocapture

# E2E WebSocket tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test e2e_websocket -- --nocapture

# E2E Reader Groups tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test e2e_reader_groups -- --nocapture

# Cross-language E2E tests (requires Java + Python SDK toolchains)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test cross_language_e2e -- --include-ignored

# Coordinator integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test coordinator_integration -- --nocapture

# Jepsen-inspired tests (run each suite explicitly)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test jepsen_reader_groups -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test jepsen_crash -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test jepsen_linearizability -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test jepsen_offset -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test jepsen_partition -- --nocapture

# Negative/error handling tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test negative_tests -- --nocapture

# One-shot full DB suite run (continues after individual suite failures)
/bin/zsh -lc 'export DATABASE_URL=postgres://postgres:postgres@localhost:5433; tests=(admin_api_integration auth_integration coordinator_integration db_integration e2e_reader_groups e2e_websocket integration jepsen_reader_groups jepsen_crash jepsen_linearizability jepsen_offset jepsen_partition negative_tests); for t in $tests; do echo "=== $t ==="; cargo test -q -p flux-broker --test $t -- --nocapture || true; done'

# Cross-language suite run (ignored by default)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flux-broker --test cross_language_e2e -- --include-ignored --nocapture
```

## Git Workflow

```bash
# Check status
git status

# View recent commits
git log --oneline -10

# Commit with co-author
git commit -m "$(cat <<'EOF'
commit message here

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"

# Amend last commit
git commit --amend -m "new message"
```

## Performance Profiling

```bash
# Benchmark (broker e2e load test)
# Notes:
# - DATABASE_URL must not include a database name (tests create isolated DBs)
# - test_e2e_load_one_million_requests is ignored by default; run with --ignored
#
# 300k request benchmark (faster iteration)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLUX_LOAD_PRODUCERS=30 \
FLUX_LOAD_BATCHES_PER_PRODUCER=10000 \
FLUX_LOAD_PARTITIONS=32 \
FLUX_LOAD_RECORDS_PER_BATCH=128 \
FLUX_LOAD_PAYLOAD_BYTES=32 \
FLUX_LOAD_MAX_IN_FLIGHT=64 \
FLUX_LOAD_FETCH_TIMEOUT_SECS=600 \
FLUX_LOAD_ENABLE_OTEL=1 \
cargo test -p flux-broker --test e2e_load test_e2e_load_one_million_requests -- --ignored --nocapture

# 1M request benchmark
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLUX_LOAD_PRODUCERS=40 \
FLUX_LOAD_BATCHES_PER_PRODUCER=25000 \
FLUX_LOAD_PARTITIONS=32 \
FLUX_LOAD_RECORDS_PER_BATCH=128 \
FLUX_LOAD_PAYLOAD_BYTES=32 \
FLUX_LOAD_MAX_IN_FLIGHT=64 \
FLUX_LOAD_FETCH_TIMEOUT_SECS=1200 \
FLUX_LOAD_ENABLE_OTEL=0 \
cargo test -p flux-broker --test e2e_load test_e2e_load_one_million_requests -- --ignored --nocapture

# Generate flamegraph (requires cargo-flamegraph)
cargo flamegraph -p flux-broker --bin flux-broker -- <args>

# Flamegraph with 1.2M request load test (40 * 30,000, records_per_batch=128)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLUX_LOAD_PRODUCERS=40 \
FLUX_LOAD_BATCHES_PER_PRODUCER=30000 \
FLUX_LOAD_PARTITIONS=32 \
FLUX_LOAD_RECORDS_PER_BATCH=128 \
FLUX_LOAD_PAYLOAD_BYTES=32 \
FLUX_LOAD_MAX_IN_FLIGHT=64 \
FLUX_LOAD_FETCH_TIMEOUT_SECS=1800 \
FLUX_LOAD_ENABLE_OTEL=0 \
cargo flamegraph -p flux-broker --test e2e_load -- test_e2e_load_one_million_requests --ignored --nocapture

# Profile with perf
perf record -g cargo run -p flux-broker --bin flux-broker --release -- <args>
perf report
```

## Environment Variables

```bash
# Database
DATABASE_URL=postgres://postgres:postgres@localhost:5433

# AWS S3 (for local testing with MinIO)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://localhost:9000

# Logging
RUST_LOG=debug
RUST_LOG=flux_broker=debug,flux_wire=info
```

## Local S3 with MinIO

```bash
# Start MinIO
docker run -d --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://flux
```

## Useful Cargo Commands

```bash
# Check what would be compiled
cargo check --workspace

# Show dependency tree
cargo tree -p flux-broker

# Update dependencies
cargo update

# Show outdated dependencies
cargo outdated

# Generate docs
cargo doc --workspace --open
```

## Pending

- [ ] Focus next optimization on reducing append ack path queueing: `flush_buffer_residency` and `flush_queue_wait` are the largest controllable contributors after dedup.
- [ ] Keep writer batching enabled in perf goals; `records_per_batch=1` dramatically underutilizes throughput.
- [ ] Evaluate dedup lock contention under high in-flight single-writer load (`dedup_check` remains material in request-heavy runs).
- [ ] Decide whether to add an explicit writer benchmark profile in CI: `1 writer + batched requests` with `records/sec` and `bytes/sec` targets.

### Latest Findings (2026-02-13)

- 1.2M request flamegraph run (`40 x 30,000`, `records_per_batch=1`):
  - `req_rps=70,895`, `payload_Bps=2,268,639`, `payload_MiBps=2.16`
  - `req_p50=37.951ms`, `req_p95=46.015ms`, `req_p99=50.047ms`
  - Hot metrics:
    - `append_enqueue_to_ack` avg `20.705ms`
    - `flush_buffer_residency` avg `12.079ms`
    - `dedup_check` avg `6.672ms`
    - `flush_queue_wait` avg `2.788ms`
- Batched writer confirmation (`records_per_batch=100`) with same 300k records:
  - `30 writers`: `rec_rps=744,982`, `payload_MiBps=22.74`
  - `1 writer`: `rec_rps=449,469`, `payload_MiBps=13.72`
  - Conclusion: use `records/sec` and `bytes/sec` as primary throughput metrics; request/sec is no longer the primary KPI when batching is enabled.

### Latest Findings (2026-02-14)

- Expanded load sweep (`20 writers`, payload `32B`, `max_in_flight=64`) confirms batching is a dominant lever:
  - `records_per_batch=1` (`80,000 req`): `rec_rps=37,906`, `payload_MiBps=1.16`, `req_p95=41.343ms`
  - `records_per_batch=100` (`80,000 req`): `rec_rps=2,265,236`, `payload_MiBps=69.13`, `req_p95=67.199ms`
  - `records_per_batch=250` (`30,000 req`): `rec_rps=2,980,241`, `payload_MiBps=90.95`, `req_p95=131.839ms`
  - `records_per_batch=500` (`20,000 req`): `rec_rps=3,009,584`, `payload_MiBps=91.85`, `req_p95=384.255ms`
- Throughput gains flatten above `250` while request latency and queueing rise sharply.
- Default for `test_e2e_load_one_million_requests` is now `records_per_batch=128` unless overridden by `FLUX_LOAD_RECORDS_PER_BATCH`.
