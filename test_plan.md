# Flux Test Suite Audit & Fix Plan

## Overall Assessment

The test suite is **well above average** for a project at this stage — ~240+ tests across unit, integration, property-based, and Jepsen-style chaos categories. The `OperationHistory` verifier, `FaultyObjectStore`, and coordinator regression tests show genuine correctness engineering. However, there are **systematic gaps** and several instances of **test hacking** that undermine confidence in the claimed guarantees.

---

## Fix Plan (Priority Order)

### Fix 1: `decode_i32` silent truncation in varint.rs [DONE]

**File:** `crates/flux-wire/src/varint.rs:67-70`

**Problem:** `decode_i32` calls `decode_i64` and casts `value as i32`. If the decoded value exceeds i32 range, this silently truncates. A malicious or buggy peer could cause silent data corruption.

**Fix:** Added range check returning `DecodeError::I32Overflow` if value exceeds i32 range. Added `I32Overflow` variant to `DecodeError`. Added tests for overflow and boundary values.

**Status:** Complete. All 16 varint tests pass.

---

### Fix 2: Wire `verify_poll_contiguity()` into chaos tests [DONE]

**File:** `crates/flux-broker/tests/common/operation_history.rs:259`

**Problem:** `verify_poll_contiguity()` validates the core gap-free offset guarantee — that sequential reads by the same consumer don't skip offsets. The function is implemented but **never called by any test**. This is the single biggest gap in the test suite.

**Fix:** Add `h.verify_poll_contiguity().expect("INVARIANT: sequential reads don't skip offsets")` after the existing verification block in every chaos test that uses `OperationHistory`:

- `jepsen_exactly_once.rs` (~line 366) [DONE]
- `jepsen_multi_broker.rs` (~line 609, ~line 793) [DONE]
- `jepsen_network_partition.rs` (~line 322) [DONE]
- `jepsen_chaos_workload.rs` (~line 374, ~line 702, ~line 1015) [DONE]

**Status:** Complete. All chaos tests compile with the new verification.

---

### Fix 3: Fix vacuous `watermark >= 0` assertions in reader group tests [DONE]

**File:** `crates/flux-broker/tests/jepsen_reader_groups.rs:221,783`

**Problem:** Asserting a u64-like value is `>= 0` is always true.

**Fix:**
- `test_concurrent_commit_same_reader`: Changed to `watermark > 0` with comment explaining contiguous ordering dependency.
- `test_concurrent_poll_commit_stress`: Changed to `watermark >= total_committed` AND `watermark > 0`.

**Status:** Complete. Compiles successfully.

---

### Fix 4: Fix negative tests to assert specific error codes

**File:** `crates/flux-broker/tests/negative_tests.rs`

**Problem:**
- `test_produce_to_nonexistent_topic`: Asserts only `!data.is_empty()` for binary responses. Any non-empty response passes.
- `test_fetch_from_nonexistent_topic`: Accepts any response as valid. Comment says "The important thing is it doesn't crash."

These are "no-crash" tests, not correctness tests.

**Fix:** Decode the response using the wire protocol and assert the appropriate error code (e.g., `ERR_INTERNAL_ERROR` or a topic-not-found code). If the system returns a `Close` frame, assert the close reason is appropriate.

---

### Fix 5: Fix fake property test for HeartbeatResponseExt

**File:** `crates/flux-wire/src/proptest_tests.rs`

**Problem:** `prop_heartbeat_response_ext_roundtrip` takes a `_dummy: u64` parameter that is never used. It runs 500 iterations but tests the exact same fixed `HeartbeatResponseExt{status: Ok, success: true}` every time. The `UnknownMember` variant is never tested.

**Fix:** Replace the dummy parameter with actual proptest generators:
```rust
fn arb_heartbeat_response_ext() -> impl Strategy<Value = HeartbeatResponseExt> {
    (any::<bool>(), prop_oneof![Just(HeartbeatStatus::Ok), Just(HeartbeatStatus::UnknownMember)])
        .prop_map(|(success, status)| HeartbeatResponseExt { success, status, .. })
}
```

---

### Fix 6: Fix property test response roundtrips to check contents not just len

**File:** `crates/flux-wire/src/proptest_tests.rs`

**Problem:** `prop_produce_response_roundtrip`, `prop_read_response_roundtrip`, `prop_poll_response_roundtrip` only verify `acks.len()` / `results.len()` matches but never check the **contents** (topic IDs, offsets, record values). Given the empty-key conflation bug, these could be masking real corruption.

**Fix:** After checking length, iterate through decoded results and compare each field against the original. For records specifically, compare key bytes and value bytes.

---

### Fix 7: Add final-read absence check to chaos tests

**File:** `crates/flux-broker/tests/common/operation_history.rs`

**Problem:** Tests check that all acked writes are present (verify_acknowledged_writes_visible) but do NOT check that writes which were NOT acked are absent from the committed log. A false positive (data committed but error returned to client) would go undetected.

**Fix:** Add `verify_no_phantom_writes()` to `OperationHistory`:
```rust
/// Verify that no values appear in reads that were never acknowledged to any writer.
pub fn verify_no_phantom_writes(&self) -> Result<(), String> {
    let acked_values: HashSet<&[u8]> = self.writes.iter()
        .filter(|w| w.success)
        .map(|w| w.value.as_ref())
        .collect();
    for read in &self.reads {
        if read.success {
            for value in &read.returned_values {
                if !acked_values.contains(value.as_ref()) {
                    return Err(format!("Phantom write detected: value {:?} in reads but never acked", value));
                }
            }
        }
    }
    Ok(())
}
```

Wire this into all chaos tests alongside the other verification calls.

---

### Fix 8: Fix `catch_unwind` panic swallowing in wire union.rs

**File:** `crates/flux-wire/src/union.rs`

**Problem:** `decode_from_encoded` uses `catch_unwind(AssertUnwindSafe(...))` to catch buffer-too-small panics, then doubles the buffer and retries. But it catches **all** panics, including data corruption panics, and retries indefinitely. A non-buffer panic would cause infinite retry until OOM.

**Fix:** Add a retry limit (e.g., 5 iterations, supporting up to 64KB * 2^5 = 2MB buffers). After the limit, re-panic with the original payload.

---

## Additional Issues (Not Yet Tasked)

### Issue 9: `produce_records()` bypasses the broker

**Files:** Used in `jepsen_crash.rs`, `jepsen_offset.rs`, `jepsen_linearizability.rs`, `jepsen_partition.rs`

**Problem:** This helper writes directly to S3 + Postgres, skipping the broker's buffer, flush loop, and dedup logic. Tests using it are storage layer tests masquerading as broker tests.

**Recommendation:** Replace `produce_records()` with actual broker writes through WebSocket in these tests. This is a larger refactor — keep it as a follow-up.

---

### Issue 10: `TopicId(0)` hardcoded in OperationHistory

**Files:** `jepsen_multi_broker.rs`, `jepsen_network_partition.rs`, `jepsen_chaos_workload.rs`, `jepsen_exactly_once.rs`, `operation_history.rs:61,92`

**Problem:** All chaos tests record writes with `TopicId(0)` regardless of actual topic ID. Verification functions group by topic ID, so this works for single-topic tests but would silently disable multi-topic verification.

**Recommendation:** Pass actual topic IDs through the operation history. The convenience methods `record_write_start` and `record_read_start` default to `TopicId(0)` — either remove them or require topic ID.

---

### Issue 11: No CRC integrity verification in any test

**Problem:** Despite CRC32 being stored per-segment in `topic_batches`, not a single test reads data back and verifies the CRC matches. The `jepsen_read_faults::test_read_corrupt_fl_returns_error_not_panic` corrupts bytes but relies on FL reader rejection — it doesn't verify CRC detection specifically.

**Recommendation:** Add a test that reads a batch, extracts the CRC from `topic_batches`, and verifies it matches the computed CRC of the segment data.

---

### Issue 12: No concurrent read-during-write test

**Problem:** No test checks what a reader observes while a flush is in-progress. Partial visibility of a batch being written is a real concern given S3-then-Postgres commit ordering.

**Recommendation:** Add a test with a slow S3 put (via `set_put_delay_ms`) and a concurrent reader. Verify the reader either sees the full batch or nothing — never a partial batch.

---

### Issue 13: SDK retry/idempotency logic completely untested

**Files:** `crates/flux-sdk/src/writer.rs`, `crates/flux-sdk/src/reader/`

**Problem:**
- Writer's `append_batch()` retry loop (retries on backpressure) — no test
- Idempotent sequence reuse on retry — no test
- `max_in_flight` semaphore — no test
- `fail_all_pending_disconnected` — no test
- Timeout behavior — no test
- Reader lifecycle (poll/commit/heartbeat, stop, rejoin on UnknownMember) — no test

**Recommendation:** Add unit tests with a mock WebSocket server that returns configurable responses (backpressure, errors, timeouts).

---

### Issue 14: Empty key vs null key conflation in protobuf roundtrip

**Problem:** `Record { key: Some(Bytes::new()) }` (empty key) and `Record { key: None }` (null key) serialize identically in protobuf3. On decode, empty keys become null keys.

**Recommendation:** Add a roundtrip test that encodes `Some(Bytes::new())` and verifies what comes back. Then decide if this is a bug or an acceptable limitation and document it.

---

### Issue 15: `CrashableBroker` (non-WS) is a clean drop, not a crash

**Problem:** `CrashableBroker.crash()` sets `self.state = None`, running Rust destructors cleanly. A real crash leaves dirty state. The `CrashableWsBroker` variant correctly uses `handle.abort()`.

**Recommendation:** Migrate tests using `CrashableBroker` to `CrashableWsBroker`, or add `abort()`-style crash to the non-WS variant.

---

### Issue 16: Timing-dependent settling throughout

**Problem:** 15+ tests use `tokio::time::sleep()` after fault injection (200ms to 2s) instead of polling for a condition. On loaded CI, these are flaky by construction.

**Recommendation:** Replace sleeps with condition-based waiting (e.g., poll until watermark reaches expected value, or retry read until expected count is returned).

---

### Issue 17: Schema registry has empty test module

**File:** `crates/flux-schema/src/registry.rs`

**Problem:** `#[cfg(test)] mod tests {}` — zero tests. The registry handles dedup, compatibility checking, and transactional registration.

**Recommendation:** Add integration tests for schema registration dedup, compatibility rejection, and concurrent registration.

---

### Issue 18: Missing wire protocol union-level coverage

**Problem:**
- 6 of 8 `ClientMessage` variants lack union-level envelope roundtrip tests (only `Append` is tested)
- 4 of 8 `ServerMessage` variants lack union-level envelope tests
- No `LeaveGroup` request/response roundtrip test at any level
- No test for `decode_client_message(&[])` (empty buffer)

**Recommendation:** Add union-level roundtrip tests for all message variants.

---

### Issue 19: No test for ACL changes on existing connections

**Problem:** If permissions are revoked mid-session, does the next request fail? This is security-critical and untested.

**Recommendation:** Add E2E test: connect with valid key, revoke permissions, verify next request is denied.

---

### Issue 20: Reader group E2E tests check `success == true` only

**Files:** `crates/flux-broker/tests/e2e_reader_groups.rs`

**Problem:**
- `test_three_readers_join_group` checks `resp.success` three times but never verifies member count, generation, or assignments
- `test_commit_offsets` checks `commit_resp.success` but never verifies watermark advanced
- `test_pipelined_poll_and_out_of_order_commit` silently skips commit assertions when polls return empty

**Recommendation:** Add structural assertions (member count, watermark value, assignment contents) to each reader group E2E test.

---

## Comparison with Jepsen Bufstream Baseline

| Jepsen Property | Flux Coverage | Gap |
|---|---|---|
| **P1: Append-only ordered log** | Partial (offset uniqueness tested, contiguity not) | Fix 2 addresses this |
| **P2: Durability of acked writes** | Good (`verify_acknowledged_writes_visible`) | Fix 7 adds absence check |
| **P3: Offset uniqueness/consistency** | Good (HashSet checks) | Checks value presence not (offset,value) binding |
| **P4: Monotonic offset progression** | Partial (send-side only) | No poll-side monotonicity test |
| **P5: No gaps in consumed sequence** | **Not tested** | Fix 2 addresses this |
| **P6-P8: Transactional properties** | N/A (no transactions) | — |
| **P9: Exactly-once** | Good (`jepsen_exactly_once.rs`) | Missing gap-free check |
| **P10: Liveness under faults** | Moderate | Issue 9: `produce_records` bypasses broker |
| **P11: Consumer group rebalance** | Moderate | No test with real data during rebalance |
| **Subsystem-aware faults** | Good | No partial S3 write (truncation) fault |
| **Final-read verification** | Partial (presence only) | Fix 7 adds absence check |
