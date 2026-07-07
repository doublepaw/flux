#!/usr/bin/env python3
"""Aggregate sharded flux-bench reports (one JSON per pod, e.g. from
`kubectl logs -l job-name=<job> --prefix=false`) into cluster totals.

Rates are computed over the union wall-clock window from the reports'
unix-ms stamps, not by summing per-pod rates: pods that start late or
finish early would otherwise inflate the total.

Usage: bench-aggregate.py report1.json report2.json ...
   or: kubectl logs ... | bench-aggregate.py -   (concatenated JSON objects)
"""
import json
import sys

MIB = 1024.0 * 1024.0


def load(args):
    if args == ["-"]:
        text = sys.stdin.read()
        dec, reports, idx = json.JSONDecoder(), [], 0
        while idx < len(text):
            while idx < len(text) and text[idx] not in "{":
                idx += 1
            if idx >= len(text):
                break
            obj, end = dec.raw_decode(text, idx)
            reports.append(obj)
            idx = end
        return reports
    return [json.load(open(p)) for p in args]


reports = load(sys.argv[1:])
if not reports:
    sys.exit("no reports")

shards = {r["shard_index"] for r in reports}
expected = reports[0]["shard_count"]
if len(shards) != expected:
    sys.exit(f"have shards {sorted(shards)}, expected {expected}")

payload = sum(r["payload_bytes"] for r in reports)
records = sum(r["records"] for r in reports)

p_start = min(r["produce_start_unix_ms"] for r in reports)
p_end = max(r["produce_end_unix_ms"] for r in reports)
produce_secs = (p_end - p_start) / 1000.0
out = {
    "shards": expected,
    "records": records,
    "payload_bytes": payload,
    "produce_secs": produce_secs,
    "produce_mib_per_sec": payload / produce_secs / MIB,
    "produce_p50_ms_worst_shard": max(r["produce_p50_ms"] for r in reports),
    "produce_p99_ms_worst_shard": max(r["produce_p99_ms"] for r in reports),
    "start_skew_ms": max(r["produce_start_unix_ms"] for r in reports) - p_start,
}
if all(r.get("fetch_end_unix_ms") for r in reports):
    f_end = max(r["fetch_end_unix_ms"] for r in reports)
    concurrent = reports[0]["concurrent"]
    # Sequential shards fetch after their own produce; concurrent shards
    # fetch from produce start. Either way the union window starts at the
    # earliest phase start.
    f_start = p_start if concurrent else min(r["produce_end_unix_ms"] for r in reports)
    fetch_secs = (f_end - f_start) / 1000.0
    out["fetch_secs"] = fetch_secs
    out["fetch_mib_per_sec"] = payload / fetch_secs / MIB
    if concurrent:
        e2e = (f_end - p_start) / 1000.0
        out["e2e_secs"] = e2e
        out["combined_mib_per_sec"] = 2.0 * payload / e2e / MIB

print(json.dumps(out, indent=2))
