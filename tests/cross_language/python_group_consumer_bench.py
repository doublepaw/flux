#!/usr/bin/env python3
"""
Group consumer throughput benchmark: poll -> count -> commit until the
expected number of records is consumed or a 120s timeout expires.

Usage:
    python python_group_consumer_bench.py \
        <url> <topic_id> <expected_records> <raw|classic> [max_bytes] [pipeline_depth]

Prints one JSON line to stdout:
    {"consumer":"python-<mode>","records":N,"elapsed_secs":S,"records_per_sec":R,"mib_per_sec":M}
"""

import asyncio
import json
import os
import sys
import time

# Add the SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from flux import GroupReader, ReaderConfig

TIMEOUT_SECS = 120
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_PIPELINE_DEPTH = 2


async def main():
    if len(sys.argv) < 5 or len(sys.argv) > 7:
        print(
            f"Usage: {sys.argv[0]} <url> <topic_id> <expected_records> "
            "<raw|classic> [max_bytes] [pipeline_depth]",
            file=sys.stderr,
        )
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])
    expected_records = int(sys.argv[3])
    mode = sys.argv[4]
    if mode not in ("raw", "classic"):
        print(f"Mode must be 'raw' or 'classic', got: {mode}", file=sys.stderr)
        sys.exit(1)
    max_bytes = int(sys.argv[5]) if len(sys.argv) >= 6 else DEFAULT_MAX_BYTES
    pipeline_depth = int(sys.argv[6]) if len(sys.argv) >= 7 else DEFAULT_PIPELINE_DEPTH

    config = ReaderConfig(
        url=url,
        group_id=f"bench-python-{mode}-{int(time.time() * 1000)}",
        topic_id=topic_id,
        max_bytes=max_bytes,
        pipeline_depth=pipeline_depth,
        raw=(mode == "raw"),
        heartbeat_interval=5.0,
    )

    records = 0
    payload_bytes = 0

    reader = await GroupReader.join(config)
    async with reader:
        reader.start_heartbeat()

        start = time.monotonic()
        deadline = start + TIMEOUT_SECS

        while records < expected_records and time.monotonic() < deadline:
            batch = await reader.poll()

            for result in batch.results:
                for record in result.records:
                    records += 1
                    payload_bytes += len(record.value) + len(record.key)

            await reader.commit(batch)

            if batch.start_offset == batch.end_offset:
                await asyncio.sleep(0.05)  # no work leased; back off briefly

        elapsed_secs = time.monotonic() - start

    records_per_sec = records / elapsed_secs
    mib_per_sec = payload_bytes / (1024.0 * 1024.0) / elapsed_secs
    print(json.dumps({
        "consumer": f"python-{mode}",
        "records": records,
        "elapsed_secs": round(elapsed_secs, 3),
        "records_per_sec": round(records_per_sec, 1),
        "mib_per_sec": round(mib_per_sec, 2),
    }))

    if records < expected_records:
        print(f"Timed out: consumed {records} of {expected_records} records", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
