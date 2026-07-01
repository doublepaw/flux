#!/usr/bin/env python3
"""
Cross-language E2E test: Python reader.

Usage:
    python python_reader.py <url> <topic_id> <expected_count>

Reads records and prints them as JSON to stdout.
"""

import asyncio
import json
import sys
import os

# Add the SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from flux import GroupReader, ReaderConfig


async def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <url> <topic_id> <expected_count>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])
    expected_count = int(sys.argv[3])

    config = ReaderConfig(
        url=url,
        group_id=f"cross-lang-test-{os.getpid()}",
        topic_id=topic_id,
        timeout=30.0,
        heartbeat_interval=5.0,
    )

    records_received = []

    reader = await GroupReader.join(config)
    async with reader:
        # Poll until we have all expected records or timeout
        max_attempts = 10
        for attempt in range(max_attempts):
            batch = await reader.poll()
            for result in batch.results:
                for record in result.records:
                    try:
                        value_str = record.value.decode('utf-8')
                        value_json = json.loads(value_str)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        value_json = {"raw": record.value.hex()}

                    records_received.append({
                        "key": record.key.decode('utf-8') if record.key else None,
                        "value": value_json,
                    })

            if len(records_received) >= expected_count:
                break

            await asyncio.sleep(0.5)

    result = {
        "reader": "python",
        "topic_id": topic_id,
        "record_count": len(records_received),
        "records": records_received,
    }
    print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())
