#!/usr/bin/env python3
"""
Cross-language E2E test: Python raw (zero-copy) reader.

Usage:
    python python_raw_reader.py <url> <topic_id> <expected_count>

Raw-reads compressed FL segments starting at offset 0, decodes them
locally (verifying CRC), and prints the records as JSON to stdout.
"""

import asyncio
import json
import sys
import os

# Add the SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from flux import RawReader, ReaderConfig


async def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <url> <topic_id> <expected_count>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])
    expected_count = int(sys.argv[3])

    config = ReaderConfig(
        url=url,
        topic_id=topic_id,
        timeout=30.0,
    )

    records_received = []
    segments_received = []
    high_watermark = 0
    offset = 0

    reader = await RawReader.connect(config)
    async with reader:
        # Read until we have all expected records or timeout
        max_attempts = 10
        for attempt in range(max_attempts):
            result = await reader.read(offset)
            high_watermark = result.high_watermark

            for segment in result.segments:
                segments_received.append({
                    "start_offset": segment.start_offset,
                    "end_offset": segment.end_offset,
                })
                for key, value in segment.records:
                    try:
                        value_str = value.decode('utf-8')
                        value_json = json.loads(value_str)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        value_json = {"raw": value.hex()}

                    records_received.append({
                        "key": key.decode('utf-8') if key else None,
                        "value": value_json,
                    })

            if result.next_offset is not None:
                offset = result.next_offset

            if len(records_received) >= expected_count:
                break

            await asyncio.sleep(0.5)

    result = {
        "reader": "python-raw",
        "topic_id": topic_id,
        "record_count": len(records_received),
        "high_watermark": high_watermark,
        "segments": segments_received,
        "records": records_received,
    }
    print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())
