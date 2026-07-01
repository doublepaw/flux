#!/usr/bin/env python3
"""
Cross-language schema E2E test: Python reader with Avro-decoded values.

Usage:
    python python_schema_reader.py <url> <topic_id> <expected_count>

Reads records, deserializes values with TestOrder.from_bytes(), prints decoded fields as JSON.
"""

import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from dataclasses import dataclass
from flux import GroupReader, ReaderConfig, schema


@schema
@dataclass
class TestOrder:
    name: str
    amount: int
    active: bool
    tags: list[str]


async def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <url> <topic_id> <expected_count>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])
    expected_count = int(sys.argv[3])

    config = ReaderConfig(
        url=url,
        group_id=f"schema-test-py-{os.getpid()}",
        topic_id=topic_id,
        timeout=30.0,
        heartbeat_interval=5.0,
    )

    decoded_records = []
    reader = await GroupReader.join(config)
    async with reader:
        max_attempts = 10
        for _ in range(max_attempts):
            batch = await reader.poll()
            for result in batch.results:
                for record in result.records:
                    order = TestOrder.from_bytes(record.value)
                    decoded_records.append({
                        "key": record.key.decode('utf-8') if record.key else None,
                        "name": order.name,
                        "amount": order.amount,
                        "active": order.active,
                        "tags": order.tags,
                    })

            if len(decoded_records) >= expected_count:
                break
            await asyncio.sleep(0.5)

    result = {
        "reader": "python",
        "topic_id": topic_id,
        "record_count": len(decoded_records),
        "records": decoded_records,
    }
    print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())
