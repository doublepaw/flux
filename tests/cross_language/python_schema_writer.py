#!/usr/bin/env python3
"""
Cross-language schema E2E test: Python writer with Avro-encoded values.

Usage:
    python python_schema_writer.py <url> <topic_id>

Serializes a TestOrder with to_bytes(), sends as record value, prints JSON result.
"""

import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from dataclasses import dataclass
from flux import Writer, WriterConfig, schema
from flux.proto import flux_wire_pb2 as pb


@schema
@dataclass
class TestOrder:
    name: str
    amount: int
    active: bool
    tags: list[str]


async def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <url> <topic_id>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])

    order = TestOrder(name="widget", amount=42, active=True, tags=["rush", "fragile"])
    value_bytes = order.to_bytes()

    config = WriterConfig(url=url, timeout=30.0)
    writer = await Writer.connect_with_config(config)
    async with writer:
        record = pb.Record(key=b"order-1", value=value_bytes)
        ack = await writer.send(topic_id, 100, [record])

        result = {
            "writer": "python",
            "topic_id": topic_id,
            "start_offset": ack.start_offset,
            "end_offset": ack.end_offset,
            "record_count": 1,
            "schema_json": TestOrder.schema_json(),
            "fields": {"name": "widget", "amount": 42, "active": True, "tags": ["rush", "fragile"]},
        }
        print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())
