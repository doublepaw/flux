# Flux Python SDK

Python client library for Flux Event Bus.

## Installation

```bash
pip install flux-sdk
```

Or install from source:

```bash
cd sdks/python
pip install -e .
```

## Usage

### FluxClient (High-Level API)

```python
import asyncio
from dataclasses import dataclass
from typing import Annotated
from flux import FluxClient, ClientConfig, schema, NonNull

@schema(topic="orders", namespace="com.example")
@dataclass
class OrderEvent:
    order_id: Annotated[str, NonNull]
    amount: int

async def main():
    config = ClientConfig(
        ws_url="ws://localhost:9000",
        admin_url="http://localhost:9001",
        api_key="tb_your_api_key",
    )

    async with FluxClient.connect(config) as client:
        # Send — one call handles schema registration + serialization + partitioning
        await client.send(OrderEvent(order_id="abc", amount=100))
        await client.send(OrderEvent(order_id="abc", amount=100), key=b"abc")       # key-based partition
        await client.send(OrderEvent(order_id="abc", amount=100), partition=2)       # explicit partition
        await client.send(OrderEvent(order_id="abc", amount=100), topic="orders-stg") # topic override

        # Read — typed objects
        async for event in client.consume(OrderEvent, group_id="my-group"):
            print(event.order_id)

asyncio.run(main())
```

### Writer (Low-Level API)

```python
import asyncio
from flux import Writer, WriterConfig
from flux.proto import flux_wire_pb2 as pb

async def main():
    # Connect
    async with Writer.connect("ws://localhost:9000") as writer:
        # Send records
        ack = await writer.send(
            topic_id=1,
            partition_id=0,
            schema_id=100,
            records=[
                pb.Record(key=b"key1", value=b"value1"),
                pb.Record(key=b"key2", value=b"value2"),
            ],
        )
        print(f"Wrote records at offsets {ack.start_offset} to {ack.end_offset}")

    # Or with authentication
    config = WriterConfig(
        url="ws://localhost:9000",
        api_key="tb_your_api_key",
    )
    async with Writer.connect_with_config(config) as writer:
        await writer.send_one(1, 0, 100, b"key", b"value")

asyncio.run(main())
```

### Reader (with Reader Groups)

```python
import asyncio
from flux import GroupReader, ReaderConfig

async def main():
    config = ReaderConfig(
        url="ws://localhost:9000",
        group_id="my-group",
        topic_id=1,
    )

    async with GroupReader.join(config) as reader:
        # Start heartbeat loop
        reader.start_heartbeat()

        # Poll for records
        async for results in reader.poll_loop():
            for result in results:
                for record in result.records:
                    print(f"Received: {record.value}")

            # Commit offsets
            await reader.commit()

asyncio.run(main())
```

### Simple Polling

```python
import asyncio
from flux import GroupReader, ReaderConfig

async def main():
    config = ReaderConfig(
        url="ws://localhost:9000",
        group_id="my-group",
        topic_id=1,
    )

    async with GroupReader.join(config) as reader:
        reader.start_heartbeat()

        while True:
            results = await reader.poll()
            for result in results:
                print(f"Partition {result.partition_id}: {len(result.records)} records")

            await reader.commit()
            await asyncio.sleep(0.1)

asyncio.run(main())
```

## Configuration

### ClientConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ws_url` | `ws://localhost:9000` | WebSocket URL for data plane |
| `admin_url` | `http://localhost:9001` | HTTP URL for admin API |
| `api_key` | `None` | API key for authentication |
| `max_in_flight` | `256` | Max concurrent in-flight requests |
| `timeout` | `30.0` | Request timeout in seconds |

### WriterConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `ws://localhost:9000` | Server URL |
| `api_key` | `None` | API key for authentication |
| `max_retries` | `5` | Max retries on backpressure |
| `initial_backoff` | `0.1` | Initial backoff in seconds |
| `max_backoff` | `10.0` | Maximum backoff in seconds |
| `timeout` | `30.0` | Request timeout in seconds |

### ReaderConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `ws://localhost:9000` | Server URL |
| `api_key` | `None` | API key for authentication |
| `group_id` | `default` | Reader group ID |
| `reader_id` | `<uuid>` | Reader ID within group |
| `topic_id` | `1` | Topic to subscribe to |
| `max_bytes` | `1048576` | Max bytes per read (1MB) |
| `timeout` | `30.0` | Request timeout in seconds |
| `heartbeat_interval` | `10.0` | Heartbeat interval in seconds |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## Wire Protocol

The SDK uses generated protobuf messages from `proto/flux_wire.proto` for:
- outer WebSocket envelope (`ClientMessage`/`ServerMessage`)
- all request/response payloads
