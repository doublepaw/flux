# Flux SDKs

Multi-language client libraries for Flux Event Bus.

## Available SDKs

| Language | Directory | Status |
|----------|-----------|--------|
| Java | [java/](java/) | Ready |
| Python | [python/](python/) | Ready |

## Features

All SDKs provide:

- **Writer**: Send records to topics with batching and retry
- **GroupReader**: Reader groups with automatic partition assignment
- **Authentication**: API key authentication support
- **Wire Protocol**: Native binary protocol (no HTTP overhead)

## Wire Protocol Compatibility

All SDKs implement the same binary wire protocol:

- Zigzag varint encoding (Avro-compatible)
- Message type prefix (1 byte)
- Length-prefixed strings and bytes

This ensures cross-language compatibility - a Java writer can send to a Python reader and vice versa.

## Quick Start

### Java

```java
// Writer
Writer writer = Writer.connect("ws://localhost:9000");
writer.sendOne(1, 0, 100, "key".getBytes(), "value".getBytes());
writer.close();

// Reader
ReaderConfig config = new ReaderConfig()
    .url("ws://localhost:9000")
    .groupId("my-group")
    .topicId(1);
GroupReader reader = GroupReader.join(config);
reader.startHeartbeat();
List<PartitionResult> results = reader.poll();
reader.commit();
reader.close();
```

### Python

```python
# Writer
async with Writer.connect("ws://localhost:9000") as writer:
    await writer.send_one(1, 0, 100, b"key", b"value")

# Reader
config = ReaderConfig(url="ws://localhost:9000", group_id="my-group", topic_id=1)
async with GroupReader.join(config) as reader:
    reader.start_heartbeat()
    results = await reader.poll()
    await reader.commit()
```

## Building

### Java

```bash
cd java/flux-sdk
mvn clean install
```

### Python

```bash
cd python
pip install -e .
pytest tests/
```
