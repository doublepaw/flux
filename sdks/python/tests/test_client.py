"""Tests for FluxClient with mocked admin and writer."""

from dataclasses import dataclass
from typing import Annotated
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from flux import schema, NonNull, FluxException
from flux.client import FluxClient, ClientConfig
from flux._admin import AdminClient
from flux.proto import flux_wire_pb2 as pb


# ---- test fixtures ----

@schema(topic="orders", namespace="com.example")
@dataclass
class OrderEvent:
    order_id: Annotated[str, NonNull]
    amount: int


@schema
@dataclass
class NoTopicEvent:
    value: int


MOCK_TOPICS = [
    {"topic_id": 1, "name": "orders"},
    {"topic_id": 2, "name": "events"},
]


def make_client(writer=None, admin=None):
    writer = writer or AsyncMock()
    admin = admin or AsyncMock()
    config = ClientConfig(ws_url="ws://test:9000", admin_url="http://test:9001")
    return FluxClient(writer, admin, config)


# ---- topic resolution ----

@pytest.mark.asyncio
async def test_send_resolves_topic_from_decorator():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send = AsyncMock(return_value=pb.BatchAck())

    client = make_client(writer=writer, admin=admin)
    await client.send(OrderEvent(order_id="abc", amount=100))

    admin.list_topics.assert_called_once()
    admin.register_schema.assert_called_once()
    writer.send.assert_called_once()
    call_args = writer.send.call_args
    assert call_args[0][0] == 1  # topic_id


@pytest.mark.asyncio
async def test_send_topic_override():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send = AsyncMock(return_value=pb.BatchAck())

    client = make_client(writer=writer, admin=admin)
    await client.send(OrderEvent(order_id="abc", amount=100), topic="events")

    call_args = writer.send.call_args
    assert call_args[0][0] == 2  # topic_id for "events"


@pytest.mark.asyncio
async def test_send_no_topic_raises():
    client = make_client()
    with pytest.raises(FluxException, match="has no topic"):
        await client.send(NoTopicEvent(value=1))


@pytest.mark.asyncio
async def test_topic_not_found_raises():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    client = make_client(admin=admin)

    with pytest.raises(FluxException, match="Topic not found"):
        await client.send(OrderEvent(order_id="x", amount=1), topic="nonexistent")


# ---- schema registration ----

@pytest.mark.asyncio
async def test_schema_cached_after_first_send():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send = AsyncMock(return_value=pb.BatchAck())

    client = make_client(writer=writer, admin=admin)
    await client.send(OrderEvent(order_id="a", amount=1))
    await client.send(OrderEvent(order_id="b", amount=2))

    # admin called once for topics, once for schema
    assert admin.list_topics.call_count == 1
    assert admin.register_schema.call_count == 1


# ---- serialization ----

@pytest.mark.asyncio
async def test_send_serializes_with_to_bytes():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send = AsyncMock(return_value=pb.BatchAck())

    client = make_client(writer=writer, admin=admin)
    event = OrderEvent(order_id="abc", amount=100)
    await client.send(event)

    call_args = writer.send.call_args
    records = call_args[0][2]
    assert len(records) == 1
    # Verify the value is the serialized event
    assert records[0].value == event.to_bytes()


@pytest.mark.asyncio
async def test_send_with_key_sets_record_key():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send = AsyncMock(return_value=pb.BatchAck())

    client = make_client(writer=writer, admin=admin)
    await client.send(OrderEvent(order_id="abc", amount=100), key=b"mykey")

    call_args = writer.send.call_args
    records = call_args[0][2]
    assert records[0].key == b"mykey"


# ---- send_batch ----

@pytest.mark.asyncio
async def test_send_batch_empty():
    client = make_client()
    result = await client.send_batch([])
    assert result == []


@pytest.mark.asyncio
async def test_send_batch_multiple():
    admin = AsyncMock()
    admin.list_topics = AsyncMock(return_value=MOCK_TOPICS)
    admin.register_schema = AsyncMock(return_value=42)
    writer = AsyncMock()
    writer.send_batch = AsyncMock(return_value=[pb.BatchAck()])

    client = make_client(writer=writer, admin=admin)
    events = [OrderEvent(order_id=str(i), amount=i) for i in range(3)]
    await client.send_batch(events)

    call_args = writer.send_batch.call_args
    batches = call_args[0][0]
    assert len(batches) == 1
    assert batches[0].topic_id == 1
    assert len(batches[0].records) == 3


# ---- @schema(topic=...) integration ----

def test_schema_decorator_stores_topic():
    assert OrderEvent.__flux_topic__ == "orders"


def test_schema_decorator_no_topic():
    assert not hasattr(NoTopicEvent, "__flux_topic__")


# ---- context manager ----

@pytest.mark.asyncio
async def test_context_manager_closes():
    writer = AsyncMock()
    writer.close = AsyncMock()
    client = make_client(writer=writer)

    async with client:
        pass

    writer.close.assert_called_once()
