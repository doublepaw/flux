# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""High-level typed client for Flux."""

import logging
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from .proto import flux_wire_pb2 as pb
from .writer import Writer, WriterConfig
from .reader import GroupReader, ReaderConfig
from ._admin import AdminClient
from .exceptions import FluxException

logger = logging.getLogger(__name__)


@dataclass
class ClientConfig:
    """Configuration for FluxClient."""

    ws_url: str = "ws://localhost:9000"
    admin_url: str = "http://localhost:9001"
    api_key: Optional[str] = None
    max_in_flight: int = 256
    timeout: float = 30.0


class FluxClient:
    """High-level typed client. Handles schema registration, serialization,
    and topic resolution automatically.

    Usage:
        async with FluxClient.connect(ClientConfig(api_key="tb_...")) as client:
            await client.send(OrderEvent(order_id="abc", amount=100))

            async for event in client.consume(OrderEvent, group_id="my-group"):
                print(event.order_id)
    """

    def __init__(
        self,
        writer: Writer,
        admin: AdminClient,
        config: ClientConfig,
    ):
        self._writer = writer
        self._admin = admin
        self._config = config
        self._topic_cache: dict[str, int] = {}  # name -> topic_id
        self._schema_cache: dict[type, int] = {}  # cls -> schema_id

    @classmethod
    async def connect(cls, config: ClientConfig) -> "FluxClient":
        writer_config = WriterConfig(
            url=config.ws_url,
            api_key=config.api_key,
            max_in_flight=config.max_in_flight,
            timeout=config.timeout,
        )
        writer = await Writer.connect_with_config(writer_config)
        admin = AdminClient(config.admin_url, config.api_key)
        return cls(writer, admin, config)

    async def send(
        self,
        obj,
        *,
        key: bytes | None = None,
        topic: str | None = None,
    ) -> pb.BatchAck:
        """Send a single typed object.

        Topic is resolved from the class's @schema(topic=...) unless overridden.
        """
        cls = type(obj)
        topic_name = topic or getattr(cls, "__flux_topic__", None)
        if topic_name is None:
            raise FluxException(
                f"{cls.__name__} has no topic. Use @schema(topic=...) or pass topic="
            )

        topic_id = await self._resolve_topic(topic_name)
        schema_id = await self._resolve_schema(cls, topic_id)
        value = obj.to_bytes()

        record = pb.Record(value=value)
        if key is not None:
            record.key = key

        return await self._writer.send(topic_id, schema_id, [record])

    async def send_batch(
        self,
        objects: list,
        *,
        key: bytes | None = None,
        topic: str | None = None,
    ) -> list[pb.BatchAck]:
        """Send a batch of typed objects (all same type)."""
        if not objects:
            return []

        cls = type(objects[0])
        topic_name = topic or getattr(cls, "__flux_topic__", None)
        if topic_name is None:
            raise FluxException(
                f"{cls.__name__} has no topic. Use @schema(topic=...) or pass topic="
            )

        topic_id = await self._resolve_topic(topic_name)
        schema_id = await self._resolve_schema(cls, topic_id)

        records = []
        for obj in objects:
            record = pb.Record(value=obj.to_bytes())
            if key is not None:
                record.key = key
            records.append(record)

        batch = pb.RecordBatch(
            topic_id=topic_id,
            schema_id=schema_id,
            records=records,
        )
        return await self._writer.send_batch([batch])

    async def consume(
        self,
        cls: type,
        *,
        group_id: str = "default",
        topic: str | None = None,
        poll_interval: float = 0.1,
    ) -> AsyncIterator:
        """Consume typed objects from a topic.

        Yields deserialized instances of `cls`.
        """
        topic_name = topic or getattr(cls, "__flux_topic__", None)
        if topic_name is None:
            raise FluxException(
                f"{cls.__name__} has no topic. Use @schema(topic=...) or pass topic="
            )

        topic_id = await self._resolve_topic(topic_name)
        reader_config = ReaderConfig(
            url=self._config.ws_url,
            api_key=self._config.api_key,
            group_id=group_id,
            topic_id=topic_id,
            timeout=self._config.timeout,
        )

        async with GroupReader.join(reader_config) as reader:
            reader.start_heartbeat()
            async for batch in reader.poll_loop(interval=poll_interval):
                for topic_result in batch.results:
                    for record in topic_result.records:
                        yield cls.from_bytes(record.value)
                await reader.commit(batch)

    async def _resolve_topic(self, name: str) -> int:
        """Resolve topic name -> topic_id. Cached after first call."""
        if name in self._topic_cache:
            return self._topic_cache[name]

        topics = await self._admin.list_topics()
        for t in topics:
            self._topic_cache[t["name"]] = t["topic_id"]

        if name not in self._topic_cache:
            raise FluxException(f"Topic not found: {name}")

        return self._topic_cache[name]

    async def _resolve_schema(self, cls: type, topic_id: int) -> int:
        """Resolve class -> schema_id. Cached after first call per type."""
        if cls in self._schema_cache:
            return self._schema_cache[cls]

        schema_id = await self._admin.register_schema(topic_id, cls.schema())
        self._schema_cache[cls] = schema_id
        return schema_id

    async def close(self) -> None:
        await self._writer.close()

    async def __aenter__(self) -> "FluxClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()