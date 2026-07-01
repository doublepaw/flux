# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Writer client for sending messages to Flux."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional
from uuid import UUID, uuid4

import websockets
from websockets.asyncio.client import ClientConnection

from flux.proto import flux_wire_pb2 as pb

from .exceptions import (
    AuthenticationException,
    BackpressureException,
    ConnectionException,
    ProtocolException,
    TimeoutException,
)

logger = logging.getLogger(__name__)

ERR_BACKPRESSURE = 1004


@dataclass
class WriterConfig:
    """Configuration for the Flux writer."""

    url: str = "ws://localhost:9000"
    api_key: Optional[str] = None
    max_retries: int = 5
    initial_backoff: float = 0.1  # seconds
    max_backoff: float = 10.0  # seconds
    timeout: float = 30.0  # seconds
    max_in_flight: int = 256


class Writer:
    """Writer client for sending messages to Flux."""

    def __init__(
        self,
        ws: ClientConnection,
        config: WriterConfig,
        writer_id: UUID,
    ):
        self._ws = ws
        self._config = config
        self._writer_id = writer_id
        self._seq = 1
        self._lock = asyncio.Lock()
        self._in_flight = asyncio.Semaphore(max(1, config.max_in_flight))
        self._pending: dict[int, asyncio.Future[list[pb.BatchAck]]] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._closed = False

    @classmethod
    async def connect(cls, url: str) -> "Writer":
        return await cls.connect_with_config(WriterConfig(url=url))

    @classmethod
    async def connect_with_config(cls, config: WriterConfig) -> "Writer":
        try:
            ws = await asyncio.wait_for(
                websockets.connect(config.url),
                timeout=config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Connection timeout")
        except Exception as e:
            raise ConnectionException(f"Failed to connect: {e}")

        writer = cls(ws, config, uuid4())
        if config.api_key:
            await writer._authenticate(config.api_key)
        writer._start_reader()
        return writer

    def _start_reader(self) -> None:
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(self._reader_loop())

    async def _authenticate(self, api_key: str) -> None:
        envelope = pb.ClientMessage()
        envelope.auth.api_key = api_key
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Authentication timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "auth":
            raise ProtocolException("Unexpected auth response")

        auth_resp = server_msg.auth
        if not auth_resp.success:
            raise AuthenticationException(auth_resp.error_message)

        logger.debug("Authentication successful")

    @property
    def writer_id(self) -> UUID:
        return self._writer_id

    async def send(
        self,
        topic_id: int,
        schema_id: int,
        records: list[pb.Record],
    ) -> pb.BatchAck:
        batch = pb.RecordBatch(
            topic_id=topic_id,
            schema_id=schema_id,
            records=records,
        )
        acks = await self.send_batch([batch])
        if not acks:
            raise ProtocolException("No acks received")
        return acks[0]

    async def send_one(
        self,
        topic_id: int,
        schema_id: int,
        key: Optional[bytes],
        value: bytes,
    ) -> pb.BatchAck:
        record = pb.Record(value=value)
        if key is not None:
            record.key = key
        return await self.send(topic_id, schema_id, [record])

    async def send_batch(self, batches: list[pb.RecordBatch]) -> list[pb.BatchAck]:
        async with self._lock:
            current_seq = self._seq
            self._seq += 1

        return await self._send_batch_with_seq(current_seq, batches)

    async def _send_batch_with_seq(
        self, current_seq: int, batches: list[pb.RecordBatch]
    ) -> list[pb.BatchAck]:
        retries = 0
        backoff = self._config.initial_backoff

        while True:
            try:
                acks = await self._send_request(current_seq, batches)
            except BackpressureException:
                if retries >= self._config.max_retries:
                    raise BackpressureException(
                        f"Server backpressure after {retries} retries"
                    )

                logger.warning(
                    "Backpressure from server, retrying in %.2fs (attempt %d/%d)",
                    backoff,
                    retries + 1,
                    self._config.max_retries,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._config.max_backoff)
                retries += 1
                continue

            logger.debug("Sent %d batches, got %d acks", len(batches), len(acks))
            return acks

    def send_batch_async(self, batches: list[pb.RecordBatch]) -> asyncio.Task:
        return asyncio.create_task(self.send_batch(batches))

    def send_async(
        self,
        topic_id: int,
        schema_id: int,
        records: list[pb.Record],
    ) -> asyncio.Task:
        return asyncio.create_task(
            self.send(topic_id, schema_id, records)
        )

    async def _send_request(
        self, seq: int, batches: list[pb.RecordBatch]
    ) -> list[pb.BatchAck]:
        if self._closed:
            raise ConnectionException("Writer is closed")

        req = pb.AppendRequest(
            writer_id=self._writer_id.bytes,
            append_seq=seq,
            batches=batches,
        )
        envelope = pb.ClientMessage()
        envelope.append.CopyFrom(req)
        loop = asyncio.get_running_loop()
        response_future: asyncio.Future[list[pb.BatchAck]] = loop.create_future()

        await self._in_flight.acquire()
        self._pending[seq] = response_future
        try:
            await self._ws.send(envelope.SerializeToString())
        except Exception as e:
            self._pending.pop(seq, None)
            self._in_flight.release()
            raise ConnectionException(f"Send failed: {e}")

        try:
            return await asyncio.wait_for(
                response_future,
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            self._pending.pop(seq, None)
            raise TimeoutException("Request timeout")
        finally:
            self._in_flight.release()

    async def _reader_loop(self) -> None:
        while True:
            try:
                response = await self._ws.recv()
            except Exception as e:
                if not self._closed:
                    self._fail_all_pending(ConnectionException(f"Connection closed: {e}"))
                break

            if not isinstance(response, bytes):
                continue

            server_msg = pb.ServerMessage()
            try:
                server_msg.ParseFromString(response)
            except Exception:
                continue

            if server_msg.WhichOneof("message") != "append":
                continue

            resp = server_msg.append
            future = self._pending.pop(resp.append_seq, None)
            if future is None or future.done():
                continue

            if not resp.success:
                if resp.error_code == ERR_BACKPRESSURE:
                    future.set_exception(BackpressureException("Server backpressure"))
                else:
                    future.set_exception(
                        ProtocolException(
                            f"Server error {resp.error_code}: {resp.error_message}"
                        )
                    )
                continue

            future.set_result(list(resp.append_acks))

    def _fail_all_pending(self, exc: Exception) -> None:
        for seq, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(exc)
            self._pending.pop(seq, None)

    async def close(self) -> None:
        self._closed = True
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        self._fail_all_pending(ConnectionException("Writer closed"))
        await self._ws.close()

    async def __aenter__(self) -> "Writer":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()