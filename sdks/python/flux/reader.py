# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Reader client for reading messages from Flux."""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import AsyncIterator, Optional
from uuid import uuid4

import websockets
from websockets.asyncio.client import ClientConnection

from flux.proto import flux_wire_pb2 as pb

from .exceptions import (
    AuthenticationException,
    ConnectionException,
    ProtocolException,
    TimeoutException,
)

logger = logging.getLogger(__name__)


@dataclass
class PollBatch:
    """A batch of results from a single poll, with offset range and lease deadline.

    Pass this to ``commit(batch)`` to commit the specific range.
    """

    results: list
    start_offset: int = 0
    end_offset: int = 0
    lease_deadline_ms: int = 0


@dataclass
class ReaderConfig:
    """Configuration for the Flux reader."""

    url: str = "ws://localhost:9000"
    api_key: Optional[str] = None
    group_id: str = "default"
    reader_id: Optional[str] = None
    topic_id: int = 1
    max_bytes: int = 1024 * 1024  # 1 MB
    timeout: float = 30.0  # seconds
    heartbeat_interval: float = 10.0  # seconds

    def __post_init__(self):
        if self.reader_id is None:
            self.reader_id = str(uuid4())


class ReaderState(Enum):
    """Reader state."""

    INIT = "init"
    ACTIVE = "active"
    STOPPED = "stopped"


class GroupReader:
    """Reader client with reader group support.

    Uses a poll-based model where the broker dispatches work to readers.
    """

    def __init__(
        self,
        ws: ClientConnection,
        config: ReaderConfig,
    ):
        self._ws = ws
        self._config = config
        self._state = ReaderState.INIT
        self._inflight: list[tuple[int, int]] = []
        self._running = True
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @classmethod
    async def join(cls, config: ReaderConfig) -> "GroupReader":
        try:
            ws = await asyncio.wait_for(
                websockets.connect(config.url),
                timeout=config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Connection timeout")
        except Exception as e:
            raise ConnectionException(f"Failed to connect: {e}")

        reader = cls(ws, config)
        if config.api_key:
            await reader._authenticate(config.api_key)
        await reader._do_join()
        return reader

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

    def start_heartbeat(self) -> None:
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        self._running = False

        async with self._lock:
            self._state = ReaderState.STOPPED

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self._do_leave()

    @property
    def state(self) -> ReaderState:
        return self._state

    async def poll(self) -> PollBatch:
        async with self._lock:
            if self._state != ReaderState.ACTIVE:
                raise ProtocolException(f"Reader not active: {self._state}")

        req = pb.PollRequest(
            group_id=self._config.group_id,
            topic_id=self._config.topic_id,
            reader_id=self._config.reader_id,
            max_bytes=self._config.max_bytes,
        )
        envelope = pb.ClientMessage()
        envelope.poll.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Poll timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "poll":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.poll
        if not resp.success:
            raise ProtocolException(
                f"Poll failed ({resp.error_code}): {resp.error_message}"
            )

        if resp.start_offset != resp.end_offset:
            async with self._lock:
                self._inflight.append((resp.start_offset, resp.end_offset))

        return PollBatch(
            results=list(resp.results),
            start_offset=resp.start_offset,
            end_offset=resp.end_offset,
            lease_deadline_ms=resp.lease_deadline_ms,
        )

    async def poll_loop(
        self, interval: float = 0.1
    ) -> AsyncIterator[PollBatch]:
        while self._running:
            try:
                batch = await self.poll()
                if batch.results:
                    yield batch
                else:
                    await asyncio.sleep(interval)
            except TimeoutException as e:
                logger.warning("Poll timeout, retrying: %s", e)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    async def commit(self, batch: PollBatch) -> None:
        if batch.start_offset == batch.end_offset:
            return

        req = pb.CommitRequest(
            group_id=self._config.group_id,
            reader_id=self._config.reader_id,
            topic_id=self._config.topic_id,
            start_offset=batch.start_offset,
            end_offset=batch.end_offset,
        )
        envelope = pb.ClientMessage()
        envelope.commit.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Commit timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "commit":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.commit
        if not resp.success:
            raise ProtocolException(
                f"Commit failed ({resp.error_code}): {resp.error_message}"
            )

        async with self._lock:
            s, e = batch.start_offset, batch.end_offset
            self._inflight = [(ss, ee) for ss, ee in self._inflight if ss != s or ee != e]

    async def _do_join(self) -> None:
        req = pb.JoinGroupRequest(
            group_id=self._config.group_id,
            reader_id=self._config.reader_id,
            topic_ids=[self._config.topic_id],
        )
        envelope = pb.ClientMessage()
        envelope.join_group.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Join timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "join_group":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.join_group
        if not resp.success:
            raise ProtocolException(
                f"Join failed ({resp.error_code}): {resp.error_message}"
            )

        async with self._lock:
            self._inflight.clear()
            self._state = ReaderState.ACTIVE

        logger.info("Joined group %s", self._config.group_id)

    async def _do_leave(self) -> None:
        async with self._lock:
            ranges = list(self._inflight)
        for start, end in ranges:
            try:
                batch = PollBatch(results=[], start_offset=start, end_offset=end)
                await self.commit(batch)
            except Exception as e:
                logger.warning("Failed to commit range [%d, %d) during leave: %s", start, end, e)

        req = pb.LeaveGroupRequest(
            group_id=self._config.group_id,
            topic_id=self._config.topic_id,
            reader_id=self._config.reader_id,
        )
        envelope = pb.ClientMessage()
        envelope.leave_group.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        logger.info("Left group %s", self._config.group_id)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval)
            if not self._running:
                break

            try:
                req = pb.HeartbeatRequest(
                    group_id=self._config.group_id,
                    topic_id=self._config.topic_id,
                    reader_id=self._config.reader_id,
                )
                envelope = pb.ClientMessage()
                envelope.heartbeat.CopyFrom(req)
                await self._ws.send(envelope.SerializeToString())

                response = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=self._config.timeout,
                )

                if not isinstance(response, bytes):
                    continue

                server_msg = pb.ServerMessage()
                server_msg.ParseFromString(response)
                if server_msg.WhichOneof("message") != "heartbeat":
                    continue

                resp = server_msg.heartbeat
                if not resp.success:
                    logger.warning(
                        "Heartbeat failed (%d): %s",
                        resp.error_code,
                        resp.error_message,
                    )
                    continue

                if resp.status == pb.HEARTBEAT_STATUS_OK:
                    logger.debug("Heartbeat OK")
                elif resp.status == pb.HEARTBEAT_STATUS_UNKNOWN_MEMBER:
                    logger.warning("Unknown member, rejoining group")
                    await self._do_join()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Heartbeat failed: %s", e)

    async def close(self) -> None:
        await self._ws.close()

    async def __aenter__(self) -> "GroupReader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        await self.close()