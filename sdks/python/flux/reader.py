# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Reader client for reading messages from Flux."""

import asyncio
import logging
from dataclasses import dataclass
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
from .segment import decode_segment

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
    raw: bool = False  # poll compressed segments and decode client-side
    pipeline_depth: int = 2  # outstanding poll requests (clamped to 1..8)

    def __post_init__(self):
        if self.reader_id is None:
            self.reader_id = str(uuid4())
        self.pipeline_depth = max(1, min(8, self.pipeline_depth))


class ReaderState(Enum):
    """Reader state."""

    INIT = "init"
    ACTIVE = "active"
    STOPPED = "stopped"


class GroupReader:
    """Reader client with reader group support.

    Uses a poll-based model where the broker dispatches work to readers.

    A single receive task routes each ServerMessage to a per-type queue so
    concurrent request paths (poll prefetcher, commit, heartbeat loop) each
    await their own replies instead of racing on ``recv()``. A background
    prefetcher keeps ``pipeline_depth`` poll requests outstanding and decodes
    responses ahead of the application, so network transfer, decode, and
    application processing overlap.
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

        self._queues: dict[str, asyncio.Queue] = {}
        self._recv_task: Optional[asyncio.Task] = None

        self._ready_batches: Optional[asyncio.Queue] = None
        self._prefetch_task: Optional[asyncio.Task] = None
        self._prefetch_error: Optional[Exception] = None

    @classmethod
    async def join(cls, config: ReaderConfig) -> "GroupReader":
        try:
            ws = await asyncio.wait_for(
                # No frame size cap: poll responses scale with max_bytes.
                websockets.connect(config.url, max_size=None),
                timeout=config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Connection timeout")
        except Exception as e:
            raise ConnectionException(f"Failed to connect: {e}")

        reader = cls(ws, config)
        reader._recv_task = asyncio.create_task(reader._recv_loop())
        if config.api_key:
            await reader._authenticate(config.api_key)
        await reader._do_join()
        reader._start_prefetcher()
        return reader

    def _queue_for(self, kind: str) -> asyncio.Queue:
        queue = self._queues.get(kind)
        if queue is None:
            queue = self._queues[kind] = asyncio.Queue()
        return queue

    async def _recv_loop(self) -> None:
        """Route each server message to its per-type queue by oneof case."""
        try:
            async for response in self._ws:
                if not isinstance(response, bytes):
                    logger.warning("Received text message, expected binary")
                    continue
                server_msg = pb.ServerMessage()
                server_msg.ParseFromString(response)
                kind = server_msg.WhichOneof("message") or "unknown"
                self._queue_for(kind).put_nowait(server_msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("Receive loop ended: %s", e)

    async def _expect(self, kind: str, timeout_error: str) -> pb.ServerMessage:
        """Await the next server message of the given oneof case."""
        try:
            return await asyncio.wait_for(
                self._queue_for(kind).get(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException(timeout_error)

    async def _authenticate(self, api_key: str) -> None:
        envelope = pb.ClientMessage()
        envelope.auth.api_key = api_key
        await self._ws.send(envelope.SerializeToString())

        server_msg = await self._expect("auth", "Authentication timeout")
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

        for task in (self._heartbeat_task, self._prefetch_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        await self._do_leave()

    @property
    def state(self) -> ReaderState:
        return self._state

    def _start_prefetcher(self) -> None:
        depth = self._config.pipeline_depth
        self._ready_batches = asyncio.Queue(maxsize=depth)
        self._prefetch_task = asyncio.create_task(self._prefetch_loop(depth))

    async def _send_poll_request(self) -> None:
        req = pb.PollRequest(
            group_id=self._config.group_id,
            topic_id=self._config.topic_id,
            reader_id=self._config.reader_id,
            max_bytes=self._config.max_bytes,
            raw=self._config.raw,
        )
        envelope = pb.ClientMessage()
        envelope.poll.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

    async def _prefetch_loop(self, depth: int) -> None:
        """Keep ``depth`` poll requests outstanding; decode each response and
        queue the ready batch (bounded queue provides backpressure). Backs off
        briefly after empty leases to avoid hot-polling an idle topic.
        """
        try:
            for _ in range(depth):
                await self._send_poll_request()
            while self._running:
                try:
                    server_msg = await asyncio.wait_for(
                        self._queue_for("poll").get(),
                        timeout=self._config.timeout,
                    )
                except asyncio.TimeoutError:
                    # No poll in flight completed within the timeout window;
                    # the requests are still outstanding, keep waiting.
                    continue

                resp = server_msg.poll
                if not resp.success:
                    raise ProtocolException(
                        f"Poll failed ({resp.error_code}): {resp.error_message}"
                    )

                if resp.start_offset != resp.end_offset:
                    async with self._lock:
                        self._inflight.append((resp.start_offset, resp.end_offset))

                results = list(resp.results)
                for segment in resp.raw_segments:
                    results.append(_decode_raw_segment(segment, resp.end_offset))

                empty = resp.start_offset == resp.end_offset
                await self._ready_batches.put(
                    PollBatch(
                        results=results,
                        start_offset=resp.start_offset,
                        end_offset=resp.end_offset,
                        lease_deadline_ms=resp.lease_deadline_ms,
                    )
                )

                if empty:
                    await asyncio.sleep(0.025)
                if self._running:
                    await self._send_poll_request()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._prefetch_error = e

    async def poll(self) -> PollBatch:
        async with self._lock:
            if self._state != ReaderState.ACTIVE:
                raise ProtocolException(f"Reader not active: {self._state}")

        if self._prefetch_error is not None:
            raise self._prefetch_error

        try:
            return await asyncio.wait_for(
                self._ready_batches.get(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            if self._prefetch_error is not None:
                raise self._prefetch_error
            raise TimeoutException("Poll timeout")

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

        server_msg = await self._expect("commit", "Commit timeout")
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

        server_msg = await self._expect("join_group", "Join timeout")
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

                server_msg = await self._expect("heartbeat", "Heartbeat timeout")
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
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        await self._ws.close()

    async def __aenter__(self) -> "GroupReader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        await self.close()


def _decode_raw_segment(segment: pb.RawSegment, high_watermark: int) -> pb.TopicResult:
    """Decode a compressed raw segment into the same shape as a classic poll result."""
    result = pb.TopicResult(
        topic_id=segment.topic_id,
        schema_id=segment.schema_id,
        high_watermark=high_watermark,
    )
    for key, value in decode_segment(segment.payload, expected_crc=segment.crc32):
        record = result.records.add()
        record.value = value
        if key is not None:
            record.key = key
    return result
