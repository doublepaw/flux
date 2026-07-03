# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Zero-copy reader: fetches compressed FL segments and decodes them locally."""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import websockets
from websockets.asyncio.client import ClientConnection

from flux.proto import flux_wire_pb2 as pb

from .exceptions import (
    AuthenticationException,
    ConnectionException,
    ProtocolException,
    TimeoutException,
)
from .reader import ReaderConfig
from .segment import decode_segment

logger = logging.getLogger(__name__)


@dataclass
class RawSegmentBatch:
    """Records decoded from a single FL segment, with its offset range."""

    topic_id: int
    schema_id: int
    start_offset: int
    end_offset: int
    records: list[tuple[Optional[bytes], bytes]]


@dataclass
class RawReadResult:
    """Result of a raw read: decoded segments plus the topic high watermark."""

    segments: list[RawSegmentBatch]
    high_watermark: int

    @property
    def records(self) -> list[tuple[Optional[bytes], bytes]]:
        """All (key, value) pairs across segments, in offset order."""
        return [record for segment in self.segments for record in segment.records]

    @property
    def next_offset(self) -> Optional[int]:
        """Offset to request next, or None if this read returned no segments."""
        return self.segments[-1].end_offset if self.segments else None


class RawReader:
    """Reader that requests raw FL segments and decodes records client-side.

    Unlike GroupReader, reads are addressed by explicit offset and do not
    participate in a reader group. `max_bytes` budgets *compressed* bytes.
    """

    def __init__(self, ws: ClientConnection, config: ReaderConfig):
        self._ws = ws
        self._config = config

    @classmethod
    async def connect(cls, config: ReaderConfig) -> "RawReader":
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

    async def read(
        self,
        offset: int,
        max_bytes: Optional[int] = None,
    ) -> RawReadResult:
        """Read raw segments starting at `offset` and decode their records."""
        req = pb.RawReadRequest(
            topic_id=self._config.topic_id,
            offset=offset,
            max_bytes=max_bytes if max_bytes is not None else self._config.max_bytes,
        )
        envelope = pb.ClientMessage()
        envelope.raw_read.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Raw read timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "raw_read":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.raw_read
        if not resp.success:
            raise ProtocolException(
                f"Raw read failed ({resp.error_code}): {resp.error_message}"
            )

        segments = [
            RawSegmentBatch(
                topic_id=seg.topic_id,
                schema_id=seg.schema_id,
                start_offset=seg.start_offset,
                end_offset=seg.end_offset,
                records=decode_segment(seg.payload, expected_crc=seg.crc32),
            )
            for seg in resp.segments
        ]
        return RawReadResult(segments=segments, high_watermark=resp.high_watermark)

    async def close(self) -> None:
        await self._ws.close()

    async def __aenter__(self) -> "RawReader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
