# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""FL segment decoding.

A segment payload is a zstd-compressed concatenation of records. Each
record is: key length (zigzag varint i64, -1 for null) + key bytes,
then value length (zigzag varint i64) + value bytes.
"""

import zlib
from typing import Optional

import zstandard

from .exceptions import ProtocolException


def decode_segment(
    payload: bytes,
    expected_crc: Optional[int] = None,
) -> list[tuple[Optional[bytes], bytes]]:
    """Decode a compressed FL segment into (key, value) pairs.

    Verifies CRC-32 over the compressed payload when `expected_crc` is given.
    """
    if expected_crc is not None:
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ProtocolException(
                f"Segment CRC mismatch: expected {expected_crc}, got {actual_crc}"
            )

    data = zstandard.ZstdDecompressor().decompressobj().decompress(payload)

    records: list[tuple[Optional[bytes], bytes]] = []
    pos = 0
    while pos < len(data):
        key, pos = _read_bytes(data, pos, allow_null=True)
        value, pos = _read_bytes(data, pos, allow_null=False)
        records.append((key, value))
    return records


def _read_bytes(
    data: bytes, pos: int, allow_null: bool
) -> tuple[Optional[bytes], int]:
    length, pos = _read_zigzag_varint(data, pos)
    if length < 0:
        if length == -1 and allow_null:
            return None, pos
        raise ProtocolException(f"Invalid length {length} in segment")
    end = pos + length
    if end > len(data):
        raise ProtocolException("Truncated record in segment")
    return data[pos:end], end


def _read_zigzag_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ProtocolException("Truncated varint in segment")
        if shift >= 64:
            raise ProtocolException("Varint too long in segment")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            break
        shift += 7
    return (result >> 1) ^ -(result & 1), pos
