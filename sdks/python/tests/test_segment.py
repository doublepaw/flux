"""Tests for FL segment decoding."""

import zlib

import pytest
import zstandard

from flux.segment import decode_segment
from flux.exceptions import ProtocolException


def encode_zigzag_varint(value: int) -> bytes:
    zigzag = (value << 1) ^ (value >> 63)
    out = bytearray()
    while zigzag >= 0x80:
        out.append((zigzag & 0x7F) | 0x80)
        zigzag >>= 7
    out.append(zigzag)
    return bytes(out)


def encode_segment(records: list[tuple[bytes | None, bytes]]) -> tuple[bytes, int]:
    """Hand-encode records into a compressed FL segment, returning (payload, crc)."""
    raw = bytearray()
    for key, value in records:
        if key is None:
            raw += encode_zigzag_varint(-1)
        else:
            raw += encode_zigzag_varint(len(key)) + key
        raw += encode_zigzag_varint(len(value)) + value
    payload = zstandard.ZstdCompressor().compress(bytes(raw))
    return payload, zlib.crc32(payload) & 0xFFFFFFFF


def test_decode_segment_round_trip():
    records = [(None, b"hello"), (b"k", b"world")]
    payload, crc = encode_segment(records)

    assert decode_segment(payload, expected_crc=crc) == records


def test_decode_segment_zigzag_encoding():
    # zigzag(-1) = 0x01 (null key), zigzag(5) = 0x0A, zigzag(1) = 0x02
    assert encode_zigzag_varint(-1) == b"\x01"
    assert encode_zigzag_varint(5) == b"\x0a"
    assert encode_zigzag_varint(1) == b"\x02"


def test_decode_segment_crc_mismatch_raises():
    payload, crc = encode_segment([(b"k", b"v")])

    with pytest.raises(ProtocolException, match="CRC mismatch"):
        decode_segment(payload, expected_crc=crc ^ 1)


def test_decode_segment_empty_payload():
    payload, crc = encode_segment([])

    assert decode_segment(payload, expected_crc=crc) == []


def test_decode_segment_truncated_record_raises():
    raw = encode_zigzag_varint(3) + b"ab"  # claims 3 key bytes, only 2 present
    payload = zstandard.ZstdCompressor().compress(raw)

    with pytest.raises(ProtocolException, match="Truncated"):
        decode_segment(payload)


def test_decode_segment_varint_length_over_127():
    value = bytes(range(256)) * 2  # 512 bytes: multi-byte varint length
    records = [(b"key", value)]
    payload, crc = encode_segment(records)

    assert decode_segment(payload, expected_crc=crc) == records
