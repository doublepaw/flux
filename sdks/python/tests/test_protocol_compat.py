"""Protocol and compatibility tests for Python SDK protobuf envelopes."""

import pytest
from google.protobuf.message import DecodeError

from flux.proto import flux_wire_pb2 as pb


def _build_append_envelope() -> pb.ClientMessage:
    msg = pb.ClientMessage()
    msg.append.writer_id = bytes(range(16))
    msg.append.append_seq = 7

    batch = msg.append.batches.add()
    batch.topic_id = 9
    batch.schema_id = 100

    r1 = batch.records.add()
    r1.key = b"k1"
    r1.value = b"v1"

    r2 = batch.records.add()
    r2.value = b"v2"

    return msg


def _build_append_response_envelope() -> pb.ServerMessage:
    msg = pb.ServerMessage()
    msg.append.append_seq = 7
    msg.append.success = True

    ack = msg.append.append_acks.add()
    ack.topic_id = 9
    ack.schema_id = 100
    ack.start_offset = 500
    ack.end_offset = 502

    return msg


def _build_join_group_envelope() -> pb.ClientMessage:
    msg = pb.ClientMessage()
    msg.join_group.group_id = "orders-group"
    msg.join_group.reader_id = "consumer-1"
    msg.join_group.topic_ids.extend([1, 2, 3])
    return msg


# Re-derive hex vectors from the updated proto (no partition_id).
APPEND_ENVELOPE_HEX = _build_append_envelope().SerializeToString().hex()
APPEND_RESPONSE_ENVELOPE_HEX = _build_append_response_envelope().SerializeToString().hex()
JOIN_GROUP_ENVELOPE_HEX = _build_join_group_envelope().SerializeToString().hex()


def test_append_envelope_wire_vector_is_stable():
    msg = _build_append_envelope()
    assert msg.SerializeToString().hex() == APPEND_ENVELOPE_HEX


def test_append_envelope_wire_vector_decodes():
    msg = pb.ClientMessage()
    msg.ParseFromString(bytes.fromhex(APPEND_ENVELOPE_HEX))

    assert msg.WhichOneof("message") == "append"
    assert msg.append.writer_id == bytes(range(16))
    assert msg.append.append_seq == 7
    assert len(msg.append.batches) == 1
    assert len(msg.append.batches[0].records) == 2
    assert msg.append.batches[0].records[0].HasField("key")
    assert not msg.append.batches[0].records[1].HasField("key")


def test_append_response_wire_vector_is_stable():
    msg = _build_append_response_envelope()
    assert msg.SerializeToString().hex() == APPEND_RESPONSE_ENVELOPE_HEX


def test_join_group_wire_vector_is_stable():
    msg = _build_join_group_envelope()
    assert msg.SerializeToString().hex() == JOIN_GROUP_ENVELOPE_HEX

    decoded = pb.ClientMessage()
    decoded.ParseFromString(bytes.fromhex(JOIN_GROUP_ENVELOPE_HEX))
    assert decoded.WhichOneof("message") == "join_group"
    assert decoded.join_group.group_id == "orders-group"
    assert decoded.join_group.reader_id == "consumer-1"
    assert list(decoded.join_group.topic_ids) == [1, 2, 3]


def test_malformed_payload_raises_decode_error():
    msg = pb.ClientMessage()
    with pytest.raises(DecodeError):
        msg.ParseFromString(bytes.fromhex("0a2c0a10000102"))
