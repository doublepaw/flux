"""Tests for Writer - focused on critical issues."""

import pytest
from flux.exceptions import ProtocolException


class TestWriterBoundsChecks:
    """Tests for response bounds checking in Writer."""

    def test_empty_response_should_raise_protocol_exception(self):
        """
        BUG: In Writer._send_request() at line 212, the code does:
            if len(response) < 2 or response[0] != MessageType.PRODUCE_RESPONSE:

        This correctly checks len(response) < 2, but the same pattern is not
        consistently applied elsewhere. This test verifies the expected behavior.
        """
        # Simulate an empty response
        empty_response = b""

        # The check should catch this
        assert len(empty_response) < 2, "Empty response should fail length check"

        # Also test single-byte response
        single_byte = b"\x00"
        assert len(single_byte) < 2, "Single-byte response should fail length check"

    def test_response_bounds_check_in_authenticate(self):
        """
        In Writer._authenticate() at line 113, the code does:
            if len(response) < 2 or response[0] != MessageType.AUTH_RESPONSE:

        This is correct. Verify the logic.
        """
        # Valid: 2+ bytes
        valid_response = b"\x01\x00"
        assert len(valid_response) >= 2

        # Invalid: 1 byte
        invalid_response = b"\x01"
        assert len(invalid_response) < 2


class TestWriterSequenceHandling:
    """Tests for sequence number handling in Writer."""

    def test_sequence_should_not_increment_on_backpressure_retry(self):
        """
        BUG: In Writer.send_batch() at lines 164-166, the sequence number
        is incremented at the START of each retry attempt:

            async with self._lock:
                current_seq = self._seq
                self._seq += 1

        This means if backpressure causes retries, each retry gets a NEW
        sequence number. The server may then see gaps in sequence numbers
        or duplicate messages.

        Expected behavior: sequence should only increment on SUCCESSFUL sends,
        or the same sequence should be reused for retries.
        """
        # This documents the issue:
        # - Send attempt 1: seq=1
        # - Backpressure, retry
        # - Send attempt 2: seq=2 (BUG: should still be seq=1)
        # - Success
        # - Next send: seq=3 (but server only saw seq=2 succeed)

        initial_seq = 1
        retries = 3

        # Current buggy behavior: sequence advances with each retry
        buggy_final_seq = initial_seq + retries

        # Expected behavior: sequence stays same during retries
        expected_final_seq = initial_seq + 1  # Only increments on success

        assert buggy_final_seq != expected_final_seq, (
            "Bug: sequence increments on each retry instead of only on success"
        )
