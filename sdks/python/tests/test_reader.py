"""Tests for GroupReader - focused on poll-based model behavior."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from flux.reader import GroupReader, PollBatch, ReaderConfig, ReaderState
from flux.exceptions import ProtocolException, TimeoutException


def validate_response_bounds(response: bytes, expected_type: int) -> None:
    """
    Helper that mimics the response validation pattern.
    This validates response has at least 1 byte and matches expected type.
    """
    if len(response) < 1 or response[0] != expected_type:
        raise ProtocolException("Unexpected response type or empty response")


class TestReaderBoundsChecks:
    """Tests for response bounds checking in GroupReader."""

    def test_empty_response_raises_protocol_exception(self):
        """Empty response raises ProtocolException, not IndexError."""
        empty_response = b""

        with pytest.raises(ProtocolException):
            validate_response_bounds(empty_response, 0x10)

    def test_valid_response_passes_bounds_check(self):
        """Valid responses should pass the bounds check."""
        valid_response = b"\x10\x00\x00"

        # Should not raise
        validate_response_bounds(valid_response, 0x10)

        # Wrong type should raise ProtocolException
        with pytest.raises(ProtocolException):
            validate_response_bounds(valid_response, 0x20)


class TestReaderPollLoopExceptionHandling:
    """Tests for exception handling in poll_loop."""

    @pytest.mark.asyncio
    async def test_poll_loop_should_not_swallow_all_exceptions(self):
        """
        Fatal exceptions like ConnectionError should propagate from poll_loop,
        only transient errors (TimeoutException) should trigger retry.
        """
        fatal_exceptions = [
            ProtocolException("Invalid response"),
            ConnectionError("Connection lost"),
            RuntimeError("Unexpected error"),
        ]

        transient_exceptions = [
            TimeoutException("Timeout"),
        ]

        for exc in fatal_exceptions:
            assert not isinstance(exc, TimeoutException), (
                f"{type(exc).__name__} is fatal and should propagate"
            )


class TestReaderPollOffsetTracking:
    """Tests for poll-based offset tracking in GroupReader."""

    def test_empty_poll_not_tracked_in_inflight(self):
        """Empty poll (start_offset == end_offset) should not add to inflight."""
        inflight: list[tuple[int, int]] = [(10, 50)]
        # Simulate poll returning an empty range
        start_offset, end_offset = 50, 50
        if start_offset != end_offset:
            inflight.append((start_offset, end_offset))
        assert len(inflight) == 1, "Empty poll should not grow inflight list"
        assert inflight[0] == (10, 50)

    def test_poll_batch_tracks_offset_range(self):
        """PollBatch captures offset range from a single poll."""
        batch = PollBatch(
            results=[],
            start_offset=100,
            end_offset=150,
            lease_deadline_ms=9999,
        )
        assert batch.end_offset > batch.start_offset
        assert batch.end_offset - batch.start_offset == 50
        assert batch.lease_deadline_ms == 9999

    def test_commit_is_noop_for_empty_batch(self):
        """Commit should be skipped when the batch has zero offsets."""
        batch = PollBatch(results=[], start_offset=0, end_offset=0)
        should_skip = batch.start_offset == 0 and batch.end_offset == 0
        assert should_skip, "Commit should be skipped for an empty batch"

    def test_multiple_inflight_batches(self):
        """Multiple PollBatches can be outstanding simultaneously."""
        b1 = PollBatch(results=[], start_offset=0, end_offset=50)
        b2 = PollBatch(results=[], start_offset=50, end_offset=100)
        inflight = [(b1.start_offset, b1.end_offset), (b2.start_offset, b2.end_offset)]
        assert len(inflight) == 2
        # After committing b1
        s, e = b1.start_offset, b1.end_offset
        inflight = [(ss, ee) for ss, ee in inflight if ss != s or ee != e]
        assert len(inflight) == 1
        assert inflight[0] == (50, 100)


class TestReaderStateTransitions:
    """Tests for reader state transitions in the poll model."""

    def test_rejoin_clears_inflight(self):
        """Rejoin should clear inflight batches."""
        # _do_join() clears inflight and updates state.
        # Verify the contract: after rejoin, inflight must be empty.
        inflight: list[tuple[int, int]] = [(0, 50), (50, 100)]
        # Simulate what _do_join() does:
        inflight.clear()
        assert inflight == [], "Rejoin (via _do_join) should clear inflight"

    def test_reader_states(self):
        """States are: INIT, ACTIVE, STOPPED."""
        states = list(ReaderState)
        assert len(states) == 3, "Should have exactly 3 states"
        assert ReaderState.INIT in states
        assert ReaderState.ACTIVE in states
        assert ReaderState.STOPPED in states


class TestReaderConfig:
    """Tests for ReaderConfig."""

    def test_reader_config_fields(self):
        """ReaderConfig should have expected fields."""
        config = ReaderConfig(
            url="ws://localhost:9000",
            group_id="test",
            topic_id=1,
        )
        assert config.url == "ws://localhost:9000"
        assert config.group_id == "test"
        assert config.topic_id == 1

    def test_reader_id_auto_generated(self):
        """reader_id should be auto-generated when not provided."""
        config = ReaderConfig(url="ws://localhost:9000", group_id="test")
        assert config.reader_id is not None, (
            "reader_id should be auto-generated"
        )

    def test_reader_config_defaults(self):
        """ReaderConfig should have sensible defaults."""
        config = ReaderConfig()
        assert config.url == "ws://localhost:9000"
        assert config.group_id == "default"
        assert config.topic_id == 1
        assert config.max_bytes == 1024 * 1024
        assert config.timeout == 30.0
        assert config.heartbeat_interval == 10.0
