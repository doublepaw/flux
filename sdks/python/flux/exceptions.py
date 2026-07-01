# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Flux SDK exceptions."""


class FluxException(Exception):
    """Base exception for Flux SDK errors."""

    pass


class ConnectionException(FluxException):
    """Connection error."""

    pass


class AuthenticationException(FluxException):
    """Authentication failed."""

    pass


class TimeoutException(FluxException):
    """Request timeout."""

    pass


class BackpressureException(FluxException):
    """Server backpressure - too many retries."""

    pass


class ProtocolException(FluxException):
    """Protocol error."""

    pass


class SchemaException(FluxException):
    """Schema generation or validation error."""

    pass