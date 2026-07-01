// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

/**
 * Base exception for Flux SDK errors.
 */
public class FluxException extends Exception {
    public FluxException(String message) {
        super(message);
    }

    public FluxException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ConnectionException extends FluxException {
        public ConnectionException(String message) {
            super(message);
        }

        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class AuthenticationException extends FluxException {
        public AuthenticationException(String message) {
            super(message);
        }
    }

    public static class TimeoutException extends FluxException {
        public TimeoutException(String message) {
            super(message);
        }
    }

    public static class BackpressureException extends FluxException {
        public BackpressureException(String message) {
            super(message);
        }
    }

    public static class ProtocolException extends FluxException {
        public ProtocolException(String message) {
            super(message);
        }

        public ProtocolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}