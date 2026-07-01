// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk.schema;

/** Unchecked exception for schema generation errors (programming mistakes). */
public class SchemaException extends RuntimeException {
    public SchemaException(String message) {
        super(message);
    }
}