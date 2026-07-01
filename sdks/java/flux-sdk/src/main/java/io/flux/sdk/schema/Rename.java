// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk.schema;

import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** A single rename pair: the old field name and the current field name. */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Rename {
    String from();
    String to();
}