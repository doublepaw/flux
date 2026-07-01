// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import java.time.Duration;

/**
 * Configuration for the Flux writer.
 */
public class WriterConfig {
    private String url = "ws://localhost:9000";
    private String apiKey = null;
    private int maxRetries = 5;
    private int maxInFlight = 256;
    private Duration initialBackoff = Duration.ofMillis(100);
    private Duration maxBackoff = Duration.ofSeconds(10);
    private Duration timeout = Duration.ofSeconds(30);

    public WriterConfig() {}

    public WriterConfig url(String url) {
        this.url = url;
        return this;
    }

    public WriterConfig apiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public WriterConfig maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public WriterConfig maxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
        return this;
    }

    public WriterConfig initialBackoff(Duration initialBackoff) {
        this.initialBackoff = initialBackoff;
        return this;
    }

    public WriterConfig maxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
        return this;
    }

    public WriterConfig timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public String getApiKey() {
        return apiKey;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxInFlight() {
        return maxInFlight;
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public Duration getTimeout() {
        return timeout;
    }
}