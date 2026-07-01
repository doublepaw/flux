// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import java.time.Duration;

/**
 * Configuration for {@link FluxClient}.
 */
public class ClientConfig {
    private String wsUrl = "ws://localhost:9000";
    private String adminUrl = "http://localhost:9001";
    private String apiKey = null;
    private int maxInFlight = 256;
    private Duration timeout = Duration.ofSeconds(30);

    public ClientConfig() {}

    public ClientConfig wsUrl(String wsUrl) {
        this.wsUrl = wsUrl;
        return this;
    }

    public ClientConfig adminUrl(String adminUrl) {
        this.adminUrl = adminUrl;
        return this;
    }

    public ClientConfig apiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public ClientConfig maxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
        return this;
    }

    public ClientConfig timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public String getWsUrl() { return wsUrl; }
    public String getAdminUrl() { return adminUrl; }
    public String getApiKey() { return apiKey; }
    public int getMaxInFlight() { return maxInFlight; }
    public Duration getTimeout() { return timeout; }
}