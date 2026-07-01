// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import java.time.Duration;
import java.util.UUID;

/**
 * Configuration for the Flux reader.
 */
public class ReaderConfig {
    private String url = "ws://localhost:9000";
    private String apiKey = null;
    private String groupId = "default";
    private String readerId = UUID.randomUUID().toString();
    private int topicId = 1;
    private int maxBytes = 1024 * 1024; // 1 MB
    private Duration timeout = Duration.ofSeconds(30);
    private Duration heartbeatInterval = Duration.ofSeconds(10);

    public ReaderConfig() {}

    public ReaderConfig url(String url) {
        this.url = url;
        return this;
    }

    public ReaderConfig apiKey(String apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public ReaderConfig groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public ReaderConfig readerId(String readerId) {
        this.readerId = readerId;
        return this;
    }

    public ReaderConfig topicId(int topicId) {
        this.topicId = topicId;
        return this;
    }

    public ReaderConfig maxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public ReaderConfig timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ReaderConfig heartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getReaderId() {
        return readerId;
    }

    public int getTopicId() {
        return topicId;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

}