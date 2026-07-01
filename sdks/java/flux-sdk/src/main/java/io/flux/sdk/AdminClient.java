// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * HTTP wrapper for the Flux broker admin API.
 */
public class AdminClient {
    private static final Gson GSON = new Gson();
    private static final Type TOPIC_LIST_TYPE = new TypeToken<List<Map<String, Object>>>() {}.getType();

    private final String baseUrl;
    private final String apiKey;
    private final HttpClient http;

    public AdminClient(String baseUrl, String apiKey, Duration timeout) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.apiKey = apiKey;
        this.http = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
    }

    /**
     * Fetch all topics. Each map contains topic_id, name, retention_hours, etc.
     */
    public List<Map<String, Object>> listTopics() throws FluxException {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/topics"))
                .GET();
        addAuth(req);

        try {
            HttpResponse<String> resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new FluxException("Admin API error " + resp.statusCode() + ": " + resp.body());
            }
            return GSON.fromJson(resp.body(), TOPIC_LIST_TYPE);
        } catch (IOException e) {
            throw new FluxException("Admin API unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        }
    }

    /**
     * Register a schema for a topic. Returns schema_id (idempotent via hash dedup).
     */
    public int registerSchema(int topicId, Map<String, Object> schema) throws FluxException {
        Map<String, Object> body = Map.of("topic_id", topicId, "schema", schema);
        String json = GSON.toJson(body);

        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/schemas"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json));
        addAuth(req);

        try {
            HttpResponse<String> resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 409) {
                throw new FluxException("Incompatible schema: " + resp.body());
            }
            if (resp.statusCode() != 200 && resp.statusCode() != 201) {
                throw new FluxException("Admin API error " + resp.statusCode() + ": " + resp.body());
            }
            Map<String, Object> result = GSON.fromJson(resp.body(), new TypeToken<Map<String, Object>>() {}.getType());
            return ((Number) result.get("schema_id")).intValue();
        } catch (IOException e) {
            throw new FluxException("Admin API unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        }
    }

    private void addAuth(HttpRequest.Builder req) {
        if (apiKey != null) {
            req.header("Authorization", "Bearer " + apiKey);
        }
    }
}