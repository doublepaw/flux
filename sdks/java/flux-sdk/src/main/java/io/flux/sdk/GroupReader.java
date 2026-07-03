// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import com.google.protobuf.ByteString;
import io.flux.sdk.proto.AuthRequest;
import io.flux.sdk.proto.ClientMessage;
import io.flux.sdk.proto.CommitRequest;
import io.flux.sdk.proto.HeartbeatRequest;
import io.flux.sdk.proto.HeartbeatStatus;
import io.flux.sdk.proto.JoinGroupRequest;
import io.flux.sdk.proto.LeaveGroupRequest;
import io.flux.sdk.proto.PollRequest;
import io.flux.sdk.proto.PollResponse;
import io.flux.sdk.proto.RawSegment;
import io.flux.sdk.proto.Record;
import io.flux.sdk.proto.ServerMessage;
import io.flux.sdk.proto.TopicResult;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reader client with reader group support.
 * <p>
 * Uses a poll-based model where the broker dispatches work to readers.
 * Wire payloads use generated protobuf message classes directly.
 */
public class GroupReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GroupReader.class);
    private static final int RESPONSE_QUEUE_CAPACITY = 1000;

    public enum State {
        INIT,
        ACTIVE,
        STOPPED
    }

    private final ReaderConfig config;
    private final FluxWebSocketClient client;
    private final ResponseRouter responseQueue;

    /// Routes each server message to a per-type queue so concurrent request
    /// threads (poll/commit vs the heartbeat loop) receive their own replies.
    static final class ResponseRouter {
        private final ConcurrentHashMap<ServerMessage.MessageCase, BlockingQueue<byte[]>> queues =
                new ConcurrentHashMap<>();

        private BlockingQueue<byte[]> queueFor(ServerMessage.MessageCase c) {
            return queues.computeIfAbsent(c, k -> new LinkedBlockingQueue<>(RESPONSE_QUEUE_CAPACITY));
        }

        void route(byte[] data) {
            ServerMessage.MessageCase c;
            try {
                c = ServerMessage.parseFrom(data).getMessageCase();
            } catch (Exception e) {
                c = ServerMessage.MessageCase.MESSAGE_NOT_SET;
            }
            queueFor(c).offer(data);
        }

        byte[] take(ServerMessage.MessageCase c, long timeoutMs) throws InterruptedException {
            return queueFor(c).poll(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A batch of results from a single poll, with offset range and lease deadline.
     * Pass this to {@link #commit(PollBatch)} to commit the specific range.
     */
    public static class PollBatch {
        private final List<TopicResult> results;
        private final long startOffset;
        private final long endOffset;
        private final long leaseDeadlineMs;

        public PollBatch(List<TopicResult> results, long startOffset, long endOffset, long leaseDeadlineMs) {
            this.results = results;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.leaseDeadlineMs = leaseDeadlineMs;
        }

        public List<TopicResult> getResults() { return results; }
        public long getStartOffset() { return startOffset; }
        public long getEndOffset() { return endOffset; }
        public long getLeaseDeadlineMs() { return leaseDeadlineMs; }
    }

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private State state = State.INIT;
    private final List<long[]> inflight = new ArrayList<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ScheduledExecutorService heartbeatExecutor;

    private GroupReader(ReaderConfig config, FluxWebSocketClient client, ResponseRouter responseQueue) {
        this.config = config;
        this.client = client;
        this.responseQueue = responseQueue;
    }

    public static GroupReader join(ReaderConfig config) throws FluxException {
        try {
            URI uri = new URI(config.getUrl());
            ResponseRouter responseQueue = new ResponseRouter();
            FluxWebSocketClient client = new FluxWebSocketClient(uri, responseQueue);

            if (!client.connectBlocking(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new FluxException.ConnectionException("Failed to connect to " + config.getUrl());
            }

            GroupReader reader = new GroupReader(config, client, responseQueue);
            if (config.getApiKey() != null) {
                reader.authenticate(config.getApiKey());
            }
            reader.doJoin();
            return reader;
        } catch (Exception e) {
            if (e instanceof FluxException) {
                throw (FluxException) e;
            }
            throw new FluxException.ConnectionException("Failed to connect", e);
        }
    }

    private void authenticate(String apiKey) throws FluxException {
        ClientMessage authMessage = ClientMessage.newBuilder()
                .setAuth(AuthRequest.newBuilder().setApiKey(apiKey).build())
                .build();
        client.send(authMessage.toByteArray());

        try {
            byte[] response = responseQueue.take(ServerMessage.MessageCase.AUTH, config.getTimeout().toMillis());
            if (response == null) {
                throw new FluxException.TimeoutException("Authentication timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.AUTH) {
                throw new FluxException.ProtocolException("Unexpected auth response");
            }

            io.flux.sdk.proto.AuthResponse auth = envelope.getAuth();
            if (!auth.getSuccess()) {
                throw new FluxException.AuthenticationException(
                        "Auth failed (" + auth.getErrorCode() + "): " + auth.getErrorMessage()
                );
            }
            log.debug("Authentication successful");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted during auth", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FluxException.ProtocolException("Failed to decode auth response", e);
        }
    }

    public void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flux-heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
                this::heartbeatTick,
                config.getHeartbeatInterval().toMillis(),
                config.getHeartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private void heartbeatTick() {
        if (!running.get()) {
            return;
        }

        try {
            HeartbeatRequest req = HeartbeatRequest.newBuilder()
                    .setGroupId(config.getGroupId())
                    .setTopicId(config.getTopicId())
                    .setReaderId(config.getReaderId())
                    .build();

            client.send(ClientMessage.newBuilder().setHeartbeat(req).build().toByteArray());

            byte[] response = responseQueue.take(ServerMessage.MessageCase.HEARTBEAT, config.getTimeout().toMillis());
            if (response == null) {
                log.warn("Heartbeat timeout");
                return;
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.HEARTBEAT) {
                log.warn("Unexpected response to heartbeat");
                return;
            }

            io.flux.sdk.proto.HeartbeatResponse resp = envelope.getHeartbeat();
            if (!resp.getSuccess()) {
                log.warn("Heartbeat failed ({}): {}", resp.getErrorCode(), resp.getErrorMessage());
                return;
            }

            if (resp.getStatus() == HeartbeatStatus.HEARTBEAT_STATUS_OK) {
                log.debug("Heartbeat OK");
            } else if (resp.getStatus() == HeartbeatStatus.HEARTBEAT_STATUS_UNKNOWN_MEMBER) {
                log.warn("Unknown member, rejoining group");
                doJoin();
            }
        } catch (Exception e) {
            log.error("Heartbeat failed", e);
        }
    }

    public void stop() throws FluxException {
        running.set(false);

        stateLock.writeLock().lock();
        try {
            state = State.STOPPED;
        } finally {
            stateLock.writeLock().unlock();
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }

        doLeave();
    }

    public State getState() {
        stateLock.readLock().lock();
        try {
            return state;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public PollBatch poll() throws FluxException {
        stateLock.readLock().lock();
        try {
            if (state != State.ACTIVE) {
                throw new FluxException.ProtocolException("Reader not active: " + state);
            }
        } finally {
            stateLock.readLock().unlock();
        }

        PollRequest req = PollRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setTopicId(config.getTopicId())
                .setReaderId(config.getReaderId())
                .setMaxBytes(config.getMaxBytes())
                .setRaw(config.isRawPoll())
                .build();
        client.send(ClientMessage.newBuilder().setPoll(req).build().toByteArray());

        try {
            byte[] response = responseQueue.take(ServerMessage.MessageCase.POLL, config.getTimeout().toMillis());
            if (response == null) {
                throw new FluxException.TimeoutException("Poll timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.POLL) {
                throw new FluxException.ProtocolException("Unexpected response type or empty response");
            }

            PollResponse resp = envelope.getPoll();
            if (!resp.getSuccess()) {
                throw new FluxException.ProtocolException(
                        "Poll failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            if (resp.getStartOffset() != resp.getEndOffset()) {
                stateLock.writeLock().lock();
                try {
                    inflight.add(new long[]{resp.getStartOffset(), resp.getEndOffset()});
                } finally {
                    stateLock.writeLock().unlock();
                }
            }

            List<TopicResult> results = new ArrayList<>(resp.getResultsList());
            for (RawSegment segment : resp.getRawSegmentsList()) {
                results.add(decodeRawSegment(segment, resp.getEndOffset()));
            }

            return new PollBatch(
                    results,
                    resp.getStartOffset(),
                    resp.getEndOffset(),
                    resp.getLeaseDeadlineMs()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FluxException.ProtocolException("Failed to decode response", e);
        }
    }

    /** Decode a compressed raw segment into the same shape as a classic poll result. */
    private static TopicResult decodeRawSegment(RawSegment segment, long highWatermark) throws FluxException {
        List<SegmentDecoder.DecodedRecord> decoded = SegmentDecoder.decode(
                segment.getPayload().toByteArray(),
                Integer.toUnsignedLong(segment.getCrc32())
        );

        TopicResult.Builder result = TopicResult.newBuilder()
                .setTopicId(segment.getTopicId())
                .setSchemaId(segment.getSchemaId())
                .setHighWatermark(highWatermark);
        for (SegmentDecoder.DecodedRecord record : decoded) {
            Record.Builder builder = Record.newBuilder()
                    .setValue(ByteString.copyFrom(record.getValue()));
            if (record.getKey() != null) {
                builder.setKey(ByteString.copyFrom(record.getKey()));
            }
            result.addRecords(builder);
        }
        return result.build();
    }

    public void commit(PollBatch batch) throws FluxException {
        if (batch.getStartOffset() == batch.getEndOffset()) {
            return;
        }

        CommitRequest req = CommitRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .setTopicId(config.getTopicId())
                .setStartOffset(batch.getStartOffset())
                .setEndOffset(batch.getEndOffset())
                .build();
        client.send(ClientMessage.newBuilder().setCommit(req).build().toByteArray());

        try {
            byte[] response = responseQueue.take(ServerMessage.MessageCase.COMMIT, config.getTimeout().toMillis());
            if (response == null) {
                throw new FluxException.TimeoutException("Commit timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.COMMIT) {
                throw new FluxException.ProtocolException("Unexpected response type or empty response");
            }

            io.flux.sdk.proto.CommitResponse resp = envelope.getCommit();
            if (!resp.getSuccess()) {
                throw new FluxException.ProtocolException(
                        "Commit failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            stateLock.writeLock().lock();
            try {
                long s = batch.getStartOffset(), e = batch.getEndOffset();
                inflight.removeIf(r -> r[0] == s && r[1] == e);
            } finally {
                stateLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FluxException.ProtocolException("Failed to decode response", e);
        }
    }

    private void doJoin() throws FluxException {
        JoinGroupRequest req = JoinGroupRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .addTopicIds(config.getTopicId())
                .build();
        client.send(ClientMessage.newBuilder().setJoinGroup(req).build().toByteArray());

        try {
            byte[] response = responseQueue.take(ServerMessage.MessageCase.JOIN_GROUP, config.getTimeout().toMillis());
            if (response == null) {
                throw new FluxException.TimeoutException("Join timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.JOIN_GROUP) {
                throw new FluxException.ProtocolException("Unexpected response type or empty response");
            }

            io.flux.sdk.proto.JoinGroupResponse resp = envelope.getJoinGroup();
            if (!resp.getSuccess()) {
                throw new FluxException.ProtocolException(
                        "Join failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            stateLock.writeLock().lock();
            try {
                inflight.clear();
                state = State.ACTIVE;
            } finally {
                stateLock.writeLock().unlock();
            }

            log.info("Joined group {}", config.getGroupId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FluxException.ProtocolException("Failed to decode response", e);
        }
    }

    private void doLeave() throws FluxException {
        List<long[]> ranges;
        stateLock.readLock().lock();
        try {
            ranges = new ArrayList<>(inflight);
        } finally {
            stateLock.readLock().unlock();
        }
        for (long[] range : ranges) {
            try {
                commit(new PollBatch(List.of(), range[0], range[1], 0));
            } catch (FluxException e) {
                log.warn("Failed to commit range [{}, {}) during leave", range[0], range[1], e);
            }
        }

        LeaveGroupRequest req = LeaveGroupRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setTopicId(config.getTopicId())
                .setReaderId(config.getReaderId())
                .build();
        client.send(ClientMessage.newBuilder().setLeaveGroup(req).build().toByteArray());
        log.info("Left group {}", config.getGroupId());
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (FluxException e) {
            log.warn("Error during close", e);
        }
        client.close();
    }

    private static class FluxWebSocketClient extends WebSocketClient {
        private static final Logger log = LoggerFactory.getLogger(FluxWebSocketClient.class);
        private final ResponseRouter responseQueue;

        public FluxWebSocketClient(URI serverUri, ResponseRouter responseQueue) {
            super(serverUri);
            this.responseQueue = responseQueue;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            log.debug("WebSocket connected");
        }

        @Override
        public void onMessage(String message) {
            log.warn("Received text message, expected binary");
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            byte[] data = new byte[bytes.remaining()];
            bytes.get(data);
            responseQueue.route(data);
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.debug("WebSocket closed: {} - {}", code, reason);
        }

        @Override
        public void onError(Exception ex) {
            log.error("WebSocket error", ex);
        }
    }
}