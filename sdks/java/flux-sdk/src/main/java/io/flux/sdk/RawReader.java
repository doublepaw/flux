// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import io.flux.sdk.proto.AuthRequest;
import io.flux.sdk.proto.ClientMessage;
import io.flux.sdk.proto.RawReadRequest;
import io.flux.sdk.proto.RawReadResponse;
import io.flux.sdk.proto.RawSegment;
import io.flux.sdk.proto.ServerMessage;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Zero-copy reader client.
 * <p>
 * Fetches compressed FL segments from the broker as-is and decodes them
 * locally via {@link SegmentDecoder}. Unlike {@link GroupReader}, raw reads
 * address offsets directly and do not participate in reader groups.
 */
public class RawReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RawReader.class);
    private static final int RESPONSE_QUEUE_CAPACITY = 1000;

    private final ReaderConfig config;
    private final FluxWebSocketClient client;
    private final BlockingQueue<byte[]> responseQueue;

    /** Offset range covered by a single decoded segment. */
    public static class SegmentRange {
        private final int topicId;
        private final int schemaId;
        private final long startOffset;
        private final long endOffset;

        public SegmentRange(int topicId, int schemaId, long startOffset, long endOffset) {
            this.topicId = topicId;
            this.schemaId = schemaId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public int getTopicId() { return topicId; }
        public int getSchemaId() { return schemaId; }
        public long getStartOffset() { return startOffset; }
        public long getEndOffset() { return endOffset; }
    }

    /** Result of a single raw read: decoded records, segment ranges, and the high watermark. */
    public static class RawReadBatch {
        private final List<SegmentDecoder.DecodedRecord> records;
        private final List<SegmentRange> segments;
        private final long highWatermark;

        public RawReadBatch(List<SegmentDecoder.DecodedRecord> records,
                            List<SegmentRange> segments,
                            long highWatermark) {
            this.records = records;
            this.segments = segments;
            this.highWatermark = highWatermark;
        }

        public List<SegmentDecoder.DecodedRecord> getRecords() { return records; }
        public List<SegmentRange> getSegments() { return segments; }
        public long getHighWatermark() { return highWatermark; }
    }

    private RawReader(ReaderConfig config, FluxWebSocketClient client, BlockingQueue<byte[]> responseQueue) {
        this.config = config;
        this.client = client;
        this.responseQueue = responseQueue;
    }

    public static RawReader connect(ReaderConfig config) throws FluxException {
        try {
            URI uri = new URI(config.getUrl());
            BlockingQueue<byte[]> responseQueue = new LinkedBlockingQueue<>(RESPONSE_QUEUE_CAPACITY);
            FluxWebSocketClient client = new FluxWebSocketClient(uri, responseQueue);

            if (!client.connectBlocking(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new FluxException.ConnectionException("Failed to connect to " + config.getUrl());
            }

            RawReader reader = new RawReader(config, client, responseQueue);
            if (config.getApiKey() != null) {
                reader.authenticate(config.getApiKey());
            }
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
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
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

    /**
     * Read compressed segments starting at {@code offset}, verify each segment's
     * CRC-32, and decode the contained records.
     *
     * @param topicId  topic to read from
     * @param offset   first offset to read
     * @param maxBytes budget in compressed bytes
     */
    public RawReadBatch rawRead(int topicId, long offset, int maxBytes) throws FluxException {
        RawReadRequest req = RawReadRequest.newBuilder()
                .setTopicId(topicId)
                .setOffset(offset)
                .setMaxBytes(maxBytes)
                .build();
        client.send(ClientMessage.newBuilder().setRawRead(req).build().toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FluxException.TimeoutException("Raw read timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.RAW_READ) {
                throw new FluxException.ProtocolException("Unexpected response type or empty response");
            }

            RawReadResponse resp = envelope.getRawRead();
            if (!resp.getSuccess()) {
                throw new FluxException.ProtocolException(
                        "Raw read failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            List<SegmentDecoder.DecodedRecord> records = new ArrayList<>();
            List<SegmentRange> segments = new ArrayList<>(resp.getSegmentsCount());
            for (RawSegment segment : resp.getSegmentsList()) {
                records.addAll(SegmentDecoder.decode(
                        segment.getPayload().toByteArray(),
                        Integer.toUnsignedLong(segment.getCrc32())
                ));
                segments.add(new SegmentRange(
                        segment.getTopicId(),
                        segment.getSchemaId(),
                        segment.getStartOffset(),
                        segment.getEndOffset()
                ));
            }

            return new RawReadBatch(records, segments, resp.getHighWatermark());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FluxException.ProtocolException("Failed to decode response", e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private static class FluxWebSocketClient extends WebSocketClient {
        private static final Logger log = LoggerFactory.getLogger(FluxWebSocketClient.class);
        private final BlockingQueue<byte[]> responseQueue;

        public FluxWebSocketClient(URI serverUri, BlockingQueue<byte[]> responseQueue) {
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
            responseQueue.offer(data);
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
