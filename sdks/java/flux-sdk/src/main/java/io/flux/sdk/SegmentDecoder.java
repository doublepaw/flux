// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Decodes compressed FL segments returned by raw reads.
 * <p>
 * A segment payload is a zstd-compressed concatenation of records. Each record
 * is a zigzag-varint key length (-1 means null key) followed by the key bytes,
 * then a zigzag-varint value length followed by the value bytes. Records are
 * packed back-to-back until the decompressed buffer is exhausted.
 */
public final class SegmentDecoder {

    /** A single record decoded from an FL segment. Key may be null. */
    public static final class DecodedRecord {
        private final byte[] key;
        private final byte[] value;

        public DecodedRecord(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        /** Record key, or null if the record has no key. */
        public byte[] getKey() { return key; }

        public byte[] getValue() { return value; }
    }

    private SegmentDecoder() {}

    /**
     * Verify the CRC-32 of the compressed payload, decompress it, and decode records.
     *
     * @param payload     compressed segment payload (zstd frame)
     * @param expectedCrc CRC-32 of the compressed payload as an unsigned 32-bit value
     * @throws FluxException on CRC mismatch or malformed segment data
     */
    public static List<DecodedRecord> decode(byte[] payload, long expectedCrc) throws FluxException {
        CRC32 crc = new CRC32();
        crc.update(payload);
        if (crc.getValue() != (expectedCrc & 0xFFFFFFFFL)) {
            throw new FluxException.ProtocolException(
                    "Segment CRC mismatch: expected " + (expectedCrc & 0xFFFFFFFFL)
                            + ", computed " + crc.getValue()
            );
        }

        ByteBuffer buf = ByteBuffer.wrap(decompress(payload));
        List<DecodedRecord> records = new ArrayList<>();
        while (buf.hasRemaining()) {
            byte[] key = readField(buf, true);
            byte[] value = readField(buf, false);
            records.add(new DecodedRecord(key, value));
        }
        return records;
    }

    private static byte[] decompress(byte[] payload) throws FluxException {
        long contentSize;
        try {
            contentSize = Zstd.getFrameContentSize(payload);
        } catch (RuntimeException e) {
            contentSize = -1; // unknown; fall back to streaming
        }

        try {
            if (contentSize > 0 && contentSize <= Integer.MAX_VALUE) {
                return Zstd.decompress(payload, (int) contentSize);
            }
            return decompressStreaming(payload);
        } catch (IOException | RuntimeException e) {
            throw new FluxException.ProtocolException("Failed to decompress segment payload", e);
        }
    }

    private static byte[] decompressStreaming(byte[] payload) throws IOException {
        try (ZstdInputStream in = new ZstdInputStream(new ByteArrayInputStream(payload))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /** Read a length-prefixed field; a -1 length means null (only valid for keys). */
    private static byte[] readField(ByteBuffer buf, boolean nullable) throws FluxException {
        long length = readZigZagVarint(buf);
        if (length == -1 && nullable) {
            return null;
        }
        if (length < 0 || length > buf.remaining()) {
            throw new FluxException.ProtocolException(
                    "Invalid field length " + length + " with " + buf.remaining() + " bytes remaining"
            );
        }
        byte[] bytes = new byte[(int) length];
        buf.get(bytes);
        return bytes;
    }

    /** Zigzag varint: little-endian 7-bit groups with 0x80 continuation, then zigzag decode. */
    private static long readZigZagVarint(ByteBuffer buf) throws FluxException {
        long raw = 0;
        int shift = 0;
        while (true) {
            if (!buf.hasRemaining()) {
                throw new FluxException.ProtocolException("Truncated varint in segment");
            }
            byte b = buf.get();
            raw |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift >= 64) {
                throw new FluxException.ProtocolException("Varint too long in segment");
            }
        }
        return (raw >>> 1) ^ -(raw & 1);
    }
}
